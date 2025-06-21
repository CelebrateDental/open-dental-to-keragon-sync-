#!/usr/bin/env python3
"""
OpenDental → Keragon Appointment Sync
Smart first-run and daily sync with clear logs.
"""

import os
import sys
import json
import logging
import datetime
import requests
import time
from typing import List, Dict, Any, Optional, Tuple
from requests.exceptions import HTTPError, RequestException
from dataclasses import dataclass, field
from concurrent.futures import ThreadPoolExecutor, as_completed
from zoneinfo import ZoneInfo  # Python 3.9+

# === CONFIG ===
@dataclass
class Config:
    api_base_url: str = os.environ.get('OPEN_DENTAL_API_URL', 'https://api.opendental.com/api/v1')
    developer_key: str = os.environ.get('OPEN_DENTAL_DEVELOPER_KEY', '')
    customer_key: str = os.environ.get('OPEN_DENTAL_CUSTOMER_KEY', '')
    keragon_webhook_url: str = os.environ.get('KERAGON_WEBHOOK_URL', '')
    state_file: str = os.environ.get('STATE_FILE', 'last_sync_state.json')
    log_level: str = os.environ.get('LOG_LEVEL', 'INFO')
    lookahead_hours: int = int(os.environ.get('LOOKAHEAD_HOURS', '720'))
    max_workers: int = int(os.environ.get('MAX_WORKERS', '5'))
    request_timeout: int = int(os.environ.get('REQUEST_TIMEOUT', '30'))
    retry_attempts: int = int(os.environ.get('RETRY_ATTEMPTS', '3'))
    timezone: str = os.environ.get('CLINIC_TIMEZONE', 'America/Chicago')
    clinic_nums: List[int] = field(default_factory=lambda: [int(x) for x in os.environ.get('CLINIC_NUMS', '').split(',') if x.strip().isdigit()])
    operatory_filters: Dict[int, List[int]] = field(default_factory=lambda: {
        9034: [11579, 11580, 11588],
        9035: [11574, 11576, 11577],
    })

config = Config()
logger = logging.getLogger('opendental_sync')
handler = logging.StreamHandler(sys.stdout)
handler.setFormatter(logging.Formatter('%(asctime)s - %(levelname)s - %(message)s'))
logger.addHandler(handler)
logger.setLevel(getattr(logging, config.log_level.upper(), logging.INFO))
logging.getLogger('urllib3').setLevel(logging.WARNING)

# === RETRY ===
def retry(fn):
    def wrapper(*args, **kwargs):
        last_exc = None
        for i in range(config.retry_attempts):
            try:
                return fn(*args, **kwargs)
            except (RequestException, HTTPError) as e:
                last_exc = e
                time.sleep(2 ** i)
        logger.error(f"All retries failed: {last_exc}")
        raise last_exc
    return wrapper

# === STATE ===
def load_state() -> Dict[int, str]:
    if not os.path.exists(config.state_file):
        return {}
    with open(config.state_file) as f:
        return json.load(f)

def save_state(state: Dict[int, str]):
    tmp = config.state_file + '.tmp'
    with open(tmp, 'w') as f:
        json.dump(state, f, indent=2)
    os.replace(tmp, config.state_file)

# === UTILS ===
def make_headers():
    return {'Authorization': f'ODFHIR {config.developer_key}/{config.customer_key}', 'Content-Type': 'application/json'}

def parse_iso(dt: Optional[str]) -> Optional[datetime.datetime]:
    if not dt: return None
    for fmt in ("%Y-%m-%dT%H:%M:%S.%f", "%Y-%m-%dT%H:%M:%S", "%Y-%m-%d %H:%M:%S"):
        try: return datetime.datetime.strptime(dt, fmt)
        except ValueError: continue
    return None

def get_times_pattern(appt):
    start = parse_iso(appt.get('AptDateTime'))
    if start:
        start = start.replace(tzinfo=ZoneInfo(config.timezone))
    pattern = appt.get('Pattern') or ''
    duration = max(len(pattern) * 5, 5)
    end = start + datetime.timedelta(minutes=duration) if start else None
    return start, end, duration

@retry
def fetch_operatories(clinic: int) -> List[int]:
    resp = requests.get(f"{config.api_base_url}/operatories", headers=make_headers(), params={'ClinicNum': clinic}, timeout=config.request_timeout)
    resp.raise_for_status()
    return [op['OperatoryNum'] for op in resp.json()]

@retry
def fetch_appointments(clinic: int, ops: List[int], since: Optional[str]) -> List[Dict[str, Any]]:
    now = datetime.datetime.utcnow()
    start, end = now.strftime("%Y-%m-%d"), (now + datetime.timedelta(hours=config.lookahead_hours)).strftime("%Y-%m-%d")
    logger.info(f"Clinic {clinic}: fetching appointments from {start} to {end}")
    appts = []
    for status in ['Scheduled','Complete','Broken']:
        for op in ops:
            params = {'ClinicNum': clinic, 'dateStart': start, 'dateEnd': end, 'AptStatus': status, 'Op': op}
            if since:
                params['DateTStamp'] = since
            resp = requests.get(f"{config.api_base_url}/appointments", headers=make_headers(), params=params, timeout=config.request_timeout)
            resp.raise_for_status()
            chunk = resp.json()
            for a in (chunk if isinstance(chunk, list) else [chunk]):
                a['_clinic'] = clinic
                a['_operatory'] = op
                appts.append(a)
    return appts

@retry
def fetch_patient(pat_num: int) -> Dict[str, Any]:
    resp = requests.get(f"{config.api_base_url}/patients/{pat_num}", headers=make_headers(), timeout=config.request_timeout)
    resp.raise_for_status()
    return resp.json()

@retry
def send_to_keragon(appt, patient):
    start, end, dur = get_times_pattern(appt)
    payload = {
        'appointmentId': str(appt.get('AptNum')),
        'appointmentTime': start.isoformat() if start else '',
        'appointmentEndTime': end.isoformat() if end else '',
        'appointmentDurationMinutes': dur,
        'status': appt.get('AptStatus'),
        'notes': appt.get('Note', '') + ' [fromOD]',
        'patientId': str(appt.get('PatNum')),
        'firstName': patient.get('FName',''),
        'lastName': patient.get('LName',''),
        'gender': patient.get('Gender',''),
        'address': patient.get('Address',''),
        'address2': patient.get('Address2',''),
        'city': patient.get('City',''),
        'state': patient.get('State',''),
        'zipCode': patient.get('Zip',''),
        'balance': patient.get('Balance',0)
    }
    clean = {k:v for k,v in payload.items() if v not in (None,'',0)}
    requests.post(config.keragon_webhook_url, json=clean, timeout=config.request_timeout).raise_for_status()
    return f"{patient.get('FName','')} {patient.get('LName','')} {start} → {end}"

# === MAIN ===
def run():
    if not all([config.developer_key, config.customer_key, config.keragon_webhook_url]):
        logger.critical("Missing credentials or webhook URL.")
        sys.exit(1)

    state = load_state()
    new_state = {}

    for clinic in config.clinic_nums:
        all_ops = fetch_operatories(clinic)
        ops = config.operatory_filters.get(clinic, all_ops)
        logger.info(f"Clinic {clinic}: found ops {all_ops}, filtering to {ops}")
        since = state.get(str(clinic))
        logger.info(f"Clinic {clinic}: {'First run' if not since else f'Subsequent run, filtering by DateTStamp: {since}'}")
        appts = fetch_appointments(clinic, ops, since)
        logger.info(f"Clinic {clinic}: total fetched {len(appts)}")

        to_send = []
        if since:
            to_send = [a for a in appts if parse_iso(a.get('DateTStamp')) and parse_iso(a['DateTStamp']) > parse_iso(since)]
            logger.info(f"Clinic {clinic}: filtered to {len(to_send)} new/modified appointments")
        else:
            to_send = appts
            logger.info(f"Clinic {clinic}: first run, sending all {len(to_send)}")

        patients = {}
        with ThreadPoolExecutor(max_workers=config.max_workers) as pool:
            futs = {pool.submit(fetch_patient, a['PatNum']): a['PatNum'] for a in to_send}
            for fut in as_completed(futs):
                patients[futs[fut]] = fut.result()

        for a in to_send:
            try:
                info = send_to_keragon(a, patients.get(a['PatNum'], {}))
                logger.info(f"[KERAGON SYNCED] {info}")
            except Exception as e:
                logger.error(f"Failed to send apt {a.get('AptNum')}: {e}")

        logger.info(f"Clinic {clinic}: done. Fetched={len(appts)} Sent={len(to_send)}")
        new_state[str(clinic)] = datetime.datetime.utcnow().isoformat()

    save_state(new_state)

if __name__ == '__main__':
    run()
