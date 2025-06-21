# Rewriting the entire script with the requested modifications:
# - On first run: fetch all appointments in the window regardless of DateTStamp
# - On subsequent runs: fetch only appointments modified since last sync using DateTStamp
# - Log how many appointments are sent to Keragon after each run

#!/usr/bin/env python3
"""
OpenDental → Keragon Appointment Sync (Incremental Updates using DateTStamp)
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
from zoneinfo import ZoneInfo

# === CONFIGURATION ===
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

def load_state() -> Tuple[Dict[int, str], bool]:
    if not os.path.exists(config.state_file):
        return {}, True
    try:
        with open(config.state_file) as f:
            data = json.load(f)
        return data, False
    except:
        return {}, True

def save_state(state: Dict[int, str]):
    tmp = config.state_file + '.tmp'
    with open(tmp, 'w') as f:
        json.dump(state, f, indent=2)
    os.replace(tmp, config.state_file)

def make_headers() -> Dict[str, str]:
    return {'Authorization': f'ODFHIR {config.developer_key}/{config.customer_key}', 'Content-Type': 'application/json'}

def parse_iso(dt_str: Optional[str]) -> Optional[datetime.datetime]:
    if not dt_str:
        return None
    for fmt in ("%Y-%m-%dT%H:%M:%S.%f", "%Y-%m-%dT%H:%M:%S", "%Y-%m-%d %H:%M:%S", "%Y-%m-%d"):
        try:
            return datetime.datetime.strptime(dt_str, fmt)
        except ValueError:
            continue
    return None

def get_times_pattern(appt: Dict[str, Any]) -> Tuple[Optional[datetime.datetime], Optional[datetime.datetime], int]:
    start = parse_iso(appt.get('AptDateTime'))
    if start:
        start = start.replace(tzinfo=ZoneInfo(config.timezone))
    pattern = appt.get('Pattern') or ''
    duration = max(len(pattern) * 5, 5)
    end = start + datetime.timedelta(minutes=duration) if start else None
    return start, end, duration

@retry
def fetch_operatories(clinic: int) -> List[int]:
    params = {'ClinicNum': clinic}
    resp = requests.get(f"{config.api_base_url}/operatories", headers=make_headers(), params=params, timeout=config.request_timeout)
    resp.raise_for_status()
    data = resp.json()
    return [op['OperatoryNum'] for op in data] if isinstance(data, list) else []

@retry
def fetch_appointments(clinic: int, ops: List[int], since: Optional[str], first_run: bool) -> List[Dict[str, Any]]:
    now = datetime.datetime.utcnow()
    start_str = now.strftime("%Y-%m-%d")
    end_str = (now + datetime.timedelta(hours=config.lookahead_hours)).strftime("%Y-%m-%d")
    logger.info(f"Clinic {clinic}: fetching appointments from {start_str} to {end_str}")

    all_appts = []
    statuses = ['Scheduled', 'Complete', 'Broken']
    base = {'ClinicNum': clinic, 'dateStart': start_str, 'dateEnd': end_str}

    for status in statuses:
        for oper in ops:
            params = base.copy()
            params['AptStatus'] = status
            params['Op'] = oper
            if not first_run and since:
                params['DateTStamp'] = since
            resp = requests.get(f"{config.api_base_url}/appointments", headers=make_headers(), params=params, timeout=config.request_timeout)
            resp.raise_for_status()
            data = resp.json()
            items = data if isinstance(data, list) else [data]
            for ap in items:
                ap['_clinic'] = clinic
                ap['_operatory'] = oper
                all_appts.append(ap)
    return all_appts

@retry
def fetch_patient(pat_num: int) -> Dict[str, Any]:
    if not pat_num:
        return {}
    resp = requests.get(f"{config.api_base_url}/patients/{pat_num}", headers=make_headers(), timeout=config.request_timeout)
    resp.raise_for_status()
    return resp.json()

@retry
def send_to_keragon(appt: Dict[str, Any], patient: Dict[str, Any]) -> None:
    start, end, duration = get_times_pattern(appt)
    payload = {
        'appointmentId': str(appt.get('AptNum')),
        'appointmentTime': start.isoformat() if start else '',
        'appointmentEndTime': end.isoformat() if end else '',
        'appointmentDurationMinutes': duration,
        'status': appt.get('AptStatus'),
        'notes': appt.get('Note', '') + ' [fromOD]',
        'patientId': str(appt.get('PatNum')),
        'firstName': patient.get('FName', ''),
        'lastName': patient.get('LName', ''),
        'gender': patient.get('Gender', ''),
        'address': patient.get('Address', ''),
        'address2': patient.get('Address2', ''),
        'city': patient.get('City', ''),
        'state': patient.get('State', ''),
        'zipCode': patient.get('Zip', ''),
        'balance': patient.get('Balance', 0),
    }
    clean = {k: v for k, v in payload.items() if v not in (None, '', 0)}
    requests.post(config.keragon_webhook_url, json=clean, timeout=config.request_timeout).raise_for_status()

def run(force_first: bool = False):
    if not all([config.developer_key, config.customer_key, config.keragon_webhook_url]):
        logger.critical("Missing credentials or webhook URL")
        sys.exit(1)

    last_state, first_run = load_state()
    overall_first = force_first or first_run
    new_state: Dict[int, str] = {}
    total_sent = 0

    for clinic in config.clinic_nums:
        all_ops = fetch_operatories(clinic)
        logger.info(f"Clinic {clinic}: found operatories: {all_ops}")
        ops_to_process = config.operatory_filters.get(clinic, all_ops)
        logger.info(f"Clinic {clinic}: processing operatories: {ops_to_process}")

        since = None if overall_first else last_state.get(str(clinic))
        appts = fetch_appointments(clinic, ops_to_process, since, overall_first)
        logger.info(f"Clinic {clinic}: found {len(appts)} appointments to process")

        patients = {}
        with ThreadPoolExecutor(max_workers=config.max_workers) as executor:
            futures = {executor.submit(fetch_patient, ap['PatNum']): ap['PatNum'] for ap in appts}
            for fut in as_completed(futures):
                patients[futures[fut]] = fut.result()

        for appt in appts:
            try:
                send_to_keragon(appt, patients.get(appt.get('PatNum'), {}))
                total_sent += 1
            except Exception as e:
                logger.error(f"Error sending apt {appt.get('AptNum')}: {e}")

        logger.info(f"Clinic {clinic}: done. {len(appts)} appointments processed.")
        new_state[str(clinic)] = datetime.datetime.utcnow().isoformat()

    save_state(new_state)
    logger.info(f"Total appointments sent to Keragon in this run: {total_sent}")

if __name__ == '__main__':
    import argparse
    parser = argparse.ArgumentParser(description='OpenDental → Keragon Sync')
    parser.add_argument('--once', action='store_true', help='First-run full fetch')
    parser.add_argument('--reset', action='store_true', help='Clear sync state')
    parser.add_argument('--verbose', action='store_true', help='Debug logging')
    args = parser.parse_args()
    if args.verbose:
        logger.setLevel(logging.DEBUG)
    if args.reset:
        try: os.remove(config.state_file)
        except FileNotFoundError: pass
        sys.exit(0)
    run(force_first=args.once)

