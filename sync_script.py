#!/usr/bin/env python3
"""
OpenDental → Keragon Appointment Sync
✅ Uses pattern length for duration
✅ On first run: fetch all for next 30 days, no filter
✅ On later runs: fetch all, filter locally by DateTStamp
✅ Logs patient name + start/end nicely
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

def load_state() -> Tuple[Dict[str, str], bool]:
    if not os.path.exists(config.state_file):
        return {}, True
    try:
        with open(config.state_file) as f:
            return json.load(f), False
    except:
        return {}, True

def save_state(state: Dict[str, str]):
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
def fetch_appointments(clinic: int, ops: List[int]) -> List[Dict[str, Any]]:
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
def send_to_keragon(appt: Dict[str, Any], patient: Dict[str, Any]) -> Dict[str, Any]:
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
    }
    requests.post(config.keragon_webhook_url, json=payload, timeout=config.request_timeout).raise_for_status()
    return {
        'clinic': appt.get('_clinic'),
        'operatory': appt.get('_operatory'),
        'apt': appt.get('AptNum'),
        'first': patient.get('FName', ''),
        'last': patient.get('LName', ''),
        'start': start.isoformat() if start else '',
        'end': end.isoformat() if end else ''
    }

def run(force_first: bool = False):
    if not all([config.developer_key, config.customer_key, config.keragon_webhook_url]):
        logger.critical("Missing credentials or webhook URL")
        sys.exit(1)

    last_state, first_run_flag = load_state()
    new_state: Dict[str, str] = {}

    for clinic in config.clinic_nums:
        all_ops = fetch_operatories(clinic)
        ops_to_use = config.operatory_filters.get(clinic, all_ops)
        logger.info(f"Clinic {clinic}: found ops {all_ops}, using ops {ops_to_use}")

        since = last_state.get(str(clinic))
        if since and not force_first:
            logger.info(f"Clinic {clinic}: Subsequent run, will filter locally using DateTStamp > {since}")
        else:
            logger.info(f"Clinic {clinic}: First run, no filter")

        appts = fetch_appointments(clinic, ops_to_use)
        logger.info(f"Clinic {clinic}: total fetched appointments: {len(appts)}")

        if since and not force_first:
            since_dt = parse_iso(since)
            to_send = [a for a in appts if parse_iso(a.get('DateTStamp')) and parse_iso(a.get('DateTStamp')) > since_dt]
        else:
            to_send = appts

        logger.info(f"Clinic {clinic}: appointments after filter: {len(to_send)}")

        patients = {}
        with ThreadPoolExecutor(max_workers=config.max_workers) as executor:
            futures = {executor.submit(fetch_patient, a['PatNum']): a['PatNum'] for a in to_send}
            for fut in as_completed(futures):
                patients[futures[fut]] = fut.result()

        for appt in to_send:
            info = send_to_keragon(appt, patients.get(appt.get('PatNum'), {}))
            logger.info(f"[KERAGON] {info['first']} {info['last']} | {info['start']} → {info['end']}")

        new_state[str(clinic)] = datetime.datetime.utcnow().isoformat()

    save_state(new_state)

if __name__ == '__main__':
    import argparse
    parser = argparse.ArgumentParser(description='OpenDental → Keragon Sync')
    parser.add_argument('--once', action='store_true', help='Force first run')
    parser.add_argument('--reset', action='store_true', help='Clear sync state')
    parser.add_argument('--verbose', action='store_true', help='Debug logging')
    args = parser.parse_args()
    if args.verbose:
        logger.setLevel(logging.DEBUG)
    if args.reset:
        os.remove(config.state_file)
        sys.exit(0)
    run(force_first=args.once)
