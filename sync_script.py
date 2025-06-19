#!/usr/bin/env python3
"""
OpenDental → Keragon Appointment Sync
- Primary: FHIR `/appointment` for start/end
- Fallback: ChartModules `/ProgNotes` for duration
- Logs detailed info per clinic and operatory
- Reports fetch window and completion per clinic
- Includes patient payload (gender, address, zip code, balance)
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
    fhir_base_url: str = os.environ.get('OPEN_DENTAL_FHIR_URL', 'https://api.opendental.com/fhir/v2')
    developer_key: str = os.environ.get('OPEN_DENTAL_DEVELOPER_KEY', '')
    customer_key: str = os.environ.get('OPEN_DENTAL_CUSTOMER_KEY', '')
    keragon_webhook_url: str = os.environ.get('KERAGON_WEBHOOK_URL', '')
    state_file: str = os.environ.get('STATE_FILE', 'last_sync_state.json')
    log_level: str = os.environ.get('LOG_LEVEL', 'INFO')
    lookahead_hours: int = int(os.environ.get('LOOKAHEAD_HOURS', '720'))
    max_workers: int = int(os.environ.get('MAX_WORKERS', '5'))
    request_timeout: int = int(os.environ.get('REQUEST_TIMEOUT', '30'))
    retry_attempts: int = int(os.environ.get('RETRY_ATTEMPTS', '3'))
    clinic_nums: List[int] = field(default_factory=lambda: [int(x) for x in os.environ.get('CLINIC_NUMS','').split(',') if x.strip().isdigit()])
    operatory_filters: Dict[int, List[int]] = field(default_factory=lambda: {
        9034: [11579, 11580, 11588],
        9035: [11574, 11576, 11577],
    })

# Initialize logger
config = Config()
logger = logging.getLogger('opendental_sync')
handler = logging.StreamHandler(sys.stdout)
handler.setFormatter(logging.Formatter('%(asctime)s - %(levelname)s - %(message)s'))
logger.addHandler(handler)
logger.setLevel(getattr(logging, config.log_level.upper(), logging.INFO))
logging.getLogger('urllib3').setLevel(logging.WARNING)

# === RETRY DECORATOR ===
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

# === STATE MANAGEMENT ===

def load_state() -> Tuple[Dict[int, datetime.datetime], bool]:
    if not os.path.exists(config.state_file):
        return {}, True
    try:
        with open(config.state_file) as f:
            data = json.load(f)
        return {int(k): datetime.datetime.fromisoformat(v) for k, v in data.items()}, False
    except:
        return {}, True


def save_state(state: Dict[int, datetime.datetime]):
    tmp = config.state_file + '.tmp'
    with open(tmp, 'w') as f:
        json.dump({str(k): v.isoformat() for k, v in state.items()}, f, indent=2)
    os.replace(tmp, config.state_file)

# === UTILITIES ===

def make_headers() -> Dict[str, str]:
    return {'Authorization': f'ODFHIR {config.developer_key}/{config.customer_key}'}


def parse_iso(dt_str: Optional[str]) -> Optional[datetime.datetime]:
    if not dt_str:
        return None
    for fmt in ("%Y-%m-%dT%H:%M:%S.%f", "%Y-%m-%dT%H:%M:%S", "%Y-%m-%d %H:%M:%S", "%Y-%m-%d"):
        try:
            return datetime.datetime.strptime(dt_str, fmt)
        except ValueError:
            continue
    return None

# === FHIR APPOINTMENT FETCH ===
@retry
def fetch_fhir_appointment(appt_id: str) -> Dict[str, Any]:
    url = f"{config.fhir_base_url}/appointment/{appt_id}"
    headers = {
        'Authorization': f'ODFHIR {config.developer_key}/{config.customer_key}',
        'Accept': 'application/fhir+json'
    }
    resp = requests.get(url, headers=headers, timeout=config.request_timeout)
    resp.raise_for_status()
    return resp.json()

# === CHARTMODULES PROGNOTES FETCH ===
@retry
def fetch_prognotes(pat_num: int) -> List[Dict[str, Any]]:
    url = f"{config.api_base_url}/chartmodules/{pat_num}/ProgNotes"
    headers = {**make_headers(), 'Content-Type': 'application/json'}
    resp = requests.get(url, headers=headers, timeout=config.request_timeout)
    resp.raise_for_status()
    return resp.json()

# === DURATION & TIMES ===
@retry
def get_times(appt: Dict[str, Any]) -> Tuple[Optional[datetime.datetime], Optional[datetime.datetime], int]:
    # Try FHIR first
    try:
        f_res = fetch_fhir_appointment(str(appt.get('AptNum')))
        start = parse_iso(f_res.get('start'))
        end = parse_iso(f_res.get('end'))
        if start and end:
            return start, end, int((end - start).total_seconds() / 60)
    except Exception:
        pass
    # Fallback to ProgNotes
    start = parse_iso(appt.get('AptDateTime'))
    duration = 60
    if start:
        try:
            notes = fetch_prognotes(appt.get('PatNum'))
            for note in notes:
                if note.get('AptNum') == appt.get('AptNum'):
                    h, m = note.get('Length', '0:60').split(':')
                    duration = int(h)*60 + int(m)
                    break
        except Exception:
            pass
    end = start + datetime.timedelta(minutes=duration) if start else None
    return start, end, duration

# === FETCH APPOINTMENTS ===
@retry
def fetch_appointments(clinic: int, since: Optional[datetime.datetime], first_run: bool) -> List[Dict[str, Any]]:
    now = datetime.datetime.utcnow()
    start_str = now.strftime("%Y-%m-%d")
    end_str = (now + datetime.timedelta(hours=config.lookahead_hours)).strftime("%Y-%m-%d")
    logger.info(f"Clinic {clinic}: fetching appointments from {start_str} to {end_str}")

    all_appts = []
    statuses = ['Scheduled', 'Complete', 'Broken']
    oper_list = config.operatory_filters.get(clinic, [None])
    base = {'ClinicNum': clinic, 'dateStart': start_str, 'dateEnd': end_str}

    for status in statuses:
        for oper in oper_list:
            params = {**base, 'AptStatus': status}
            if oper:
                params['Op'] = oper
            if not first_run and since:
                params['DateTStamp'] = since.isoformat()

            headers = {**make_headers(), 'Content-Type': 'application/json'}
            resp = requests.get(f"{config.api_base_url}/appointments", headers=headers, params=params, timeout=config.request_timeout)
            resp.raise_for_status()
            data = resp.json()
            items = data if isinstance(data, list) else [data]

            for ap in items:
                ap['_clinic'] = clinic
                ap['_operatory'] = oper
                all_appts.append(ap)

    return all_appts

# === PATIENT FETCH ===
@retry
def fetch_patient(pat_num: int) -> Dict[str, Any]:
    if not pat_num:
        return {}
    headers = {**make_headers(), 'Content-Type': 'application/json'}
    try:
        r = requests.get(f"{config.api_base_url}/patients/{pat_num}", headers=headers, timeout=config.request_timeout)
        r.raise_for_status()
        return r.json()
    except Exception:
        r = requests.get(f"{config.api_base_url}/patients", headers=headers, params={'PatNum': pat_num}, timeout=config.request_timeout)
        r.raise_for_status()
        arr = r.json()
        return arr[0] if isinstance(arr, list) else {}

# === SEND TO KERAGON ===
@retry
def send_to_keragon(appt: Dict[str, Any], patient: Dict[str, Any]) -> Dict[str, Any]:
    start, end, duration = get_times(appt)
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

    return {
        'clinic': appt.get('_clinic'),
        'operatory': appt.get('_operatory'),
        'apt': appt.get('AptNum'),
        'first': patient.get('FName', ''),
        'last': patient.get('LName', ''),
        'start': start.isoformat() if start else '',
        'end': end.isoformat() if end else ''
    }

# === MAIN SYNC & CLI ===

def run(force_first: bool = False):
    if not all([config.developer_key, config.customer_key, config.keragon_webhook_url]):
        logger.critical("Missing credentials or webhook URL")
        sys.exit(1)

    last_state, first_run = load_state()
    overall_first = force_first or first_run
    new_state: Dict[int, datetime.datetime] = {}
    processed: List[Dict[str, Any]] = []

    for clinic in config.clinic_nums:
        since = None if overall_first else last_state.get(clinic)
        appts = fetch_appointments(clinic, since, overall_first)
        logger.info(f"Processing {len(appts)} appointments for clinic {clinic}")

        patients: Dict[int, Dict[str, Any]] = {}
        with ThreadPoolExecutor(max_workers=config.max_workers) as executor:
            futures = {executor.submit(fetch_patient, ap['PatNum']): ap['PatNum'] for ap in appts}
            for fut in as_completed(futures):
                pat_num = futures[fut]
                patients[pat_num] = fut.result() or {}

        for appt in appts:
            try:
                info = send_to_keragon(appt, patients.get(appt.get('PatNum'), {}))
                processed.append(info)
            except Exception as e:
                logger.error(f"Error sending apt {appt.get('AptNum')}: {e}")

        logger.info(f"Finished processing clinic {clinic}")
        new_state[clinic] = datetime.datetime.utcnow()

    save_state(new_state)

    for rec in processed:
        logger.info(
            f"Synced apt {rec['apt']} (clinic {rec['clinic']}, operatory {rec['operatory']}) "
            f"for {rec['first']} {rec['last']} from {rec['start']} to {rec['end']}"
        )


if __name__ == '__main__':
    import argparse
    parser = argparse.ArgumentParser(description='OpenDental → Keragon Sync')
    parser.add_argument('--once', action='store_true', help='First-run fetch')
    parser.add_argument('--reset', action='store_true', help='Clear sync state')
    parser.add_argument('--verbose', action='store_true', help='Debug logging')
    args = parser.parse_args()
    if args.verbose:
        logger.setLevel(logging.DEBUG)
    if args.reset:
        try:
            os.remove(config.state_file)
        except FileNotFoundError:
            pass
        sys.exit(0)
    run(force_first=args.once)
