#!/usr/bin/env python3
import os
import sys
import json
import logging
import datetime
import requests
from zoneinfo import ZoneInfo
from typing import List, Dict, Any, Optional

# === CONFIGURATION ===
API_BASE_URL        = os.environ.get('OPEN_DENTAL_API_URL', 'https://api.opendental.com/api/v1')
DEVELOPER_KEY       = os.environ.get('OPEN_DENTAL_DEVELOPER_KEY')
CUSTOMER_KEY        = os.environ.get('OPEN_DENTAL_CUSTOMER_KEY')
KERAGON_WEBHOOK_URL = os.environ.get('KERAGON_WEBHOOK_URL')

STATE_FILE      = 'last_sync_state.json'
LOG_LEVEL       = os.environ.get('LOG_LEVEL', 'INFO')
LOOKAHEAD_HOURS = int(os.environ.get('LOOKAHEAD_HOURS', '720'))

CLINIC_NUMS = [
    int(x) for x in os.environ.get('CLINIC_NUMS', '').split(',')
    if x.strip().isdigit()
]

CLINIC_OPERATORY_FILTERS: Dict[int, List[int]] = {
    9034: [11579, 11580, 11588],
    9035: [11574, 11576, 11577],
}

VALID_STATUSES = {'Scheduled', 'Complete', 'Broken'}

# === LOGGING ===
logging.basicConfig(
    level=getattr(logging, LOG_LEVEL),
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[logging.StreamHandler(sys.stdout)]
)
logger = logging.getLogger('opendental_sync')

# === STATE MANAGEMENT ===
def load_last_sync_time() -> Optional[datetime.datetime]:
    if os.path.exists(STATE_FILE):
        try:
            with open(STATE_FILE) as f:
                data = json.load(f)
            ts = data.get('lastSync')
            if ts:
                return datetime.datetime.fromisoformat(ts)
        except Exception as e:
            logger.error(f"Error reading state file: {e}")
    return None


def save_last_sync_time(dt: datetime.datetime) -> None:
    try:
        with open(STATE_FILE, 'w') as f:
            json.dump({'lastSync': dt.isoformat()}, f, indent=2)
    except Exception as e:
        logger.error(f"Error saving state file: {e}")

# === UTILITIES ===
def parse_time(s: Optional[str]) -> Optional[datetime.datetime]:
    if not s:
        return None
    try:
        return datetime.datetime.fromisoformat(s.rstrip('Z'))
    except ValueError:
        logger.warning(f"Unrecognized time format: {s}")
        return None


def convert_to_tz(dt: datetime.datetime, tz: str = "America/Chicago") -> datetime.datetime:
    if dt.tzinfo:
        return dt.astimezone(ZoneInfo(tz))
    return dt.replace(tzinfo=ZoneInfo(tz))


def make_auth_header() -> Dict[str, str]:
    return {
        'Authorization': f'ODFHIR {DEVELOPER_KEY}/{CUSTOMER_KEY}',
        'Content-Type': 'application/json'
    }

# === PATTERN-BASED DURATION ===
def calculate_pattern_duration(pattern: str, minutes_per_slot: int = 5) -> int:
    if not pattern:
        return 0
    slots = sum(1 for ch in pattern.upper() if ch in ('X', '/'))
    return slots * minutes_per_slot


def calculate_end_time(start_dt: datetime.datetime, pattern: str) -> datetime.datetime:
    dur = calculate_pattern_duration(pattern)
    if dur <= 0:
        logger.warning(f"No slots in pattern '{pattern}', defaulting to 60 minutes")
        dur = 60
    return start_dt + datetime.timedelta(minutes=dur)

# === DATA FETCHERS ===
def get_filtered_ops(clinic: int) -> List[int]:
    return CLINIC_OPERATORY_FILTERS.get(clinic, [])


def fetch_appointments(
    clinic: int,
    since: Optional[datetime.datetime],
    ops: List[int]
) -> List[Dict[str, Any]]:
    now = datetime.datetime.utcnow()
    params = {
        'ClinicNum': clinic,
        'dateStart': now.strftime("%Y-%m-%d"),
        'dateEnd':   (now + datetime.timedelta(hours=LOOKAHEAD_HOURS)).strftime("%Y-%m-%d")
    }
    if since:
        # bump by 1 second to ensure strict > lastSync
        eff = since + datetime.timedelta(seconds=1)
        params['DateTStamp'] = eff.strftime("%Y-%m-%d %H:%M:%S")
        logger.info(f"Clinic {clinic}: fetching since strictly after {since.isoformat()}")

    try:
        r = requests.get(
            f"{API_BASE_URL}/appointments",
            headers=make_auth_header(),
            params=params,
            timeout=30
        )
        r.raise_for_status()
        data = r.json()
        appts = data if isinstance(data, list) else [data]
    except Exception as e:
        logger.error(f"Error fetching appointments for {clinic}: {e}")
        return []

    filtered = []
    for a in appts:
        if a.get('AptStatus') not in VALID_STATUSES:
            continue
        op = a.get('Op') or a.get('OperatoryNum')
        if ops and op not in ops:
            continue
        filtered.append(a)
    logger.info(f"Clinic {clinic}: {len(filtered)} valid appointment(s)")
    return filtered


def get_patient_details(pat_num: int) -> Dict[str, Any]:
    if not pat_num:
        return {}
    try:
        r = requests.get(
            f"{API_BASE_URL}/patients/{pat_num}",
            headers=make_auth_header(), timeout=15
        )
        r.raise_for_status()
        return r.json()
    except:
        return {}

# === KERAGON SYNC ===
def send_to_keragon(appt: Dict[str, Any]) -> bool:
    name = f"{appt.get('FName','')} {appt.get('LName','')}".strip() or 'Unknown'
    st_raw = appt.get('AptDateTime')
    st = parse_time(st_raw)
    if not st:
        logger.error(f"Invalid start time for Apt {appt.get('AptNum')}")
        return False
    st = convert_to_tz(st)
    en = calculate_end_time(st, appt.get('Pattern', ''))

    logger.info(
        f"Syncing Apt {appt.get('AptNum')} for {name} "
        f"from {st.isoformat()} to {en.isoformat()}"
    )

    patient = get_patient_details(appt.get('PatNum'))
    payload = {
        'appointmentId': str(appt.get('AptNum')),
        'appointmentTime': st.isoformat(),
        'appointmentEndTime': en.isoformat(),
        'appointmentDurationMinutes': int((en - st).total_seconds() / 60),
        'status': appt.get('AptStatus'),
        'notes': appt.get('Note', '') + ' [fromOD]',
        'patientId': str(appt.get('PatNum')),
        'firstName': patient.get('FName', appt.get('FName', '')),
        'lastName': patient.get('LName', appt.get('LName', '')),
        'email': patient.get('Email', ''),
        'phone': patient.get('WirelessPhone', '') or patient.get('HmPhone', ''),
        'gender': patient.get('Gender', ''),
        'birthdate': patient.get('Birthdate', ''),
        'address': patient.get('Address', ''),
        'city': patient.get('City', ''),
        'state': patient.get('State', ''),
        'zipCode': patient.get('Zip', ''),
        'balance': patient.get('Balance', 0)
    }
    try:
        r = requests.post(KERAGON_WEBHOOK_URL, json=payload, timeout=30)
        r.raise_for_status()
        return True
    except Exception as e:
        logger.error(f"Failed to send Apt {appt.get('AptNum')}: {e}")
        return False

# === MAIN SYNC ===
def run_sync():
    if not (DEVELOPER_KEY and CUSTOMER_KEY and KERAGON_WEBHOOK_URL and CLINIC_NUMS):
        logger.critical("Missing configuration – check your environment variables")
        sys.exit(1)

    last_sync = load_last_sync_time()
    now_utc = datetime.datetime.utcnow()
    logger.info(f"Starting sync; last_sync={last_sync}")

    total_sent = 0
    for clinic in CLINIC_NUMS:
        logger.info(f"--- Clinic {clinic} ---")
        ops = get_filtered_ops(clinic)
        appts = fetch_appointments(clinic, last_sync, ops)
        for a in appts:
            if send_to_keragon(a):
                total_sent += 1

    save_last_sync_time(now_utc)
    logger.info(f"Sync complete: sent {total_sent} appointments. Updated lastSync to {now_utc.isoformat()}")

# === CLI ===
if __name__ == '__main__':
    import argparse
    p = argparse.ArgumentParser(description='Sync OpenDental → Keragon')
    p.add_argument('--reset', action='store_true', help='Clear saved last sync timestamp and exit')
    p.add_argument('--verbose', action='store_true', help='Enable DEBUG logging')
    args = p.parse_args()

    if args.verbose:
        logger.setLevel(logging.DEBUG)
    if args.reset:
        if os.path.exists(STATE_FILE):
            os.remove(STATE_FILE)
            logger.info("State file cleared")
        sys.exit(0)

    run_sync()
