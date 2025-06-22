#!/usr/bin/env python3
import os
import sys
import json
import logging
import datetime
import requests
from datetime import timezone, timedelta
from zoneinfo import ZoneInfo
from typing import List, Dict, Any, Optional

# For a fixed GMT-05:00 offset (no DST)
FIXED_MINUS_5 = timezone(timedelta(hours=-5))

# === CONFIGURATION ===
API_BASE_URL        = os.environ.get('OPEN_DENTAL_API_URL', 'https://api.opendental.com/api/v1')
DEVELOPER_KEY       = os.environ.get('OPEN_DENTAL_DEVELOPER_KEY')
CUSTOMER_KEY        = os.environ.get('OPEN_DENTAL_CUSTOMER_KEY')
KERAGON_WEBHOOK_URL = os.environ.get('KERAGON_WEBHOOK_URL')

STATE_FILE      = 'last_sync_state.json'
LOG_LEVEL       = os.environ.get('LOG_LEVEL', 'INFO')
LOOKAHEAD_HOURS = int(os.environ.get('LOOKAHEAD_HOURS', '720'))

CLINIC_NUMS = [int(x) for x in os.environ.get('CLINIC_NUMS', '').split(',') if x.strip().isdigit()]

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
def load_last_sync_times() -> Dict[int, Optional[datetime.datetime]]:
    state: Dict[int, Optional[datetime.datetime]] = {}
    if os.path.exists(STATE_FILE):
        try:
            with open(STATE_FILE) as f:
                data = json.load(f)
            for c in CLINIC_NUMS:
                ts = data.get(str(c))
                state[c] = datetime.datetime.fromisoformat(ts) if ts else None
        except Exception as e:
            logger.error(f"Error reading state file: {e}")
            state = {c: None for c in CLINIC_NUMS}
    else:
        state = {c: None for c in CLINIC_NUMS}
    return state


def save_last_sync_times(times: Dict[int, Optional[datetime.datetime]]) -> None:
    out = {str(c): dt.isoformat() if dt else None for c, dt in times.items()}
    try:
        with open(STATE_FILE, 'w') as f:
            json.dump(out, f, indent=2)
    except Exception as e:
        logger.error(f"Error saving state file: {e}")

# === UTILITIES ===
def parse_time(s: Optional[str]) -> Optional[datetime.datetime]:
    if not s:
        return None
    try:
        if s.endswith('Z'):
            s = s[:-1] + '+00:00'
        return datetime.datetime.fromisoformat(s)
    except ValueError:
        logger.warning(f"Unrecognized time format: {s}")
        return None


def make_auth_header() -> Dict[str, str]:
    return {'Authorization': f'ODFHIR {DEVELOPER_KEY}/{CUSTOMER_KEY}', 'Content-Type': 'application/json'}

# === DURATION CALCULATION ===
def calculate_pattern_duration(pattern: str, minutes_per_slot: int = 5) -> int:
    return sum(1 for ch in (pattern or '').upper() if ch in ('X', '/')) * minutes_per_slot


def calculate_end_time(start_dt: datetime.datetime, pattern: str) -> datetime.datetime:
    dur = calculate_pattern_duration(pattern)
    if dur <= 0:
        logger.warning(f"No slots in pattern '{pattern}', defaulting to 60 minutes")
        dur = 60
    return start_dt + datetime.timedelta(minutes=dur)

# === APPOINTMENT FETCHING ===
def get_filtered_ops(clinic: int) -> List[int]:
    return CLINIC_OPERATORY_FILTERS.get(clinic, [])


def fetch_appointments(
    clinic: int,
    since: Optional[datetime.datetime],
    ops: List[int]
) -> List[Dict[str, Any]]:
    now_utc = datetime.datetime.utcnow().replace(tzinfo=timezone.utc)
    params = {
        'ClinicNum': clinic,
        'dateStart': now_utc.strftime('%Y-%m-%d'),
        'dateEnd': (now_utc + datetime.timedelta(hours=LOOKAHEAD_HOURS)).strftime('%Y-%m-%d')
    }
    if since:
        eff_utc = (since + datetime.timedelta(seconds=1)).astimezone(timezone.utc)
        params['DateTStamp'] = eff_utc.strftime('%Y-%m-%d %H:%M:%S')
        logger.info(f"Clinic {clinic}: fetching after UTC {params['DateTStamp']}")
    logger.debug(f"→ GET /appointments with params: {params}")
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

    valid = [a for a in appts
             if a.get('AptStatus') in VALID_STATUSES
             and ((a.get('Op') or a.get('OperatoryNum')) in ops if ops else True)]
    logger.info(f"Clinic {clinic}: {len(valid)} appointment(s) to sync")
    logger.debug(f"← fetched IDs: {[a.get('AptNum') for a in valid]}")
    return valid

# === PATIENT ===
def get_patient_details(pat_num: int) -> Dict[str, Any]:
    if not pat_num:
        return {}
    try:
        r = requests.get(
            f"{API_BASE_URL}/patients/{pat_num}",
            headers=make_auth_header(),
            timeout=15
        )
        r.raise_for_status()
        return r.json()
    except Exception:
        return {}

# === SYNC ===
def send_to_keragon(appt: Dict[str, Any], clinic: int) -> bool:
    patient = get_patient_details(appt.get('PatNum'))
    first = patient.get('FName') or appt.get('FName', '')
    last = patient.get('LName') or appt.get('LName', '')
    name = f"{first} {last}".strip() or 'Unknown'
    provider_name = 'Dr. Gharbi' if clinic == 9034 else 'Dr. Ensley'

    st = parse_time(appt.get('AptDateTime'))
    if not st:
        logger.error(f"Invalid start time for Apt {appt.get('AptNum')}")
        return False
    st_utc = st.astimezone(timezone.utc)
    st_payload = st_utc.isoformat(timespec='seconds').replace('+00:00', 'Z')

    en_utc = calculate_end_time(st_utc, appt.get('Pattern', ''))
    en_payload = en_utc.isoformat(timespec='seconds').replace('+00:00', 'Z')

    logger.info(
        f"Syncing Apt {appt.get('AptNum')} for {name} "
        f"({provider_name}) from {st_payload} to {en_payload}"
    )

    payload = {
        'appointmentId': str(appt.get('AptNum')),
        'appointmentTime': st_payload,
        'appointmentEndTime': en_payload,
        'appointmentDurationMinutes': int((en_utc - st_utc).total_seconds() / 60),
        'status': appt.get('AptStatus'),
        'notes': appt.get('Note', '') + ' [fromOD]',
        'patientId': str(appt.get('PatNum')),
        'firstName': first,
        'lastName': last,
        'providerName': provider_name,
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

# === MAIN ===
def run_sync():
    if not (DEVELOPER_KEY and CUSTOMER_KEY and KERAGON_WEBHOOK_URL and CLINIC_NUMS):
        logger.critical("Missing configuration – check your environment variables")
        sys.exit(1)

    last_syncs = load_last_sync_times()
    new_syncs: Dict[int, datetime.datetime] = {}

    for clinic in CLINIC_NUMS:
        logger.info(f"--- Clinic {clinic} ---")
        appointments = fetch_appointments(clinic, last_syncs.get(clinic), get_filtered_ops(clinic))
        sent = 0
        for appt in appointments:
            if send_to_keragon(appt, clinic):
                sent += 1
        logger.info(f"Clinic {clinic}: sent {sent} appointment(s)")
        new_syncs[clinic] = datetime.datetime.now(timezone.utc)

    save_last_sync_times(new_syncs)
    logger.info("All clinics synced; state file updated.")

if __name__ == '__main__':
    import argparse
    parser = argparse.ArgumentParser(description='Sync OpenDental → Keragon')
    parser.add_argument('--reset', action='store_true', help='Clear saved last sync timestamps and exit')
    parser.add_argument('--verbose', action='store_true', help='Enable DEBUG logging')
    args = parser.parse_args()

    if args.verbose:
        logger.setLevel(logging.DEBUG)
    if args.reset and os.path.exists(STATE_FILE):
        os.remove(STATE_FILE)
        logger.info("State file cleared")
        sys.exit(0)

    run_sync()
