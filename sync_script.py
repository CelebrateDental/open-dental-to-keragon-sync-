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
def load_last_sync_times() -> Dict[int, Optional[datetime.datetime]]:
    state: Dict[int, Optional[datetime.datetime]] = {}
    if os.path.exists(STATE_FILE):
        try:
            with open(STATE_FILE) as f:
                data = json.load(f)
            for c in CLINIC_NUMS:
                ts = data.get(str(c))
                if ts:
                    try:
                        state[c] = datetime.datetime.fromisoformat(ts)
                    except ValueError:
                        logger.warning(f"Invalid timestamp for clinic {c}: {ts}")
                        state[c] = None
                else:
                    state[c] = None
        except Exception as e:
            logger.error(f"Error reading state file: {e}")
            state = {c: None for c in CLINIC_NUMS}
    else:
        state = {c: None for c in CLINIC_NUMS}
    return state


def save_last_sync_times(times: Dict[int, Optional[datetime.datetime]]) -> None:
    out: Dict[str, Optional[str]] = {}
    for c, dt in times.items():
        out[str(c)] = dt.isoformat() if dt else None
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

# === DURATION CALCULATION ===
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

# === APPOINTMENT FETCHING ===
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
        eff = since + datetime.timedelta(seconds=1)
        params['DateTStamp'] = eff.strftime("%Y-%m-%d %H:%M:%S")
        logger.info(f"Clinic {clinic}: fetching after {since.isoformat()}")
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

    valid = []
    for a in appts:
        if a.get('AptStatus') not in VALID_STATUSES:
            continue
        op = a.get('Op') or a.get('OperatoryNum')
        if ops and op not in ops:
            continue
        valid.append(a)
    logger.info(f"Clinic {clinic}: {len(valid)} appointment(s) to sync")
    return valid


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
    except Exception:
        return {}

# === PROVIDER FETCH ===
def get_provider_name(prov_id: Optional[int]) -> str:
    """
    Fetch provider via list endpoint by filter, since direct GET may not be supported.
    """
    if not prov_id:
        return ''
    try:
        rprov = requests.get(
            f"{API_BASE_URL}/providers",
            headers=make_auth_header(),
            params={'ProvNum': prov_id},
            timeout=15
        )
        rprov.raise_for_status()
        data = rprov.json()
        # data may be a list
        pr = data[0] if isinstance(data, list) and data else data
        return f"{pr.get('FName','').strip()} {pr.get('LName','').strip()}".strip()
    except Exception:
        logger.warning(f"Could not fetch provider {prov_id}")
        return ''

# === KERAGON SYNC ===
def send_to_keragon(appt: Dict[str, Any]) -> bool:
    patient = get_patient_details(appt.get('PatNum'))
    first = patient.get('FName') or appt.get('FName', '')
    last = patient.get('LName') or appt.get('LName', '')
    name = f"{first} {last}".strip() or 'Unknown'

    prov_id = appt.get('ProvNum') or appt.get('ProviderNum')
    provider_name = get_provider_name(prov_id)

    st = parse_time(appt.get('AptDateTime'))
    if not st:
        logger.error(f"Invalid start time for Apt {appt.get('AptNum')}")
        return False
    st = convert_to_tz(st)
    en = calculate_end_time(st, appt.get('Pattern', ''))

    logger.info(f"Syncing Apt {appt.get('AptNum')} for {name} "
                f"({provider_name}) from {st.isoformat()} to {en.isoformat()}")

    payload = {
        'appointmentId': str(appt.get('AptNum')),
        'appointmentTime': st.isoformat(),
        'appointmentEndTime': en.isoformat(),
        'appointmentDurationMinutes': int((en - st).total_seconds() / 60),
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

# === MAIN SYNC ===
def run_sync():
    if not (DEVELOPER_KEY and CUSTOMER_KEY and KERAGON_WEBHOOK_URL and CLINIC_NUMS):
        logger.critical("Missing configuration – check your environment variables")
        sys.exit(1)

    last_syncs = load_last_sync_times()
    new_syncs: Dict[int, datetime.datetime] = {}

    for clinic in CLINIC_NUMS:
        logger.info(f"--- Clinic {clinic} ---")
        last = last_syncs.get(clinic)
        appointments = fetch_appointments(clinic, last, get_filtered_ops(clinic))
        sent = 0
        for appt in appointments:
            if send_to_keragon(appt):
                sent += 1
        logger.info(f"Clinic {clinic}: sent {sent} appointment(s)")
        new_syncs[clinic] = datetime.datetime.utcnow()

    save_last_sync_times(new_syncs)
    logger.info("All clinics synced; state file updated.")

# === CLI ===
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
