#!/usr/bin/env python3
import os
import sys
import json
import logging
import datetime
import requests
from zoneinfo import ZoneInfo
from typing import List, Dict, Any, Optional
from requests.exceptions import HTTPError, RequestException

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
def load_last_sync_state() -> Dict[int, Dict[str, Any]]:
    if os.path.exists(STATE_FILE):
        try:
            with open(STATE_FILE) as f:
                data = json.load(f)
            state = {}
            for k, v in data.items():
                c = int(k)
                last = v.get('lastSync')
                state[c] = {
                    'lastSync': datetime.datetime.fromisoformat(last) if last else None,
                    'didFull': v.get('didFull', False)
                }
            for c in CLINIC_NUMS:
                if c not in state:
                    state[c] = {'lastSync': None, 'didFull': False}
            return state
        except Exception as e:
            logger.error(f"Error reading state: {e}")
    return {c: {'lastSync': None, 'didFull': False} for c in CLINIC_NUMS}

def save_last_sync_state(state: Dict[int, Dict[str, Any]]) -> None:
    out = {}
    for c, v in state.items():
        out[str(c)] = {
            'lastSync': v['lastSync'].isoformat() if v['lastSync'] else None,
            'didFull': v['didFull']
        }
    try:
        with open(STATE_FILE, 'w') as f:
            json.dump(out, f, indent=2)
    except Exception as e:
        logger.error(f"Error saving state: {e}")

# === UTILITIES ===
def parse_time(s: Optional[str]) -> Optional[datetime.datetime]:
    if not s:
        return None
    for fmt in ("%Y-%m-%dT%H:%M:%S","%Y-%m-%d %H:%M:%S"):
        try:
            return datetime.datetime.strptime(s, fmt)
        except ValueError:
            continue
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

# === PATTERN DURATION ONLY ===
def calculate_pattern_duration(pattern: str, minutes_per_slot: int = 5) -> int:
    """
    Count both 'X' and '/' in the primary pattern.
    Each represents one slot of minutes_per_slot (default 5 min).
    """
    if not pattern:
        return 0
    slots = sum(1 for ch in pattern.upper() if ch in ('X','/'))
    duration = slots * minutes_per_slot
    logger.debug(f"Pattern '{pattern}' → {slots} slots → {duration} minutes")
    return duration

def calculate_end_time(start_dt: datetime.datetime, pattern: str) -> datetime.datetime:
    """
    End = start + duration from pattern only.
    Falls back to 60 min if pattern yields zero.
    """
    if not start_dt:
        return None
    dur = calculate_pattern_duration(pattern)
    if dur <= 0:
        dur = 60
        logger.warning(f"No slots in pattern '{pattern}', defaulting 60m")
    return start_dt + datetime.timedelta(minutes=dur)

# === DATA FETCHERS ===
def list_operatories_for_clinic(clinic: int) -> List[int]:
    try:
        r = requests.get(
            f"{API_BASE_URL}/operatories",
            headers=make_auth_header(),
            params={'ClinicNum': clinic},
            timeout=15
        )
        r.raise_for_status()
        ops = r.json()
        nums = [o['OperatoryNum'] for o in ops]
        logger.info(f"Clinic {clinic}: operatory list = {nums}")
        return nums
    except Exception as e:
        logger.error(f"Failed to list operatories for {clinic}: {e}")
        return []

def get_filtered_ops(clinic: int) -> List[int]:
    f = CLINIC_OPERATORY_FILTERS.get(clinic)
    if f:
        logger.info(f"Clinic {clinic}: filtering to ops {f}")
        return f
    logger.info(f"Clinic {clinic}: no operatory filter")
    return []

def fetch_appointments(
    clinic: int,
    since: Optional[datetime.datetime],
    ops: List[int]
) -> List[Dict[str, Any]]:
    """
    First run (didFull=False): fetch next 30 days, no DateTStamp param.
    Subsequent: include DateTStamp=since to only pull new/updated.
    """
    now = datetime.datetime.utcnow()
    start = now.strftime("%Y-%m-%d")
    end   = (now + datetime.timedelta(hours=LOOKAHEAD_HOURS)).strftime("%Y-%m-%d")
    params = {
        'ClinicNum': clinic,
        'dateStart': start,
        'dateEnd':   end
    }
    if since:
        params['DateTStamp'] = since.strftime("%Y-%m-%d %H:%M:%S")
        logger.info(f"Clinic {clinic}: fetching updates since {params['DateTStamp']}")

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
        logger.error(f"Error fetching appts for clinic {clinic}: {e}")
        return []

    # filter status + op
    out = []
    for a in appts:
        if a.get('AptStatus') not in VALID_STATUSES:
            continue
        op = a.get('Op') or a.get('OperatoryNum')
        if ops and op not in ops:
            continue
        out.append(a)
    logger.info(f"Clinic {clinic}: fetched {len(appts)} raw, {len(out)} after filter")
    return out

def filter_new_appointments(
    appts: List[Dict[str, Any]],
    since: datetime.datetime
) -> List[Dict[str, Any]]:
    if not since:
        return appts
    new = []
    for a in appts:
        mod = parse_time(a.get('DateTStamp'))
        if mod and mod > since:
            new.append(a)
    logger.info(f"{len(new)}/{len(appts)} are new/updated since {since.isoformat()}")
    return new

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
        try:
            r = requests.get(
                f"{API_BASE_URL}/patients",
                headers=make_auth_header(),
                params={'PatNum': pat_num}, timeout=15
            )
            r.raise_for_status()
            d = r.json()
            if isinstance(d, list) and d:
                return d[0]
        except Exception:
            pass
    return {}

# === KERAGON SYNC ===
def send_to_keragon(appt: Dict[str, Any]) -> bool:
    # patient name
    name = f"{appt.get('FName','')} {appt.get('LName','')}".strip() or "Unknown"
    # start/end
    raw = appt.get('AptDateTime')
    st = parse_time(raw)
    if not st:
        logger.error(f"Invalid start time for apt {appt.get('AptNum')}")
        return False
    st = convert_to_tz(st)
    en = calculate_end_time(st, appt.get('Pattern',''))
    # log
    logger.info(
        f"Syncing Apt {appt.get('AptNum')} for {name} "
        f"on {st.date()} from {st.time()} to {en.time()}"
    )
    # build payload
    patient = get_patient_details(appt.get('PatNum'))
    payload = {
        'appointmentId': str(appt.get('AptNum')),
        'appointmentTime': st.isoformat(),
        'appointmentEndTime': en.isoformat(),
        'appointmentDurationMinutes': int((en-st).total_seconds()/60),
        'status': appt.get('AptStatus'),
        'notes': appt.get('Note','') + ' [fromOD]',
        'patientId': str(appt.get('PatNum')),
        'firstName': patient.get('FName', appt.get('FName','')),
        'lastName': patient.get('LName', appt.get('LName','')),
        'email': patient.get('Email',''),
        'phone': patient.get('WirelessPhone','') or patient.get('HmPhone',''),
        'gender': patient.get('Gender',''),
        'birthdate': patient.get('Birthdate',''),
        'address': patient.get('Address',''),
        'city': patient.get('City',''),
        'state': patient.get('State',''),
        'zipCode': patient.get('Zip',''),
        'balance': patient.get('Balance', 0)
    }
    try:
        r = requests.post(KERAGON_WEBHOOK_URL, json=payload, timeout=30)
        r.raise_for_status()
        logger.debug(f"Payload sent: {payload}")
        return True
    except Exception as e:
        logger.error(f"Failed to send Apt {appt.get('AptNum')}: {e}")
        return False

def process_appointments(
    clinic: int,
    appts: List[Dict[str, Any]],
    lastSync: Optional[datetime.datetime],
    didFull: bool
) -> Dict[str, Any]:
    now = datetime.datetime.utcnow()
    to_send = appts if not didFull or lastSync is None else filter_new_appointments(appts, lastSync)
    if not to_send:
        logger.info(f"Clinic {clinic}: no new appointments to sync")
        return {'newLastSync': lastSync, 'didFull': True}
    logger.info(f"Clinic {clinic}: syncing {len(to_send)} appointment(s)")
    for a in to_send:
        send_to_keragon(a)
    return {'newLastSync': now, 'didFull': True}

# === MAIN SYNC ===
def run_sync():
    if not (DEVELOPER_KEY and CUSTOMER_KEY and KERAGON_WEBHOOK_URL):
        logger.critical("Missing API keys or webhook URL")
        sys.exit(1)
    if not CLINIC_NUMS:
        logger.critical("No CLINIC_NUMS configured")
        sys.exit(1)

    logger.info(f"Starting sync for clinics: {CLINIC_NUMS}")
    state = load_last_sync_state()
    new_state = {}

    for clinic in CLINIC_NUMS:
        logger.info(f"--- Clinic {clinic} ---")
        _ = list_operatories_for_clinic(clinic)
        ops = get_filtered_ops(clinic)

        lastSync = state[clinic]['lastSync']
        didFull   = state[clinic]['didFull']
        appts = fetch_appointments(clinic, lastSync, ops)

        result = process_appointments(clinic, appts, lastSync, didFull)
        new_state[clinic] = {
            'lastSync': result['newLastSync'],
            'didFull': result['didFull']
        }

    save_last_sync_state(new_state)
    logger.info("Sync complete")

# === CLI ===
if __name__ == "__main__":
    import argparse
    p = argparse.ArgumentParser(description='Sync OpenDental → Keragon')
    p.add_argument('--reset', action='store_true', help='Clear state file')
    p.add_argument('--verbose', action='store_true', help='Debug logging')
    args = p.parse_args()

    if args.verbose:
        logger.setLevel(logging.DEBUG)
    if args.reset and os.path.exists(STATE_FILE):
        os.remove(STATE_FILE)
        logger.info("State file cleared")
        sys.exit(0)

    run_sync()
