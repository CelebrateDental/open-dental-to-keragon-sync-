#!/usr/bin/env python3
import os
import sys
import json
import logging
import datetime
import requests
from typing import List, Dict, Any, Optional
from requests.exceptions import HTTPError, RequestException
from collections import defaultdict

# === CONFIGURATION ===
API_BASE_URL        = os.environ.get('OPEN_DENTAL_API_URL', 'https://api.opendental.com/api/v1')
DEVELOPER_KEY       = os.environ.get('OPEN_DENTAL_DEVELOPER_KEY')
CUSTOMER_KEY        = os.environ.get('OPEN_DENTAL_CUSTOMER_KEY')
KERAGON_WEBHOOK_URL = os.environ.get('KERAGON_WEBHOOK_URL')
STATE_FILE          = 'last_sync_state.json'
LOG_LEVEL           = os.environ.get('LOG_LEVEL', 'INFO')

# Sync window params
OVERLAP_MINUTES     = int(os.environ.get('OVERLAP_MINUTES', '5'))
LOOKAHEAD_HOURS     = int(os.environ.get('LOOKAHEAD_HOURS', '720'))  # 30 days = 30×24 hrs

# Pull clinic numbers dynamically from env
CLINIC_NUMS = [
    int(x) for x in os.environ.get('CLINIC_NUMS', '').split(',')
    if x.strip().isdigit()
]

# Map each ClinicNum → the list of operatories you care about
CLINIC_OPERATORY_FILTERS: Dict[int, List[int]] = {
    9034: [11579, 11580],
    9035: [11574, 11576, 11577],
}

# === LOGGER SETUP ===
logging.basicConfig(
    level=getattr(logging, LOG_LEVEL),
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[logging.StreamHandler(sys.stdout)]
)
logger = logging.getLogger('opendental_sync')

# === UTILITIES ===

def load_last_sync_state() -> Dict[int, datetime.datetime]:
    if os.path.exists(STATE_FILE):
        try:
            with open(STATE_FILE, 'r') as f:
                data = json.load(f)
            return {int(k): datetime.datetime.fromisoformat(v) for k, v in data.items()}
        except Exception as e:
            logger.error(f"Error reading state file: {e}")
    # First-run fallback: look back 24 hours
    cutoff = datetime.datetime.utcnow() - datetime.timedelta(hours=24)
    return {c: cutoff for c in CLINIC_NUMS}

def save_last_sync_state(state: Dict[int, datetime.datetime]) -> None:
    try:
        serial = {str(k): v.isoformat() for k, v in state.items()}
        with open(STATE_FILE, 'w') as f:
            json.dump(serial, f)
    except Exception as e:
        logger.error(f"Error saving state file: {e}")

def parse_time(ts: Optional[str]) -> Optional[datetime.datetime]:
    if not ts:
        return None
    for fmt in ("%Y-%m-%dT%H:%M:%S", "%Y-%m-%d %H:%M:%S", "%Y-%m-%d"):
        try:
            return datetime.datetime.strptime(ts, fmt)
        except ValueError:
            continue
    logger.warning(f"Unrecognized time format: {ts}")
    return None

def make_auth_header() -> Dict[str, str]:
    return {
        'Authorization': f'ODFHIR {DEVELOPER_KEY}/{CUSTOMER_KEY}',
        'Content-Type': 'application/json'
    }

def list_operatories_for_clinic(clinic: int) -> List[Dict[str, Any]]:
    """
    Fetch and log all operatories for a given clinic before syncing appointments.
    """
    endpoint = f"{API_BASE_URL}/operatories"
    headers = make_auth_header()
    try:
        resp = requests.get(endpoint, headers=headers, params={'ClinicNum': clinic}, timeout=15)
        resp.raise_for_status()
        ops = resp.json()
        logger.info(f"Operatories for clinic {clinic}:")
        for o in ops:
            logger.info(f"  OperatoryNum={o.get('OperatoryNum')} | OpName={o.get('OpName','')}")
        return ops
    except Exception as e:
        logger.error(f"Failed to list operatories for clinic {clinic}: {e}")
        return []

def get_filtered_operatories_for_clinic(clinic: int) -> List[int]:
    filt = CLINIC_OPERATORY_FILTERS.get(clinic, [])
    if filt:
        logger.info(f"Clinic {clinic}: filtering to ops {filt}")
    else:
        logger.info(f"Clinic {clinic}: no operatory filter, will fetch all")
    return filt

# === API INTERACTION ===

def fetch_appointments(clinic: int, since: datetime.datetime,
                       filtered_ops: List[int] = None) -> List[Dict[str, Any]]:
    endpoint = f"{API_BASE_URL}/appointments"
    headers = make_auth_header()
    now = datetime.datetime.utcnow()
    date_start = (since - datetime.timedelta(minutes=OVERLAP_MINUTES)).strftime("%Y-%m-%d")
    date_end = (now + datetime.timedelta(hours=LOOKAHEAD_HOURS)).strftime("%Y-%m-%d")

    all_appts: List[Dict[str, Any]] = []
    statuses = ['Scheduled', 'Complete', 'Broken']

    if filtered_ops:
        for op in filtered_ops:
            for st in statuses:
                params = {
                    'dateStart': date_start,
                    'dateEnd': date_end,
                    'ClinicNum': clinic,
                    'AptStatus': st,
                    'Op': op,
                    'Limit': 100
                }
                try:
                    resp = requests.get(endpoint, headers=headers, params=params, timeout=30)
                    resp.raise_for_status()
                    data = resp.json()
                    if isinstance(data, list):
                        for a in data:
                            a.setdefault('Op', op)
                        all_appts.extend(data)
                except (HTTPError, RequestException) as e:
                    logger.error(f"Error fetching {st}/Op{op}: {e}")
    else:
        for st in statuses:
            params = {
                'dateStart': date_start,
                'dateEnd': date_end,
                'ClinicNum': clinic,
                'AptStatus': st,
                'Limit': 100
            }
            try:
                resp = requests.get(endpoint, headers=headers, params=params, timeout=30)
                resp.raise_for_status()
                data = resp.json()
                if isinstance(data, list):
                    all_appts.extend(data)
            except (HTTPError, RequestException) as e:
                logger.error(f"Error fetching {st}: {e}")

    # Raw fetched logging
    logger.info(f"Raw fetched for clinic {clinic}:")
    for a in all_appts:
        logger.info(
            f"  AptNum={a.get('AptNum')} | Op={a.get('Op')} | "
            f"AptDateTime={a.get('AptDateTime')} | DateTStamp={a.get('DateTStamp')}"
        )

    # Client-side filter
    if filtered_ops:
        before = len(all_appts)
        allowed = set(filtered_ops)
        all_appts = [a for a in all_appts if a.get('Op') in allowed]
        logger.info(f"Filtered out {before - len(all_appts)}; kept {len(all_appts)} for ops {filtered_ops}")

    return all_appts

def filter_new_appointments(appts: List[Dict[str, Any]], since: datetime.datetime) -> List[Dict[str, Any]]:
    new_list = []
    for a in appts:
        mod = parse_time(a.get('DateTStamp'))
        if mod and mod >= since:
            new_list.append(a)
    logger.info(f"Kept {len(new_list)} new appointments since {since.isoformat()}")
    return new_list

def get_patient_details(pat_num: int) -> Dict[str, Any]:
    if not pat_num:
        return {}
    endpoint = f"{API_BASE_URL}/patients/{pat_num}"
    headers = make_auth_header()
    try:
        resp = requests.get(endpoint, headers=headers, timeout=15)
        resp.raise_for_status()
        return resp.json()
    except Exception:
        logger.warning(f"Primary fetch failed for PatNum {pat_num}, trying fallback...")
    try:
        resp = requests.get(
            f"{API_BASE_URL}/patients", headers=headers, params={'PatNum': pat_num}, timeout=15
        )
        resp.raise_for_status()
        data = resp.json()
        if isinstance(data, list) and data:
            return data[0]
        if isinstance(data, dict):
            return data
    except Exception as e:
        logger.error(f"Fallback patient fetch failed: {e}")
    return {}

def send_to_keragon(appt: Dict[str, Any]) -> bool:
    pat_num = appt.get('PatNum')
    patient = get_patient_details(pat_num) if pat_num else {}

    # 1) Parse and tag start time with fixed CST (UTC–06:00)
    raw_start = appt.get('AptDateTime')  # e.g. "2025-06-05 08:00:00"
    start_dt = parse_time(raw_start)     # naive datetime(2025, 6, 5, 8, 0, 0)
    if start_dt:
        from datetime import timezone, timedelta
        fixed_cst = timezone(timedelta(hours=-6))
        aware_start = start_dt.replace(tzinfo=fixed_cst)
        iso_start = aware_start.isoformat()         # "2025-06-05T08:00:00-06:00"
    else:
        iso_start = None

    # 2) Derive duration from Pattern (each “X” = 10 minutes)
    pattern = appt.get('Pattern', "")
    SLOT_INCREMENT = 10  # 10 minutes per “X”
    num_slots = pattern.count("X")               # e.g. "////XX////" → 2 X’s
    duration_minutes = num_slots * SLOT_INCREMENT  # e.g. 2 × 10 = 20 minutes

    # 3) Compute end time = start + duration_minutes
    if start_dt and duration_minutes:
        end_dt = start_dt + datetime.timedelta(minutes=duration_minutes)
        aware_end = end_dt.replace(tzinfo=fixed_cst)
        iso_end = aware_end.isoformat()               # e.g. "2025-06-05T08:20:00-06:00"
    else:
        iso_end = None

    payload = {
        'firstName':         patient.get('FName', appt.get('FName', '')),
        'lastName':          patient.get('LName', appt.get('LName', '')),
        'email':             patient.get('Email', appt.get('Email', '')),
        'phone':             (patient.get('HmPhone') 
                              or patient.get('WkPhone') 
                              or patient.get('WirelessPhone') 
                              or appt.get('HmPhone', '')),
        'appointmentTime':   iso_start or appt.get('AptDateTime'),
        'endTime':           iso_end,
        'locationId':        str(appt.get('ClinicNum', '')),
        'calendarId':        str(appt.get('Op', '')),
        'status':            appt.get('AptStatus', ''),
        'providerName':      appt.get('provAbbr', ''),
        'appointmentLength': appt.get('Pattern', ''),
        'notes':             appt.get('Note', ''),
        'appointmentId':     str(appt.get('AptNum', '')),
        'patientId':         str(appt.get('PatNum', '')),
        'birthdate':         patient.get('Birthdate', ''),
        'zipCode':           patient.get('Zip', ''),
        'gender':            patient.get('Gender', ''),
        'clinicName':        patient.get('ClinicName', appt.get('ClinicName', '')),
        'address':           patient.get('Address', ''),
        'address2':          patient.get('Address2', ''),
        'city':              patient.get('City', ''),
        'state':             patient.get('State', ''),
        'balanceTotal':      patient.get('BalTotal', 0.0),
        # If Keragon needs an assignedUserId, add 'assignedUserId': 'YOUR_ID' here
    }
    try:
        r = requests.post(KERAGON_WEBHOOK_URL, json=payload, timeout=30)
        r.raise_for_status()
        logger.info(
            f"Sent to Keragon: {payload['firstName']} {payload['lastName']} "
            f"(Op {payload['calendarId']}) start={payload['appointmentTime']} end={payload['endTime']}"
        )
        return True
    except Exception as e:
        logger.error(f"Keragon send failed: {e}")
        return False

def process_appointments(clinic: int, appts: List[Dict[str, Any]], last_sync: datetime.datetime) -> datetime.datetime:
    if not appts:
        return last_sync
    new_appts = filter_new_appointments(appts, last_sync)
    if not new_appts:
        return last_sync

    now = datetime.datetime.utcnow()
    max_mod = last_sync
    success = 0
    for a in new_appts:
        mod = parse_time(a.get('DateTStamp'))
        if mod and mod > max_mod:
            max_mod = mod
        if send_to_keragon(a):
            success += 1
    logger.info(f"Clinic {clinic}: processed {success}/{len(new_appts)} new appointments")
    return min(max_mod, now)

# === MAIN SYNC ===

def run_sync():
    if not (DEVELOPER_KEY and CUSTOMER_KEY and KERAGON_WEBHOOK_URL):
        logger.critical("Missing API keys or webhook URL; check environment vars")
        sys.exit(1)
    if not CLINIC_NUMS:
        logger.critical("No CLINIC_NUMS defined; check environment vars")
        sys.exit(1)

    logger.info(f"Syncing clinics: {CLINIC_NUMS}")
    last_state = load_last_sync_state()
    new_state = last_state.copy()

    for clinic in CLINIC_NUMS:
        # Log operatories for verification
        list_operatories_for_clinic(clinic)

        since = last_state.get(clinic, datetime.datetime.utcnow() - datetime.timedelta(hours=24))
        logger.info(f"Clinic {clinic}: since {since.isoformat()}")
        filt_ops = get_filtered_operatories_for_clinic(clinic)
        appts = fetch_appointments(clinic, since, filt_ops)
        new_state[clinic] = process_appointments(clinic, appts, since)

    save_last_sync_state(new_state)
    logger.info("Sync complete")

if __name__ == "__main__":
    import argparse
    parser = argparse.ArgumentParser(description='Sync Open Dental appointments to Keragon')
    parser.add_argument('--once', action='store_true', help='Run sync once and exit')
    parser.add_argument('--test', action='store_true', help='Test connectivity only')
    parser.add_argument('--dump-state', action='store_true', help='Print last sync state and exit')
    parser.add_argument('--reset', action='store_true', help='Reset state file and exit')
    parser.add_argument('--verbose', action='store_true', help='Enable debug logging')
    args = parser.parse_args()

    if args.verbose:
        logger.setLevel(logging.DEBUG)
    if args.dump_state:
        if os.path.exists(STATE_FILE):
            print(json.dumps(json.load(open(STATE_FILE)), indent=2))
        else:
            print("No state file exists.")
        sys.exit(0)
    if args.reset and os.path.exists(STATE_FILE):
        os.remove(STATE_FILE)
        logger.info("State file removed.")
        sys.exit(0)
    if args.test:
        sys.exit(0)

    run_sync()
