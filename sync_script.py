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
from collections import defaultdict

# === CONFIGURATION ===
API_BASE_URL        = os.environ.get('OPEN_DENTAL_API_URL', 'https://api.opendental.com/api/v1')
DEVELOPER_KEY       = os.environ.get('OPEN_DENTAL_DEVELOPER_KEY')
CUSTOMER_KEY        = os.environ.get('OPEN_DENTAL_CUSTOMER_KEY')
KERAGON_WEBHOOK_URL = os.environ.get('KERAGON_WEBHOOK_URL')

### (CHANGE) ###
### We now expect the state file to hold, for each clinic, both a lastSync timestamp and a
### boolean “didFullFetch” that tells us whether we have ever done the full‐30‐day send. ###
STATE_FILE = 'last_sync_state.json'

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
    9034: [11579, 11580,11588],
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

### (CHANGE) ###
### The state file now holds a dict mapping clinicNum → { "lastSync": isoString or None,
###                                                      "didFullFetch": bool }. ###
def load_last_sync_state() -> Dict[int, Dict[str, Any]]:
    if os.path.exists(STATE_FILE):
        try:
            with open(STATE_FILE, 'r') as f:
                data = json.load(f)
            # Convert each “lastSync” back into a datetime (or None), keep didFullFetch as bool
            result: Dict[int, Dict[str, Any]] = {}
            for k, v in data.items():
                cnum = int(k)
                last_sync_str = v.get('lastSync')
                did_full = v.get('didFullFetch', False)
                if last_sync_str:
                    last_sync_dt = datetime.datetime.fromisoformat(last_sync_str)
                else:
                    last_sync_dt = None
                result[cnum] = {
                    'lastSync': last_sync_dt,
                    'didFullFetch': bool(did_full)
                }
            # Make sure we have an entry for every clinic
            for c in CLINIC_NUMS:
                if c not in result:
                    result[c] = {'lastSync': None, 'didFullFetch': False}
            return result

        except Exception as e:
            logger.error(f"Error reading state file: {e}")
            # If we can’t parse it, fall back to “never synced anything for any clinic”
    # If no file or parse error, say for each clinic: lastSync=None, didFullFetch=False
    fallback = {}
    for c in CLINIC_NUMS:
        fallback[c] = {'lastSync': None, 'didFullFetch': False}
    return fallback

### (CHANGE) ###
### Serialize that same structure back to disk. ###
def save_last_sync_state(state: Dict[int, Dict[str, Any]]) -> None:
    try:
        serial: Dict[str, Any] = {}
        for k, v in state.items():
            if v['lastSync'] is not None:
                last_str = v['lastSync'].isoformat()
            else:
                last_str = None
            serial[str(k)] = {
                'lastSync': last_str,
                'didFullFetch': v['didFullFetch']
            }
        with open(STATE_FILE, 'w') as f:
            json.dump(serial, f, indent=2)
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

def fetch_appointments(clinic: int, since: Optional[datetime.datetime],
                       filtered_ops: List[int] = None) -> List[Dict[str, Any]]:
    """
    Fetch all appointments whose AptDateTime falls between today and today+LOOKAHEAD_HOURS,
    then return the raw list. We'll apply our “first‐run vs incremental” logic afterward.
    """
    endpoint = f"{API_BASE_URL}/appointments"
    headers = make_auth_header()
    now = datetime.datetime.utcnow()

    # ── FETCH WINDOW: always from “today” (UTC date) up to “today + LOOKAHEAD_HOURS” ──
    date_start = now.strftime("%Y-%m-%d")
    date_end   = (now + datetime.timedelta(hours=LOOKAHEAD_HOURS)).strftime("%Y-%m-%d")

    all_appts: List[Dict[str, Any]] = []
    statuses = ['Scheduled', 'Complete', 'Broken']

    if filtered_ops:
        for op in filtered_ops:
            for st in statuses:
                params = {
                    'dateStart': date_start,
                    'dateEnd':   date_end,
                    'ClinicNum': clinic,
                    'AptStatus': st,
                    'Op':        op,
                    'Limit':     1000
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
                'dateEnd':   date_end,
                'ClinicNum': clinic,
                'AptStatus': st,
                'Limit':     1000
            }
            try:
                resp = requests.get(endpoint, headers=headers, params=params, timeout=30)
                resp.raise_for_status()
                data = resp.json()
                if isinstance(data, list):
                    all_appts.extend(data)
            except (HTTPError, RequestException) as e:
                logger.error(f"Error fetching {st}: {e}")

    # Log everything we fetched
    logger.info(f"Raw fetched for clinic {clinic}:")
    for a in all_appts:
        logger.info(
            f"  AptNum={a.get('AptNum')} | Op={a.get('Op')} | "
            f"AptDateTime={a.get('AptDateTime')} | DateTStamp={a.get('DateTStamp')}"
        )

    # Client‐side operatory filter (again, just in case API returns something outside our filtered_ops)
    if filtered_ops:
        before = len(all_appts)
        allowed = set(filtered_ops)
        all_appts = [a for a in all_appts if a.get('Op') in allowed]
        logger.info(f"Filtered out {before - len(all_appts)}; kept {len(all_appts)} for ops {filtered_ops}")

    return all_appts

def filter_new_appointments(appts: List[Dict[str, Any]],
                            since: Optional[datetime.datetime]) -> List[Dict[str, Any]]:
    """
    ONCE we have the raw list, if this is *not* the first run, we only keep those whose
    DateTStamp > since.  (If since is None, we will treat that as “first run” and return []—the caller will know.)
    """
    if since is None:
        # signal to the caller that we haven't done any “incremental” run yet
        return []

    new_list: List[Dict[str, Any]] = []
    for a in appts:
        mod = parse_time(a.get('DateTStamp'))
        # keep only those modified AFTER our lastSync
        if mod and (since is not None) and (mod > since):
            new_list.append(a)
    logger.info(f"Kept {len(new_list)} new/updated appointments since {since.isoformat()}")
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

    # 1) Parse the naïve “AptDateTime”
    raw_start = appt.get('AptDateTime')       # e.g. "2025-06-05 08:00:00"
    start_dt  = parse_time(raw_start)         # datetime(2025, 6, 5, 8, 0, 0)

    if start_dt:
        # 2) Localize to America/Chicago (handles DST automatically)
        central = ZoneInfo("America/Chicago")
        aware_start = start_dt.replace(tzinfo=central)
        iso_start   = aware_start.isoformat() # e.g. "2025-06-05T08:00:00-05:00"
    else:
        iso_start = None

    # 3) Derive “end” time from “Pattern” (10 min per “X”)
    SLOT_INCREMENT = 10
    pattern        = appt.get('Pattern', "")
    num_slots      = pattern.count("X")
    duration_min   = num_slots * SLOT_INCREMENT

    if start_dt and duration_min:
        end_dt = start_dt + datetime.timedelta(minutes=duration_min)
        aware_end = end_dt.replace(tzinfo=central)
        iso_end = aware_end.isoformat()      # e.g. "2025-06-05T09:20:00-05:00"
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

def process_appointments(clinic: int,
                         appts: List[Dict[str, Any]],
                         lastSync: Optional[datetime.datetime],
                         didFullFetch: bool) -> Dict[str, Any]:
    """
    Two modes:
     1) If didFullFetch is False, send EVERY appt in this 30-day window.
     2) If didFullFetch is True, send ONLY those whose DateTStamp > lastSync.
    Return a dict { 'newLastSync': datetime, 'didFullFetch': True/False }.
    """
    now = datetime.datetime.utcnow()

    if not didFullFetch:
        # ── FIRST RUN FOR THIS CLINIC ──
        logger.info(f"Clinic {clinic}: first run → sending every appointment in the next 30 days.")
        sent_count = 0
        max_mod = lastSync if lastSync else now
        for a in appts:
            # (We’re ignoring DateTStamp filtering because it’s the first time.)
            if send_to_keragon(a):
                sent_count += 1
            # Track the latest DateTStamp so that subsequent runs can be incremental:
            mod = parse_time(a.get('DateTStamp'))
            if mod and (lastSync is None or mod > max_mod):
                max_mod = mod

        logger.info(f"Clinic {clinic}: first‐run processed {sent_count}/{len(appts)} appts.")
        # After the first‐run, we mark didFullFetch=True and set lastSync = “now” or max_mod
        return {
            'newLastSync': max_mod if max_mod else now,
            'didFullFetch': True
        }

    else:
        # ── SUBSEQUENT RUN FOR THIS CLINIC ──
        # We only send the “new or updated” appointments whose DateTStamp > lastSync
        if lastSync is None:
            # Defensive: if lastSync is somehow None despite didFullFetch=True, treat everything
            filtered = appts
        else:
            filtered = filter_new_appointments(appts, lastSync)

        if not filtered:
            logger.info(f"Clinic {clinic}: no new/updated appointments since {lastSync.isoformat()}.")
            # Keep lastSync unchanged
            return {
                'newLastSync': lastSync,
                'didFullFetch': True
            }

        sent_count = 0
        max_mod = lastSync
        for a in filtered:
            mod = parse_time(a.get('DateTStamp'))
            if mod and mod > max_mod:
                max_mod = mod
            if send_to_keragon(a):
                sent_count += 1

        logger.info(f"Clinic {clinic}: processed {sent_count}/{len(filtered)} updated appointments.")
        return {
            # Update lastSync to the newest DateTStamp of anything we just sent,
            # but never move lastSync into the future beyond “now”.
            'newLastSync': max_mod if max_mod else now,
            'didFullFetch': True
        }

# === MAIN SYNC ===

def run_sync():
    if not (DEVELOPER_KEY and CUSTOMER_KEY and KERAGON_WEBHOOK_URL):
        logger.critical("Missing API keys or webhook URL; check environment vars")
        sys.exit(1)
    if not CLINIC_NUMS:
        logger.critical("No CLINIC_NUMS defined; check environment vars")
        sys.exit(1)

    logger.info(f"Syncing clinics: {CLINIC_NUMS}")

    # Load a dict: clinicNum → {'lastSync': datetime or None, 'didFullFetch': bool}
    state = load_last_sync_state()
    new_state = {}

    for clinic in CLINIC_NUMS:
        # 1) Log operatories for verification
        list_operatories_for_clinic(clinic)

        # 2) What we stored last time for this clinic:
        clinic_state = state.get(clinic, {'lastSync': None, 'didFullFetch': False})
        lastSync = clinic_state['lastSync']       # datetime or None
        didFullFetch = clinic_state['didFullFetch']

        if lastSync:
            logger.info(f"Clinic {clinic}: lastSync={lastSync.isoformat()}, didFullFetch={didFullFetch}")
        else:
            logger.info(f"Clinic {clinic}: no lastSync recorded, didFullFetch={didFullFetch}")

        filt_ops = get_filtered_operatories_for_clinic(clinic)
        appts = fetch_appointments(clinic, lastSync, filt_ops)

        result = process_appointments(clinic, appts, lastSync, didFullFetch)

        # result is { 'newLastSync': datetime, 'didFullFetch': True }
        new_state[clinic] = {
            'lastSync': result['newLastSync'],
            'didFullFetch': result['didFullFetch']
        }

    # Save the updated state for next time
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
