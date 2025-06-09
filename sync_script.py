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

STATE_FILE = 'last_sync_state.json'
LOG_LEVEL           = os.environ.get('LOG_LEVEL', 'INFO')
OVERLAP_MINUTES     = int(os.environ.get('OVERLAP_MINUTES', '5'))
LOOKAHEAD_HOURS     = int(os.environ.get('LOOKAHEAD_HOURS', '720'))

CLINIC_NUMS = [
    int(x) for x in os.environ.get('CLINIC_NUMS', '').split(',')
    if x.strip().isdigit()
]

CLINIC_OPERATORY_FILTERS: Dict[int, List[int]] = {
    9034: [11579, 11580, 11588],
    9035: [11574, 11576, 11577],
}

logging.basicConfig(
    level=getattr(logging, LOG_LEVEL),
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[logging.StreamHandler(sys.stdout)]
)
logger = logging.getLogger('opendental_sync')


def load_last_sync_state() -> Dict[int, Dict[str, Any]]:
    if os.path.exists(STATE_FILE):
        try:
            with open(STATE_FILE, 'r') as f:
                data = json.load(f)
            result: Dict[int, Dict[str, Any]] = {}
            for k, v in data.items():
                cnum = int(k)
                last_sync_str = v.get('lastSync')
                did_full = v.get('didFullFetch', False)
                last_sync_dt = datetime.datetime.fromisoformat(last_sync_str) if last_sync_str else None
                result[cnum] = {'lastSync': last_sync_dt, 'didFullFetch': bool(did_full)}
            for c in CLINIC_NUMS:
                if c not in result:
                    result[c] = {'lastSync': None, 'didFullFetch': False}
            return result
        except Exception as e:
            logger.error(f"Error reading state file: {e}")
    return {c: {'lastSync': None, 'didFullFetch': False} for c in CLINIC_NUMS}


def save_last_sync_state(state: Dict[int, Dict[str, Any]]) -> None:
    try:
        serial: Dict[str, Any] = {}
        for k, v in state.items():
            last_str = v['lastSync'].isoformat() if v['lastSync'] else None
            serial[str(k)] = {'lastSync': last_str, 'didFullFetch': v['didFullFetch']}
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
    return {'Authorization': f'ODFHIR {DEVELOPER_KEY}/{CUSTOMER_KEY}', 'Content-Type': 'application/json'}


def list_operatories_for_clinic(clinic: int) -> List[Dict[str, Any]]:
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


def fetch_appointments(clinic: int, since: Optional[datetime.datetime], filtered_ops: List[int] = None) -> List[Dict[str, Any]]:
    endpoint = f"{API_BASE_URL}/appointments"
    headers = make_auth_header()
    now = datetime.datetime.utcnow()
    date_start = now.strftime("%Y-%m-%d")
    date_end = (now + datetime.timedelta(hours=LOOKAHEAD_HOURS)).strftime("%Y-%m-%d")
    all_appts: List[Dict[str, Any]] = []
    statuses = ['Scheduled', 'Complete', 'Broken']
    ops_iter = filtered_ops or [None]
    for op in ops_iter:
        for st in statuses:
            params = {
                'dateStart': date_start,
                'dateEnd': date_end,
                'ClinicNum': clinic,
                'AptStatus': st,
                'Limit': 1000
            }
            if op is not None:
                params['Op'] = op
            try:
                resp = requests.get(endpoint, headers=headers, params=params, timeout=30)
                resp.raise_for_status()
                data = resp.json()
                if isinstance(data, list):
                    for a in data:
                        if op is not None:
                            a.setdefault('Op', op)
                        all_appts.append(a)
            except Exception as e:
                logger.error(f"Error fetching {st}/Op{op}: {e}")
    logger.info(f"Raw fetched for clinic {clinic}: {len(all_appts)} records")
    if filtered_ops:
        before = len(all_appts)
        allowed = set(filtered_ops)
        all_appts = [a for a in all_appts if a.get('Op') in allowed]
        logger.info(f"Filtered out {before - len(all_appts)}; kept {len(all_appts)}")
    return all_appts


def filter_new_appointments(appts: List[Dict[str, Any]], since: Optional[datetime.datetime]) -> List[Dict[str, Any]]:
    if since is None:
        return []
    new_list: List[Dict[str, Any]] = []
    for a in appts:
        mod = parse_time(a.get('DateTStamp'))
        if mod and mod > since:
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
        logger.warning(f"Primary fetch failed for PatNum {pat_num}")
    try:
        resp = requests.get(f"{API_BASE_URL}/patients", headers=headers, params={'PatNum': pat_num}, timeout=15)
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
    apt_id = str(appt.get('AptNum', ''))
    original_note = appt.get('Note', '') or ""
    now = datetime.datetime.utcnow()

    # 1) Strip the GHL tag if present
    if "[fromGHL]" in original_note:
        cleaned = original_note.replace("[fromGHL]", "").strip()
        try:
            patch_url = f"{API_BASE_URL}/appointments/{apt_id}"
            resp = requests.patch(
                patch_url,
                headers=make_auth_header(),
                json={"Note": cleaned},
                timeout=30
            )
            resp.raise_for_status()
            logger.info(f"Stripped [fromGHL] from AptNum={apt_id}")
        except Exception as e:
            logger.error(f"Failed stripping [fromGHL] on AptNum={apt_id}: {e}")
        return True

    # 2) Build payload, append fromOpenDental
    raw_start = appt.get('AptDateTime')
    start_dt = parse_time(raw_start)
    if start_dt:
        central = ZoneInfo("America/Chicago")
        aware_start = start_dt.replace(tzinfo=central)
        iso_start = aware_start.isoformat()
    else:
        iso_start = None

    duration = appt.get('Pattern', '').count('X') * 10
    if start_dt and duration:
        end_dt = start_dt + datetime.timedelta(minutes=duration)
        aware_end = end_dt.replace(tzinfo=central)
        iso_end = aware_end.isoformat()
    else:
        iso_end = None

    tagged_note = f"{original_note} [fromOpenDental]"

    # Fetch patient details once
    patient = get_patient_details(appt.get('PatNum')) if appt.get('PatNum') else {}

    payload = {
        'firstName':         patient.get('FName', appt.get('FName', '')),
        'lastName':          patient.get('LName', appt.get('LName', '')),
        'email':             patient.get('Email', appt.get('Email', '')),
        'phone':             (patient.get('HmPhone')
                              or patient.get('WkPhone')
                              or patient.get('WirelessPhone')
                              or appt.get('HmPhone', '')),
        'appointmentTime':   iso_start or raw_start,
        'endTime':           iso_end,
        'locationId':        str(appt.get('ClinicNum', '')),
        'calendarId':        str(appt.get('Op', '')),
        'status':            appt.get('AptStatus', ''),
        'providerName':      appt.get('provAbbr', ''),
        'appointmentLength': appt.get('Pattern', ''),
        'notes':             tagged_note,
        'appointmentId':     apt_id,
        'patientId':         str(appt.get('PatNum', '')),
        # Newly added fields:
        'birthdate':         patient.get('Birthdate', ''),
        'zipCode':           patient.get('Zip', ''),
        'state':             patient.get('State', ''),
        'city':              patient.get('City', ''),
    }

    try:
        r = requests.post(KERAGON_WEBHOOK_URL, json=payload, timeout=30)
        r.raise_for_status()
        logger.info(f"Sent AptNum={apt_id} to Keragon with [fromOpenDental]")
        return True
    except Exception as e:
        logger.error(f"Keragon send failed for AptNum={apt_id}: {e}")
        return False

def process_appointments(clinic: int, appts: List[Dict[str, Any]], lastSync: Optional[datetime.datetime], didFullFetch: bool) -> Dict[str, Any]:
    now = datetime.datetime.utcnow()
    max_mod = lastSync or now
    to_send = appts if not didFullFetch or lastSync is None else filter_new_appointments(appts, lastSync)
    if didFullFetch and not to_send:
        logger.info(f"Clinic {clinic}: no updates since {lastSync}")
        return {'newLastSync': lastSync, 'didFullFetch': True}
    sent_count = 0
    for a in to_send:
        # send_to_keragon handles strip vs append
        success = send_to_keragon(a)
        if success:
            sent_count += 1
            # advance mod time so we don't resend
            max_mod = now if now > max_mod else max_mod
    logger.info(f"Clinic {clinic}: processed {sent_count}/{len(to_send)} appts")
    return {'newLastSync': max_mod, 'didFullFetch': True}


def run_sync():
    if not (DEVELOPER_KEY and CUSTOMER_KEY and KERAGON_WEBHOOK_URL):
        logger.critical("Missing API keys or webhook URL")
        sys.exit(1)
    if not CLINIC_NUMS:
        logger.critical("No CLINIC_NUMS defined")
        sys.exit(1)
    state = load_last_sync_state()
    new_state = {}
    for clinic in CLINIC_NUMS:
        list_operatories_for_clinic(clinic)
        lastSync = state[clinic]['lastSync']
        didFullFetch = state[clinic]['didFullFetch']
        appts = fetch_appointments(clinic, lastSync, get_filtered_operatories_for_clinic(clinic))
        result = process_appointments(clinic, appts, lastSync, didFullFetch)
        new_state[clinic] = {'lastSync': result['newLastSync'], 'didFullFetch': result['didFullFetch']}
    save_last_sync_state(new_state)
    logger.info("Sync complete")

if __name__ == "__main__":
    import argparse
    parser = argparse.ArgumentParser(description='Sync Open Dental appointments to Keragon')
    parser.add_argument('--once', action='store_true')
    parser.add_argument('--test', action='store_true')
    parser.add_argument('--dump-state', action='store_true')
    parser.add_argument('--reset', action='store_true')
    parser.add_argument('--verbose', action='store_true')
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
