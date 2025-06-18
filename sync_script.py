#!/usr/bin/env python3
"""
OpenDental → Keragon Appointment Sync
- Parses correct OpenDental patterns for appointment duration
- Includes full patient payload (gender, address, zip code)
- Optimized API calls, retries, and threading
"""

import os
import sys
import json
import logging
import datetime
import requests
import time
from typing import List, Dict, Any, Optional
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
    clinic_nums: List[int] = field(default_factory=lambda: (
        [int(x) for x in os.environ.get('CLINIC_NUMS','').split(',') if x.strip().isdigit()]
    ))
    operatory_filters: Dict[int, List[int]] = field(default_factory=lambda: {
        9034: [11579, 11580, 11588],
        9035: [11574, 11576, 11577],
    })

# === LOGGING SETUP ===
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
                delay = 2 ** i
                logger.warning(f"Retry {i+1}/{config.retry_attempts} after error: {e}. Waiting {delay}s.")
                time.sleep(delay)
        logger.error(f"All retries failed: {last_exc}")
        raise last_exc
    return wrapper

# === STATE MANAGEMENT ===
def load_state() -> (Dict[int, datetime.datetime], bool):
    if not os.path.exists(config.state_file):
        logger.info("No state file; first run.")
        return {}, True
    try:
        with open(config.state_file) as f:
            data = json.load(f)
        state = {int(k): datetime.datetime.fromisoformat(v) for k, v in data.items()}
        logger.info(f"Loaded state for clinics: {list(state.keys())}")
        return state, False
    except Exception as e:
        logger.error(f"Failed to load state: {e}. Treating as first run.")
        return {}, True

def save_state(state: Dict[int, datetime.datetime]):
    tmp = config.state_file + '.tmp'
    with open(tmp, 'w') as f:
        json.dump({str(k): v.isoformat() for k, v in state.items()}, f, indent=2)
    os.replace(tmp, config.state_file)
    logger.info("State saved.")

# === UTILITIES ===
def make_headers() -> Dict[str, str]:
    return {
        'Authorization': f'ODFHIR {config.developer_key}/{config.customer_key}',
        'Content-Type': 'application/json'
    }

def parse_iso(dt_str: Optional[str]) -> Optional[datetime.datetime]:
    if not dt_str:
        return None
    for fmt in ("%Y-%m-%dT%H:%M:%S.%f","%Y-%m-%dT%H:%M:%S","%Y-%m-%d %H:%M:%S","%Y-%m-%d"):
        try:
            return datetime.datetime.strptime(dt_str, fmt)
        except ValueError:
            continue
    logger.warning(f"Could not parse datetime '{dt_str}'")
    return None

# === DURATION CALCULATION ===
# Try API fields first, then explicit end time, then pattern+secondary

def get_appointment_duration(appt: Dict[str, Any]) -> (int, datetime.datetime):
    # 1. Check explicit duration fields
    for key in ('Length','Minutes','PatternLength','TotalTime','LengthTime'):
        val = appt.get(key)
        try:
            if val and int(val) > 0:
                mins = int(val)
                start = parse_iso(appt.get('AptDateTime'))
                return mins, start + datetime.timedelta(minutes=mins) if start else None
        except Exception:
            pass
    # 2. Check explicit end time
    start = parse_iso(appt.get('AptDateTime'))
    end = parse_iso(appt.get('AptDateTimeEnd') or appt.get('EndTime'))
    if start and end:
        mins = int((end - start).total_seconds()/60)
        return mins, end
    # 3. Fallback to pattern + patternSecondary
    pattern = appt.get('Pattern','') or ''
    sec = appt.get('PatternSecondary','') or ''
    count = pattern.count('X') + sec.count('X')
    if count:
        mins = min(max(count*10,15),480)
        return mins, start + datetime.timedelta(minutes=mins) if start else None
    # Default
    return 60, start + datetime.timedelta(minutes=60) if start else None

# === FETCH APPOINTMENTS ===
@retry
def fetch_appointments(clinic: int, since: Optional[datetime.datetime], first_run: bool) -> List[Dict[str, Any]]:
    now = datetime.datetime.utcnow()
    start = now.strftime("%Y-%m-%d")
    end = (now + datetime.timedelta(hours=config.lookahead_hours)).strftime("%Y-%m-%d")
    all_appts: List[Dict[str, Any]] = []
    statuses = ['Scheduled','Complete','Broken']
    ops = config.operatory_filters.get(clinic,[None])
    base = {'ClinicNum':clinic,'dateStart':start,'dateEnd':end}
    for status in statuses:
        for op in ops:
            params = base.copy(); params['AptStatus']=status
            if op: params['Op']=op
            if not first_run and since:
                params['DateTStamp']=since.isoformat()
            resp = requests.get(f"{config.api_base_url}/appointments",headers=make_headers(),params=params,timeout=config.request_timeout)
            resp.raise_for_status()
            all_appts.extend(resp.json() if isinstance(resp.json(),list) else [resp.json()])
    logger.info(f"Fetched {len(all_appts)} appointments for clinic {clinic}")
    return all_appts

# === PATIENT FETCH ===
@retry
def fetch_patient(pat_num: int) -> Dict[str,Any]:
    if not pat_num: return {}
    try:
        r = requests.get(f"{config.api_base_url}/patients/{pat_num}",headers=make_headers(),timeout=config.request_timeout); r.raise_for_status(); return r.json()
    except:
        r = requests.get(f"{config.api_base_url}/patients",headers=make_headers(),params={'PatNum':pat_num},timeout=config.request_timeout); r.raise_for_status(); d=r.json(); return d[0] if isinstance(d,list) and d else {}

# === SEND TO KERAGON ===
@retry
def send_to_keragon(appt: Dict[str,Any], patient: Dict[str,Any]) -> bool:
    # compute duration + end
    duration, end = get_appointment_duration(appt)
    start = parse_iso(appt.get('AptDateTime'))
    # build payload
    payload = {
        'appointmentId':str(appt.get('AptNum')),
        'appointmentTime':start.replace(tzinfo=ZoneInfo('UTC')).isoformat() if start else '',
        'appointmentEndTime':end.replace(tzinfo=ZoneInfo('UTC')).isoformat() if end else '',
        'appointmentDurationMinutes':duration,
        'status':appt.get('AptStatus'),
        'notes':appt.get('Note','') + ' [fromOpenDental]',
        # patient
        'patientId':str(appt.get('PatNum')),
        'firstName':patient.get('FName',''),
        'lastName':patient.get('LName',''),
        'email':patient.get('Email',''),
        'phone':patient.get('WirelessPhone') or patient.get('HmPhone') or '',
        'gender':patient.get('Gender',''),
        'birthdate':patient.get('Birthdate',''),
        # address
        'address':patient.get('Address',''),
        'address2':patient.get('Address2',''),
        'city':patient.get('City',''),
        'state':patient.get('State',''),
        'zipCode':patient.get('Zip',''),
    }
    clean = {k:v for k,v in payload.items() if v not in (None,'',0)}
    resp = requests.post(config.keragon_webhook_url,json=clean,timeout=config.request_timeout)
    resp.raise_for_status()
    # enhanced log
    name = f"{patient.get('FName','')} {patient.get('LName','')}".strip()
    st = start.replace(tzinfo=ZoneInfo('UTC')).isoformat() if start else ''
    ed = end.replace(tzinfo=ZoneInfo('UTC')).isoformat() if end else ''
    logger.info(f"✓ Sent appointment {appt.get('AptNum')} for {name} from {st} to {ed} to Keragon")
    return True

# === MAIN SYNC ===
def run(force_first:bool=False):
    if not all([config.developer_key,config.customer_key,config.keragon_webhook_url]):
        logger.critical("Missing credentials or webhook URL")
        sys.exit(1)
    last_state, first_run = load_state()
    overall_first = force_first or first_run
    new_state:Dict[int,datetime.datetime] = {}
    for clinic in config.clinic_nums:
        logger.info(f"=== Clinic {clinic} ===")
        since = None if overall_first else last_state.get(clinic)
        appts = fetch_appointments(clinic,since,overall_first)
        # parallel patient fetch
        patients={}
        with ThreadPoolExecutor(max_workers=config.max_workers) as exe:
            futs={exe.submit(fetch_patient,ap['PatNum']):ap['PatNum'] for ap in appts}
            for fut in as_completed(futs):
                pat=futs[fut]
                try: patients[pat]=fut.result()
                except: patients[pat]={}
        # send
        for appt in appts:
            try: send_to_keragon(appt,patients.get(appt.get('PatNum'),{}))
            except Exception as e: logger.error(f"Error sending apt {appt.get('AptNum')}: {e}")
        new_state[clinic]=datetime.datetime.utcnow()
    save_state(new_state)
    logger.info("Sync complete.")

# === CLI ===
if __name__=='__main__':
    import argparse
    p=argparse.ArgumentParser(desc='OpenDental→Keragon Sync')
    p.add_argument('--once',action='store_true',help='Force fetch all (first-run)')
    p.add_argument('--reset',action='store_true',help='Clear sync state')
    p.add_argument('--verbose',action='store_true',help='Debug logging')
    args=p.parse_args()
    if args.verbose: logger.setLevel(logging.DEBUG)
    if args.reset:
        try: os.remove(config.state_file); logger.info("State cleared.")
        except: logger.info("No state to clear.")
        sys.exit(0)
    run(force_first=args.once)
