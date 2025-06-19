#!/usr/bin/env python3
"""
OpenDental → Keragon Appointment Sync
- Primary source: FHIR `/Appointment` for start/end
- Fallback duration: ChartModules `/ProgNotes` if FHIR has no end or duration
- Logs detailed info (clinic, operatory, patient, appt num, start/end)
- Includes full patient payload (gender, address, zip code, balance)
- Optimized API calls, retries, and threading
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
    clinic_nums: List[int] = field(default_factory=lambda: (
        [int(x) for x in os.environ.get('CLINIC_NUMS','').split(',') if x.strip().isdigit()]
    ))
    operatory_filters: Dict[int, List[int]] = field(default_factory=lambda: {
        9034: [11579, 11580, 11588],
        9035: [11574, 11576, 11577],
    })

# Initialize
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
    if not os.path.exists(config.state_file): return {}, True
    try:
        with open(config.state_file) as f:
            data = json.load(f)
        state = {int(k): datetime.datetime.fromisoformat(v) for k,v in data.items()}
        return state, False
    except:
        return {}, True

def save_state(state: Dict[int, datetime.datetime]):
    tmp = config.state_file + '.tmp'
    with open(tmp,'w') as f:
        json.dump({str(k): v.isoformat() for k,v in state.items()}, f, indent=2)
    os.replace(tmp, config.state_file)

# === UTILITIES ===
def make_headers() -> Dict[str, str]:
    return {
        'Authorization': f'ODFHIR {config.developer_key}/{config.customer_key}',
        'Content-Type': 'application/json'
    }

def parse_iso(dt_str: Optional[str]) -> Optional[datetime.datetime]:
    if not dt_str: return None
    for fmt in ("%Y-%m-%dT%H:%M:%S.%f","%Y-%m-%dT%H:%M:%S","%Y-%m-%d %H:%M:%S","%Y-%m-%d"):
        try: return datetime.datetime.strptime(dt_str, fmt)
        except ValueError: continue
    return None

# === FHIR APPOINTMENT FETCH ===
@retry
def fetch_fhir_appointment(appt_id: str) -> Dict[str, Any]:
    url = f"{config.fhir_base_url}/Appointment/{appt_id}"
    headers = make_headers()
    headers['Accept'] = 'application/fhir+json'
    resp = requests.get(url, headers=headers, timeout=config.request_timeout)
    resp.raise_for_status()
    return resp.json()

# === CHARTMODULES PROGNOTES FETCH ===
@retry
def fetch_prognotes(pat_num: int) -> List[Dict[str, Any]]:
    url = f"{config.api_base_url}/chartmodules/{pat_num}/ProgNotes"
    resp = requests.get(url, headers=make_headers(), timeout=config.request_timeout)
    resp.raise_for_status()
    return resp.json()

# === DURATION & TIMES ===
def get_times(appt: Dict[str, Any]) -> Tuple[Optional[datetime.datetime], Optional[datetime.datetime], int]:
    # Try FHIR for start/end
    try:
        f = fetch_fhir_appointment(str(appt.get('AptNum')))
        start = parse_iso(f.get('start'))
        end   = parse_iso(f.get('end'))
        if start and end:
            mins = int((end - start).total_seconds()/60)
            return start, end, mins
    except Exception:
        pass
    # Fallback to ProgNotes duration
    start = parse_iso(appt.get('AptDateTime'))
    duration, end = 60, None
    if start:
        try:
            notes = fetch_prognotes(appt.get('PatNum'))
            for note in notes:
                if note.get('AptNum') == appt.get('AptNum'):
                    h,m = note.get('Length','0:60').split(':')
                    duration = int(h)*60 + int(m)
                    break
        except Exception:
            pass
        end = start + datetime.timedelta(minutes=duration)
    return start, end, duration

# === FETCH APPOINTMENTS ===
@retry
def fetch_appointments(clinic: int, since: Optional[datetime.datetime], first_run: bool) -> List[Dict[str, Any]]:
    now = datetime.datetime.utcnow()
    s = now.strftime("%Y-%m-%d")
    e = (now + datetime.timedelta(hours=config.lookahead_hours)).strftime("%Y-%m-%d")
    out = []
    statuses = ['Scheduled','Complete','Broken']
    ops = config.operatory_filters.get(clinic, [None])
    base = {'ClinicNum':clinic, 'dateStart':s, 'dateEnd':e}
    for status in statuses:
        for op in ops:
            prm = base.copy(); prm['AptStatus'] = status
            if op: prm['Op']=op
            if not first_run and since: prm['DateTStamp']=since.isoformat()
            r = requests.get(f"{config.api_base_url}/appointments", headers=make_headers(), params=prm, timeout=config.request_timeout)
            r.raise_for_status()
            data = r.json()
            for a in (data if isinstance(data,list) else [data]):
                a['_clinic']=clinic; a['_oper']=op
                out.append(a)
    return out

# === PATIENT FETCH ===
@retry
def fetch_patient(pat_num: int) -> Dict[str, Any]:
    if not pat_num: return {}
    try:
        r = requests.get(f"{config.api_base_url}/patients/{pat_num}", headers=make_headers(), timeout=config.request_timeout)
        r.raise_for_status(); return r.json()
    except:
        r = requests.get(f"{config.api_base_url}/patients", headers=make_headers(), params={'PatNum':pat_num}, timeout=config.request_timeout)
        r.raise_for_status(); d=r.json(); return d[0] if isinstance(d,list) else {}

# === SEND TO KERAGON ===
@retry
def send_to_keragon(appt: Dict[str, Any], patient: Dict[str, Any]) -> Dict[str, Any]:
    clinic = appt.get('_clinic'); oper = appt.get('_oper')
    start, end, duration = get_times(appt)
    payload = {
        'appointmentId':str(appt.get('AptNum')),
        'appointmentTime': start.isoformat() if start else '',
        'appointmentEndTime': end.isoformat() if end else '',
        'appointmentDurationMinutes': duration,
        'status': appt.get('AptStatus'),
        'notes': appt.get('Note','') + ' [fromOD]',
        'patientId':str(appt.get('PatNum')),
        'firstName': patient.get('FName',''), 'lastName': patient.get('LName',''),
        'gender':patient.get('Gender',''), 'address':patient.get('Address',''),
        'city':patient.get('City',''), 'state':patient.get('State',''), 'zipCode':patient.get('Zip',''),
        'balance':patient.get('Balance',0)
    }
    requests.post(config.keragon_webhook_url, json={k:v for k,v in payload.items() if v}, timeout=config.request_timeout).raise_for_status()
    return {'clinic':clinic,'oper':oper,'apt':appt.get('AptNum'),
            'first':patient.get('FName',''),'last':patient.get('LName',''),
            'start':start.isoformat() if start else '','end':end.isoformat() if end else ''}

# === MAIN SYNC & CLI ===
def run(force_first: bool=False):
    if not all([config.developer_key,config.customer_key,config.keragon_webhook_url]):
        logger.critical("Missing creds/webhook"); sys.exit(1)
    last_state, first_run = load_state()
    overall_first = force_first or first_run
    new_state={}; processed=[]
    for clinic in config.clinic_nums:
        since = None if overall_first else last_state.get(clinic)
        appts = fetch_appointments(clinic, since, overall_first)
        patients={}
        with ThreadPoolExecutor(max_workers=config.max_workers) as exe:
            futs={exe.submit(fetch_patient,a['PatNum']):a for a in appts}
            for fut in as_completed(futs): pat=futs[fut]; patients[pat['PatNum']]=fut.result() or {}
        for a in appts:
            try: info=send_to_keragon(a,patients.get(a['PatNum'],{})); processed.append(info)
            except Exception as e: logger.error(f"Error sending apt {a['AptNum']}: {e}")
        new_state[clinic]=datetime.datetime.utcnow()
    save_state(new_state)
    for r in processed:
        logger.info(f"Synced apt {r['apt']} (clinic {r['clinic']}, oper {r['oper']}) " +
                    f"for {r['first']} {r['last']} from {r['start']} to {r['end']}")

if __name__=='__main__':
    import argparse
    p=argparse.ArgumentParser(description='OpenDental→Keragon Sync')
    p.add_argument('--once',action='store_true',help='First run')
    p.add_argument('--reset',action='store_true',help='Clear state')
    p.add_argument('--verbose',action='store_true',help='Debug')
    args=p.parse_args()
    if args.verbose: logger.setLevel(logging.DEBUG)
    if args.reset: 
        try: os.remove(config.state_file) 
        except: pass
        sys.exit(0)
    run(force_first=args.once)
