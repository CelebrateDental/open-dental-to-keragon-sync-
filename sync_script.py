#!/usr/bin/env python3
"""
Optimized OpenDental to Keragon Appointment Sync
Fixes appointment timing issues and implements best practices
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
from collections import defaultdict
from dataclasses import dataclass
from concurrent.futures import ThreadPoolExecutor

# === CONFIGURATION ===
@dataclass
class Config:
    api_base_url: str = os.environ.get('OPEN_DENTAL_API_URL', 'https://api.opendental.com/api/v1')
    developer_key: str = os.environ.get('OPEN_DENTAL_DEVELOPER_KEY', '')
    customer_key: str = os.environ.get('OPEN_DENTAL_CUSTOMER_KEY', '')
    keragon_webhook_url: str = os.environ.get('KERAGON_WEBHOOK_URL', '')
    state_file: str = 'last_sync_state.json'
    log_level: str = os.environ.get('LOG_LEVEL', 'INFO')
    overlap_minutes: int = int(os.environ.get('OVERLAP_MINUTES', '5'))
    first_run_lookahead_days: int = int(os.environ.get('FIRST_RUN_LOOKAHEAD_DAYS', '30'))
    max_workers: int = int(os.environ.get('MAX_WORKERS', '5'))
    request_timeout: int = int(os.environ.get('REQUEST_TIMEOUT', '30'))
    retry_attempts: int = int(os.environ.get('RETRY_ATTEMPTS', '3'))

    def __post_init__(self):
        clinic_nums_str = os.environ.get('CLINIC_NUMS', '')
        if clinic_nums_str.strip():
            self.clinic_nums = [int(x) for x in clinic_nums_str.split(',') if x.strip().isdigit()]
        else:
            self.clinic_nums = list(self.clinic_operatory_filters.keys())

        self.clinic_operatory_filters: Dict[int, List[int]] = {
            9034: [11579, 11580, 11588],
            9035: [11574, 11576, 11577],
        }

@dataclass
class AppointmentData:
    apt_num: int
    pat_num: int
    clinic_num: int
    operatory_num: int
    apt_date_time: datetime.datetime
    apt_end_time: datetime.datetime
    pattern: str
    apt_status: str
    date_t_stamp: datetime.datetime
    provider_abbr: str = ''
    note: str = ''

    @classmethod
    def from_api_response(cls, data: Dict[str, Any]) -> 'AppointmentData':
        def parse_dt(val):
            for fmt in ("%Y-%m-%dT%H:%M:%S.%f","%Y-%m-%dT%H:%M:%S","%Y-%m-%d %H:%M:%S.%f","%Y-%m-%d %H:%M:%S","%Y-%m-%d","%m/%d/%Y %H:%M:%S","%m/%d/%Y"):
                try:
                    return datetime.datetime.strptime(val, fmt)
                except Exception:
                    continue
            return datetime.datetime.utcnow()

        patnum = int(data.get('PatNum', 0) or 0)
        aptnum = int(data.get('AptNum', 0) or 0)
        clinic = int(data.get('ClinicNum', 0) or 0)
        oper = int(data.get('Op', 0) or 0)
        apt_dt = parse_dt(data.get('AptDateTime', ''))
        date_t = parse_dt(data.get('DateTStamp', ''))
        pattern = data.get('Pattern', '')
        x_count = pattern.count('X')
        duration = x_count * 10 if x_count else 60
        duration = max(15, min(duration, 480))
        end_dt = apt_dt + datetime.timedelta(minutes=duration)
        return cls(
            apt_num=aptnum,
            pat_num=patnum,
            clinic_num=clinic,
            operatory_num=oper,
            apt_date_time=apt_dt,
            apt_end_time=end_dt,
            pattern=pattern,
            apt_status=data.get('AptStatus',''),
            date_t_stamp=date_t,
            provider_abbr=data.get('provAbbr',''),
            note=data.get('Note','') or ''
        )

# === LOGGER ===
config = Config()
logger = logging.getLogger('opendental_sync')
handler = logging.StreamHandler(sys.stdout)
handler.setFormatter(logging.Formatter('%(asctime)s - %(levelname)s - %(message)s'))
logger.addHandler(handler)
logger.setLevel(getattr(logging, config.log_level.upper(), logging.INFO))
logging.getLogger('urllib3').setLevel(logging.WARNING)

# === RETRY DECORATOR ===
def retry_request(fn):
    def wrapper(*args, **kwargs):
        last_exc = None
        for i in range(config.retry_attempts):
            try:
                return fn(*args, **kwargs)
            except (HTTPError, RequestException) as e:
                last_exc = e
                backoff = 1.5 ** i
                logger.warning(f"Request failed ({i+1}/{config.retry_attempts}): {e}, retrying in {backoff:.1f}s")
                time.sleep(backoff)
        logger.error(f"All retries failed: {last_exc}")
        raise last_exc
    return wrapper

# === STATE MANAGEMENT ===
def load_last_sync_state() -> Tuple[Dict[int, datetime.datetime], bool]:
    if not os.path.exists(config.state_file):
        logger.info("No state file, FIRST RUN")
        return {}, True
    try:
        data = json.load(open(config.state_file))
        if not isinstance(data, dict) or not data:
            logger.info("Empty state file, FIRST RUN")
            return {}, True
        state = {int(k): datetime.datetime.fromisoformat(v) for k,v in data.items()}
        logger.info(f"Loaded sync timestamps: {state}")
        return state, False
    except Exception as e:
        logger.error(f"Error loading state: {e}, FIRST RUN")
        return {}, True

def save_last_sync_state(state: Dict[int, datetime.datetime]):
    tmp = config.state_file + '.tmp'
    with open(tmp,'w') as f:
        json.dump({str(k):v.isoformat() for k,v in state.items()}, f, indent=2)
    os.replace(tmp, config.state_file)
    logger.debug("State saved")

# === API HELPERS ===
def make_headers():
    return {'Authorization':f"ODFHIR {config.developer_key}/{config.customer_key}",'Content-Type':'application/json'}

@retry_request
def fetch_operatories_for_clinic(clinic: int):
    resp = requests.get(f"{config.api_base_url}/operatories", headers=make_headers(), params={'ClinicNum':clinic}, timeout=config.request_timeout)
    resp.raise_for_status()
    return resp.json()

@retry_request
def fetch_appointments_batch(clinic:int, start:str, end:str, status:str, op:Optional[int]=None):
    params={'dateStart':start,'dateEnd':end,'ClinicNum':clinic,'AptStatus':status,'Limit':1000}
    if op: params['Op']=op
    resp = requests.get(f"{config.api_base_url}/appointments", headers=make_headers(), params=params, timeout=config.request_timeout)
    resp.raise_for_status()
    return resp.json() or []

@retry_request
def fetch_patient_details(pat:int)->Dict[str,Any]:
    if not pat: return {}
    try:
        r = requests.get(f"{config.api_base_url}/patients/{pat}", headers=make_headers(), timeout=config.request_timeout)
        r.raise_for_status()
        return r.json()
    except:
        r = requests.get(f"{config.api_base_url}/patients", headers=make_headers(), params={'PatNum':pat}, timeout=config.request_timeout)
        r.raise_for_status()
        d=r.json()
        return d[0] if isinstance(d,list) and d else (d if isinstance(d,dict) else {})

# === KERAGON SEND ===
@retry_request
def send_to_keragon(a: AppointmentData, pd: Dict[str, Any]) -> bool:
    payload = {
        'appointmentId': str(a.apt_num),
        'appointmentTime': a.apt_date_time.isoformat(),
        'appointmentEndTime': a.apt_end_time.isoformat(),
        'appointmentDurationMinutes': int((a.apt_end_time - a.apt_date_time).total_seconds() / 60),
        'firstName': pd.get('FName', ''),
        'lastName': pd.get('LName', ''),
        'email': pd.get('Email', ''),
        'phone': pd.get('WirelessPhone', ''),
        'patientId': str(a.pat_num),
        'status': a.apt_status,
        'locationId': str(a.clinic_num),
        'calendarId': str(a.operatory_num),
        'gender': pd.get('Gender', ''),
        'address': pd.get('Address', ''),
        'address2': pd.get('Address2', ''),
        'city': pd.get('City', ''),
        'state': pd.get('State', ''),
        'zipCode': pd.get('Zip', ''),
        'notes': a.note,
        'syncTimestamp': datetime.datetime.utcnow().isoformat(),
        'lastModified': a.date_t_stamp.isoformat()
    }
    clean = {k: v for k, v in payload.items() if v}
    r = requests.post(config.keragon_webhook_url, json=clean, headers={'Content-Type':'application/json'}, timeout=config.request_timeout)
    r.raise_for_status()
    logger.info(f"Sent apt {a.apt_num}")
    return True

# === CORE SYNC ===
def run_sync() -> bool:
    if not all([config.developer_key, config.customer_key, config.keragon_webhook_url, config.clinic_nums]):
        logger.critical("Missing configuration vars")
        return False

    last_state, first_run = load_last_sync_state()
    now = datetime.datetime.utcnow()
    new_state = {}
    for clinic in config.clinic_nums:
        since = (now - datetime.timedelta(hours=24)) if first_run else last_state.get(clinic, now - datetime.timedelta(hours=24))
        new_state[clinic] = now
        start = now.strftime("%Y-%m-%d")
        end = (now + datetime.timedelta(days=config.first_run_lookahead_days)).strftime("%Y-%m-%d")
        appts = []
        for status in ['Scheduled', 'Complete', 'Broken
