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
    first_run_lookahead_days: int = int(os.environ.get('FIRST_RUN_LOOKAHEAD_DAYS', '30'))
    max_workers: int = int(os.environ.get('MAX_WORKERS', '5'))
    request_timeout: int = int(os.environ.get('REQUEST_TIMEOUT', '30'))
    retry_attempts: int = int(os.environ.get('RETRY_ATTEMPTS', '3'))

    def __post_init__(self):
        clinic_nums_str = os.environ.get('CLINIC_NUMS', '')
        if clinic_nums_str.strip():
            self.clinic_nums = [int(x) for x in clinic_nums_str.split(',') if x.strip().isdigit()]
        else:
            # default to configured filters if no env var
            self.clinic_nums = list(self.clinic_operatory_filters.keys())
        # Configure operatory filters per clinic
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
        def parse_dt(val: Optional[str]) -> datetime.datetime:
            if not val:
                return datetime.datetime.utcnow()
            for fmt in (
                "%Y-%m-%dT%H:%M:%S.%f", "%Y-%m-%dT%H:%M:%S",
                "%Y-%m-%d %H:%M:%S.%f", "%Y-%m-%d %H:%M:%S",
                "%Y-%m-%d", "%m/%d/%Y %H:%M:%S", "%m/%d/%Y"
            ):
                try:
                    return datetime.datetime.strptime(val, fmt)
                except Exception:
                    continue
            return datetime.datetime.utcnow()

        patnum = int(data.get('PatNum') or 0)
        aptnum = int(data.get('AptNum') or 0)
        clinic = int(data.get('ClinicNum') or 0)
        oper = int(data.get('Op') or 0)
        apt_dt = parse_dt(data.get('AptDateTime', ''))
        date_t = parse_dt(data.get('DateTStamp', ''))
        pattern = data.get('Pattern', '')
        x_count = pattern.count('X')
        # default to 60 if no X's
        duration = x_count * 10 if x_count else 60
        # clamp 15–480 minutes
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
            apt_status=data.get('AptStatus', ''),
            date_t_stamp=date_t,
            provider_abbr=data.get('provAbbr', ''),
            note=data.get('Note', '') or ''
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
        with open(config.state_file) as f:
            data = json.load(f)
        if not isinstance(data, dict) or not data:
            logger.info("Empty state file, FIRST RUN")
            return {}, True
        state = {int(k): datetime.datetime.fromisoformat(v) for k, v in data.items()}
        logger.info(f"Loaded sync timestamps: {state}")
        return state, False
    except Exception as e:
        logger.error(f"Error loading state: {e}, FIRST RUN")
        return {}, True


def save_last_sync_state(state: Dict[int, datetime.datetime]):
    tmp = config.state_file + '.tmp'
    with open(tmp, 'w') as f:
        json.dump({str(k): v.isoformat() for k, v in state.items()}, f, indent=2)
    os.replace(tmp, config.state_file)
    logger.debug("State saved")

# === API HELPERS ===
def make_headers() -> Dict[str, str]:
    return {
        'Authorization': f"ODFHIR {config.developer_key}/{config.customer_key}",
        'Content-Type': 'application/json'
    }

@retry_request
def fetch_operatories_for_clinic(clinic: int) -> List[Dict[str, Any]]:
    resp = requests.get(
        f"{config.api_base_url}/operatories", headers=make_headers(),
        params={'ClinicNum': clinic}, timeout=config.request_timeout
    )
    resp.raise_for_status()
    return resp.json()

@retry_request
def fetch_appointments_batch(
    clinic: int, start: str, end: str, status: str, op: Optional[int] = None
) -> List[Dict[str, Any]]:
    params = {
        'dateStart': start,
        'dateEnd': end,
        'ClinicNum': clinic,
        'AptStatus': status,
        'Limit': 1000
    }
    if op:
        params['Op'] = op
    resp = requests.get(
        f"{config.api_base_url}/appointments", headers=make_headers(),
        params=params, timeout=config.request_timeout
    )
    resp.raise_for_status()
    return resp.json() or []

@retry_request
def fetch_patient_details(pat: int) -> Dict[str, Any]:
    if not pat:
        return {}
    try:
        r = requests.get(
            f"{config.api_base_url}/patients/{pat}", headers=make_headers(),
            timeout=config.request_timeout
        )
        r.raise_for_status()
        return r.json()
    except Exception:
        r = requests.get(
            f"{config.api_base_url}/patients", headers=make_headers(),
            params={'PatNum': pat}, timeout=config.request_timeout
        )
        r.raise_for_status()
        data = r.json()
        if isinstance(data, list) and data:
            return data[0]
        if isinstance(data, dict):
            return data
        return {}

# === KERAGON SEND ===
@retry_request
def send_to_keragon(
    a: AppointmentData, pd: Dict[str, Any]
) -> bool:
    payload = {
        'appointmentId': str(a.apt_num),
        'appointmentTime': a.apt_date_time.isoformat(),
        'appointmentEndTime': a.apt_end_time.isoformat(),
        'appointmentDurationMinutes': int((a.apt_end_time - a.apt_date_time).total_seconds() / 60),
        'firstName': pd.get('FName', ''),
        'lastName': pd.get('LName', ''),
        'email': pd.get('Email', ''),
        'phone': pd.get('WirelessPhone', ''),
        'gender': pd.get('Gender', ''),
        'address': pd.get('Address', ''),
        'address2': pd.get('Address2', ''),
        'city': pd.get('City', ''),
        'state': pd.get('State', ''),
        'zipCode': pd.get('Zip', ''),
        'patientId': str(a.pat_num),
        'status': a.apt_status,
        'locationId': str(a.clinic_num),
        'calendarId': str(a.operatory_num),
        'notes': a.note,
        'syncTimestamp': datetime.datetime.utcnow().isoformat(),
        'lastModified': a.date_t_stamp.isoformat()
    }
    clean = {k: v for k, v in payload.items() if v is not None and v != ''}
    r = requests.post(
        config.keragon_webhook_url, json=clean,
        headers={'Content-Type': 'application/json'},
        timeout=config.request_timeout
    )
    r.raise_for_status()
    logger.info(f"✓ Sent appointment {a.apt_num} to Keragon")
    return True

# === SYNC LOGIC ===
def run_sync() -> bool:
    if not all([config.developer_key, config.customer_key, config.keragon_webhook_url, config.clinic_nums]):
        logger.critical("Missing configuration vars. Check your env settings.")
        return False
    last_state, first_run = load_last_sync_state()
    now = datetime.datetime.utcnow()
    new_state: Dict[int, datetime.datetime] = {}
    for clinic in config.clinic_nums:
        since = now - datetime.timedelta(hours=24) if first_run else last_state.get(clinic, now - datetime.timedelta(hours=24))
        new_state[clinic] = now
        start = now.strftime("%Y-%m-%d")
        end = (now + datetime.timedelta(days=config.first_run_lookahead_days)).strftime("%Y-%m-%d")
        appointments: List[AppointmentData] = []
        for status in ['Scheduled', 'Complete', 'Broken']:
            ops = config.clinic_operatory_filters.get(clinic, [None])
            for op in ops:
                try:
                    batch = fetch_appointments_batch(clinic, start, end, status, op)
                    appointments.extend(batch)
                except Exception as e:
                    logger.error(f"Error fetching {status} for clinic {clinic}, op {op}: {e}")
        # parse
        objs = [AppointmentData.from_api_response(a) for a in appointments]
        to_send = objs if first_run else [o for o in objs if o.date_t_stamp >= since]
        # fetch patient data
        cache: Dict[int, Dict[str, Any]] = {}
        with ThreadPoolExecutor(max_workers=config.max_workers) as exec:
            futures: Dict[int, Any] = {}
            for a in to_send:
                if a.pat_num not in cache and a.pat_num not in futures:
                    futures[a.pat_num] = exec.submit(fetch_patient_details, a.pat_num)
            for pat_num, fut in futures.items():
                try:
                    cache[pat_num] = fut.result()
                except Exception as e:
                    logger.error(f"Failed to fetch patient {pat_num}: {e}")
                    cache[pat_num] = {}
        # send
        for a in to_send:
            try:
                send_to_keragon(a, cache.get(a.pat_num, {}))
            except Exception as e:
                logger.error(f"Failed to send appointment {a.apt_num}: {e}")
    save_last_sync_state(new_state)
    return True

# === CLI ===
def main() -> int:
    import argparse
    parser = argparse.ArgumentParser(description='Sync OpenDental → Keragon appointments')
    parser.add_argument('--once', action='store_true', help='Run sync once and exit')
    parser.add_argument('--test', action='store_true', help='Test API connectivity')
    parser.add_argument('--dump-state', action='store_true', help='Print last sync state and exit')
    parser.add_argument('--reset', action='store_true', help='Reset sync state file and exit')
    parser.add_argument('--verbose', action='store_true', help='Enable debug logging')
    args = parser.parse_args()

    if args.verbose:
        logger.setLevel(logging.DEBUG)
    if args.reset:
        if os.path.exists(config.state_file):
            os.remove(config.state_file)
            logger.info("State file reset.")
        else:
            logger.info("No state file to reset.")
        return 0
    if args.dump_state:
        if os.path.exists(config.state_file):
            print(open(config.state_file).read())
        else:
            print("No state file exists.")
        return 0
    if args.test:
        logger.info("Testing connectivity...")
        from requests.exceptions import RequestException
        # Test OpenDental
        try:
            r = requests.get(f"{config.api_base_url}/operatories", headers=make_headers(), timeout=10)
            r.raise_for_status()
            logger.info("✓ OpenDental API OK")
        except RequestException as e:
            logger.error(f"✗ OpenDental API failed: {e}")
            return 1
        # Test Keragon
        try:
            r = requests.post(config.keragon_webhook_url, json={'test':True}, timeout=10)
            r.raise_for_status()
            logger.info("✓ Keragon webhook OK")
        except RequestException as e:
            logger.error(f"✗ Keragon webhook failed: {e}")
            return 1
        return 0
    if args.once:
        success = run_sync()
        return 0 if success else 1
    # default: run once
    return 0 if run_sync() else 1

if __name__ == '__main__':
    sys.exit(main())
