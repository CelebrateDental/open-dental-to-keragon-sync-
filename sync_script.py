#!/usr/bin/env python3
"""
OpenDental → Keragon Sync (Pattern only, per clinic, per operatory)
- Gets ALL operatories for each clinic, filters by allowed IDs
- Processes each allowed operatory one-by-one
- Uses Pattern ONLY for duration (each X or / = 10 mins)
"""

import os, sys, json, logging, datetime, requests, time
from typing import List, Dict, Any, Optional, Tuple
from requests.exceptions import HTTPError, RequestException
from dataclasses import dataclass, field
from concurrent.futures import ThreadPoolExecutor, as_completed

# === CONFIG ===
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
    clinic_nums: List[int] = field(default_factory=lambda: [
        int(x) for x in os.environ.get('CLINIC_NUMS', '').split(',') if x.strip().isdigit()
    ])
    operatory_filters: Dict[int, List[int]] = field(default_factory=lambda: {
        9034: [11579, 11580, 11588],
        9035: [11574, 11576, 11577],
    })

config = Config()
logger = logging.getLogger('opendental_perclinic')
handler = logging.StreamHandler(sys.stdout)
handler.setFormatter(logging.Formatter('%(asctime)s - %(levelname)s - %(message)s'))
logger.addHandler(handler)
logger.setLevel(getattr(logging, config.log_level.upper(), logging.INFO))

# === RETRY ===
def retry(fn):
    def wrapper(*a, **kw):
        last = None
        for i in range(config.retry_attempts):
            try: return fn(*a, **kw)
            except (RequestException, HTTPError) as e:
                last = e; time.sleep(2**i)
        logger.error(f"All retries failed: {last}")
        raise last
    return wrapper

# === STATE ===
def load_state() -> Tuple[Dict[int, datetime.datetime], bool]:
    if not os.path.exists(config.state_file): return {}, True
    try:
        with open(config.state_file) as f:
            d = json.load(f)
        return {int(k): datetime.datetime.fromisoformat(v) for k, v in d.items()}, False
    except:
        return {}, True

def save_state(state: Dict[int, datetime.datetime]):
    tmp = config.state_file + '.tmp'
    with open(tmp, 'w') as f:
        json.dump({str(k): v.isoformat() for k, v in state.items()}, f, indent=2)
    os.replace(tmp, config.state_file)

def make_headers() -> Dict[str, str]:
    return {'Authorization': f'ODFHIR {config.developer_key}/{config.customer_key}'}

def parse_iso(s: Optional[str]) -> Optional[datetime.datetime]:
    if not s: return None
    for f in ("%Y-%m-%dT%H:%M:%S.%f", "%Y-%m-%dT%H:%M:%S", "%Y-%m-%d %H:%M:%S", "%Y-%m-%d"):
        try: return datetime.datetime.strptime(s, f)
        except: pass
    return None

# === GET ALL OPERATORIES ===
@retry
def get_all_operatories(clinic: int) -> List[int]:
    resp = requests.get(f"{config.api_base_url}/operatories",
                        headers=make_headers(),
                        params={'ClinicNum': clinic},
                        timeout=config.request_timeout)
    resp.raise_for_status()
    ops = resp.json()
    return [op['OperatoryNum'] for op in ops] if isinstance(ops, list) else []

# === PATTERN ONLY ===
def get_times_by_pattern(appt: Dict[str, Any]) -> Tuple[Optional[datetime.datetime], Optional[datetime.datetime], int]:
    start = parse_iso(appt.get('AptDateTime'))
    pattern = appt.get('Pattern') or ''
    count = pattern.count('X') + pattern.count('/')
    duration = max(count * 10, 10)
    end = start + datetime.timedelta(minutes=duration) if start else None
    return start, end, duration

# === FETCH APPTS ===
@retry
def fetch_appointments(clinic: int, oper: int, since: Optional[datetime.datetime], first: bool) -> List[Dict[str, Any]]:
    now = datetime.datetime.utcnow()
    start, end = now.strftime("%Y-%m-%d"), (now + datetime.timedelta(hours=config.lookahead_hours)).strftime("%Y-%m-%d")
    base = {'ClinicNum': clinic, 'Op': oper, 'dateStart': start, 'dateEnd': end}
    statuses = ['Scheduled', 'Complete', 'Broken']
    all = []
    for status in statuses:
        params = base.copy(); params['AptStatus'] = status
        if not first and since: params['DateTStamp'] = since.isoformat()
        resp = requests.get(f"{config.api_base_url}/appointments", headers=make_headers(),
                            params=params, timeout=config.request_timeout)
        resp.raise_for_status()
        data = resp.json()
        items = data if isinstance(data, list) else [data]
        for ap in items: ap['_clinic'] = clinic; ap['_operatory'] = oper; all.append(ap)
    return all

# === PATIENT ===
@retry
def fetch_patient(pat_num: int) -> Dict[str, Any]:
    if not pat_num: return {}
    resp = requests.get(f"{config.api_base_url}/patients/{pat_num}",
                        headers=make_headers(),
                        timeout=config.request_timeout)
    resp.raise_for_status()
    return resp.json()

# === SEND ===
@retry
def send_to_keragon(appt: Dict[str, Any], pat: Dict[str, Any]) -> Dict[str, Any]:
    s, e, d = get_times_by_pattern(appt)
    payload = {
        'appointmentId': str(appt.get('AptNum')),
        'appointmentTime': s.isoformat() if s else '',
        'appointmentEndTime': e.isoformat() if e else '',
        'appointmentDurationMinutes': d,
        'status': appt.get('AptStatus'),
        'notes': (appt.get('Note') or '') + ' [fromOD]',
        'patientId': str(appt.get('PatNum')),
        'firstName': pat.get('FName', ''),
        'lastName': pat.get('LName', ''),
        'gender': pat.get('Gender', ''),
        'address': pat.get('Address', ''),
        'address2': pat.get('Address2', ''),
        'city': pat.get('City', ''),
        'state': pat.get('State', ''),
        'zipCode': pat.get('Zip', ''),
        'balance': pat.get('Balance', 0),
    }
    clean = {k: v for k, v in payload.items() if v not in (None, '', 0)}
    requests.post(config.keragon_webhook_url, json=clean, timeout=config.request_timeout).raise_for_status()
    return {'clinic': appt.get('_clinic'), 'operatory': appt.get('_operatory'),
            'apt': appt.get('AptNum'), 'first': pat.get('FName', ''),
            'last': pat.get('LName', ''), 'start': s.isoformat() if s else '',
            'end': e.isoformat() if e else ''}

# === MAIN ===
def run(force_first=False):
    if not all([config.developer_key, config.customer_key, config.keragon_webhook_url]):
        logger.critical("Missing credentials or webhook URL")
        sys.exit(1)

    last_state, first_run = load_state()
    new_state, processed = {}, []
    for clinic in config.clinic_nums:
        allowed = set(config.operatory_filters.get(clinic, []))
        all_ops = get_all_operatories(clinic)
        ops = [op for op in all_ops if op in allowed]
        logger.info(f"Clinic {clinic} operatories: {ops}")
        since = None if (force_first or first_run) else last_state.get(clinic)

        for op in ops:
            appts = fetch_appointments(clinic, op, since, force_first or first_run)
            logger.info(f"Processing operatory {op} with {len(appts)} appointments")
            patients = {}
            with ThreadPoolExecutor(max_workers=config.max_workers) as executor:
                futures = {executor.submit(fetch_patient, ap['PatNum']): ap['PatNum'] for ap in appts}
                for fut in as_completed(futures):
                    patients[futures[fut]] = fut.result() or {}

            for appt in appts:
                try: processed.append(send_to_keragon(appt, patients.get(appt['PatNum'], {})))
                except Exception as e: logger.error(f"Error: {e}")

        logger.info(f"Finished clinic {clinic}")
        new_state[clinic] = datetime.datetime.utcnow()

    save_state(new_state)
    for rec in processed:
        logger.info(f"Synced apt {rec['apt']} (clinic {rec['clinic']}, op {rec['operatory']}) "
                    f"for {rec['first']} {rec['last']} from {rec['start']} to {rec['end']}")

if __name__ == '__main__':
    import argparse
    p = argparse.ArgumentParser()
    p.add_argument('--once', action='store_true')
    p.add_argument('--reset', action='store_true')
    p.add_argument('--verbose', action='store_true')
    args = p.parse_args()
    if args.verbose: logger.setLevel(logging.DEBUG)
    if args.reset:
        try: os.remove(config.state_file)
        except: pass
        sys.exit(0)
    run(force_first=args.once)
