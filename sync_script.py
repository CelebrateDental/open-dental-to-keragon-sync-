#!/usr/bin/env python3
import os
import sys
import json
import logging
import datetime
import requests
import tempfile
import shutil
import time
import threading
from datetime import timezone, timedelta
from zoneinfo import ZoneInfo
from typing import List, Dict, Any, Optional, Set, Tuple
from urllib3.util.retry import Retry
from requests.adapters import HTTPAdapter
from dataclasses import dataclass, asdict
from collections import defaultdict
import hashlib

from mega import Mega

# === CONFIGURATION ===
API_BASE_URL = os.environ.get('OPEN_DENTAL_API_URL', 'https://api.opendental.com/api/v1')
DEVELOPER_KEY = os.environ.get('OPEN_DENTAL_DEVELOPER_KEY')
CUSTOMER_KEY = os.environ.get('OPEN_DENTAL_CUSTOMER_KEY')
KERAGON_WEBHOOK_URL = os.environ.get('KERAGON_WEBHOOK_URL')

STATE_FILE = 'last_sync_state.json'
SENT_APPTS_FILE = 'sent_appointments.json'
APPT_CACHE_FILE = 'appointment_cache.json'
APPT_TYPES_CACHE_FILE = 'appointment_types_cache.json'
PATIENT_CACHE_FILE = 'patient_cache.json'
PROVIDER_CACHE_FILE = 'provider_cache.json'
EMPLOYEE_CACHE_FILE = 'employee_cache.json'
OPERATORY_CACHE_FILE = 'operatory_cache.json'
LOG_LEVEL = os.environ.get('LOG_LEVEL', 'INFO')

# === DATABASE PERFORMANCE OPTIMIZATIONS ===
BATCH_SIZE = int(os.environ.get('BATCH_SIZE', '5'))
CLINIC_DELAY_SECONDS = float(os.environ.get('CLINIC_DELAY_SECONDS', '5.0'))
MAX_CONCURRENT_CLINICS = int(os.environ.get('MAX_CONCURRENT_CLINICS', '1'))
REQUEST_TIMEOUT = int(os.environ.get('REQUEST_TIMEOUT', '120'))
RETRY_ATTEMPTS = int(os.environ.get('RETRY_ATTEMPTS', '5'))
BACKOFF_FACTOR = float(os.environ.get('BACKOFF_FACTOR', '3.0'))

# === SMART SYNC OPTIMIZATION ===
INCREMENTAL_SYNC_MINUTES = int(os.environ.get('INCREMENTAL_SYNC_MINUTES', '15'))
DEEP_SYNC_HOURS = int(os.environ.get('DEEP_SYNC_HOURS', '720'))
SAFETY_OVERLAP_HOURS = int(os.environ.get('SAFETY_OVERLAP_HOURS', '2'))

# === SCHEDULING CONFIGURATION ===
CLINIC_TIMEZONE = ZoneInfo('America/Chicago')  # GMT-5
CLINIC_OPEN_HOUR = 8
CLINIC_CLOSE_HOUR = 20
DEEP_SYNC_HOUR = 2
INCREMENTAL_INTERVAL_MINUTES = 60

# === CACHING AND OPTIMIZATION ===
ENABLE_CACHING = os.environ.get('ENABLE_CACHING', 'true').lower() == 'true'
CACHE_EXPIRY_MINUTES = int(os.environ.get('CACHE_EXPIRY_MINUTES', '5'))
USE_SPECIFIC_FIELDS = os.environ.get('USE_SPECIFIC_FIELDS', 'true').lower() == 'true'
ENABLE_PAGINATION = os.environ.get('ENABLE_PAGINATION', 'true').lower() == 'true'
PAGE_SIZE = 100
MAX_RECORDS_PER_REQUEST = int(os.environ.get('MAX_RECORDS_PER_REQUEST', '100'))

CLINIC_NUMS = [int(x) for x in os.environ.get('CLINIC_NUMS', '').split(',') if x.strip().isdigit()]

CLINIC_OPERATORY_FILTERS: Dict[int, List[int]] = {
    9034: [11579, 11580, 11588],
    9035: [11574, 11576, 11577],
}

CLINIC_BROKEN_APPOINTMENT_TYPE_FILTERS: Dict[int, List[str]] = {
    9034: ["COMP EX", "COMP EX CHILD"],
    9035: ["CASH CONSULT", "INSURANCE CONSULT", "INS CONSULT"]
}

VALID_STATUSES = {'Scheduled', 'Complete', 'Broken'}

REQUIRED_APPOINTMENT_FIELDS = [
    'AptNum', 'AptDateTime', 'AptStatus', 'PatNum', 'Op', 'OperatoryNum',
    'Pattern', 'AppointmentTypeNum', 'Note', 'DateTStamp', 'FName', 'LName',
    'ProvNum', 'ProvHyg', 'Asst'
]

_appointment_types_cache: Dict[str, Dict[int, str]] = {}
_patient_cache: Dict[int, Dict[str, Any]] = {}
_provider_cache: Dict[int, Dict[str, Any]] = {}
_employee_cache: Dict[int, Dict[str, Any]] = {}
_operatory_cache: Dict[int, List[Dict[str, Any]]] = {}
_session = None
_session_lock = threading.Lock()
_request_cache: Dict[str, Tuple[datetime.datetime, Any]] = {}
_cache_lock = threading.Lock()
SHARED_CLINIC_NUM = None  # Store the clinic used for shared fetching

# === LOGGING ===
logging.basicConfig(
    level=getattr(logging, LOG_LEVEL),
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[logging.StreamHandler(sys.stdout)]
)
logger = logging.getLogger('opendental_sync')

# === DATA STRUCTURES ===
@dataclass
class SyncWindow:
    start_time: datetime.datetime
    end_time: datetime.datetime
    is_incremental: bool = False
    clinic_num: Optional[int] = None

@dataclass
class AppointmentFilter:
    clinic_num: int
    operatory_nums: List[int]
    valid_statuses: Set[str]
    broken_appointment_types: List[str]
    exclude_ghl_tagged: bool = True

# === SCHEDULING UTILITIES ===
def is_clinic_open(now: datetime.datetime) -> bool:
    now_clinic_tz = now.astimezone(CLINIC_TIMEZONE)
    hour = now_clinic_tz.hour
    return CLINIC_OPEN_HOUR <= hour < CLINIC_CLOSE_HOUR

def is_deep_sync_time(now: datetime.datetime) -> bool:
    now_clinic_tz = now.astimezone(CLINIC_TIMEZONE)
    return now_clinic_tz.hour == DEEP_SYNC_HOUR and now_clinic_tz.minute == 0

def get_next_run_time(now: datetime.datetime, force_deep_sync: bool = False) -> datetime.datetime:
    now_clinic_tz = now.astimezone(CLINIC_TIMEZONE)
    next_run = now_clinic_tz.replace(second=0, microsecond=0)
    
    if force_deep_sync or is_deep_sync_time(now):
        logger.info("Scheduling deep sync")
        return now_clinic_tz
    elif is_clinic_open(now):
        minutes = (now_clinic_tz.minute // INCREMENTAL_INTERVAL_MINUTES + 1) * INCREMENTAL_INTERVAL_MINUTES
        next_run = next_run.replace(minute=0, hour=now_clinic_tz.hour) + timedelta(minutes=minutes)
        if next_run.hour >= CLINIC_CLOSE_HOUR:
            next_run = next_run.replace(hour=DEEP_SYNC_HOUR, minute=0) + timedelta(days=1)
    else:
        next_run = next_run.replace(hour=DEEP_SYNC_HOUR, minute=0) + timedelta(days=1)
    
    return next_run

# === CONFIG VALIDATION ===
def validate_configuration() -> bool:
    errors = []
    if not DEVELOPER_KEY or not CUSTOMER_KEY:
        errors.append("Missing Open Dental API credentials")
    if not KERAGON_WEBHOOK_URL:
        errors.append("Missing Keragon webhook URL")
    if not CLINIC_NUMS:
        errors.append("No valid clinic numbers provided")
    for clinic in CLINIC_NUMS:
        if clinic not in CLINIC_OPERATORY_FILTERS:
            errors.append(f"No operatory filters defined for clinic {clinic}")
        if clinic not in CLINIC_BROKEN_APPOINTMENT_TYPE_FILTERS:
            errors.append(f"No broken appointment type filters defined for clinic {clinic}")
    
    if errors:
        logger.error("Configuration validation failed:")
        for error in errors:
            logger.error(f"- {error}")
        return False
    return True

# === OPTIMIZED SESSION MANAGEMENT ===
def get_optimized_session():
    global _session
    if _session is None:
        with _session_lock:
            if _session is None:
                _session = requests.Session()
                retry_strategy = Retry(
                    total=RETRY_ATTEMPTS,
                    backoff_factor=BACKOFF_FACTOR,
                    status_forcelist=[408, 429, 500, 502, 503, 504, 520, 521, 522, 523, 524],
                    allowed_methods=["HEAD", "GET", "POST"],
                    raise_on_status=False
                )
                adapter = HTTPAdapter(
                    max_retries=retry_strategy,
                    pool_connections=5,
                    pool_maxsize=10,
                    pool_block=True
                )
                _session.mount("http://", adapter)
                _session.mount("https://", adapter)
                _session.headers.update({
                    'User-Agent': 'OpenDental-Sync-Optimized/3.1',
                    'Accept': 'application/json',
                    'Accept-Encoding': 'gzip, deflate',
                    'Connection': 'keep-alive'
                })
                logger.info("Initialized optimized session")
    return _session

# === ADAPTIVE RATE LIMITING ===
class AdaptiveRateLimiter:
    def __init__(self, base_delay_seconds: float):
        self.base_delay = base_delay_seconds
        self.current_delay = base_delay_seconds
        self.last_request_time = 0
        self.response_times = []
        self.lock = threading.Lock()
        self.consecutive_slow_requests = 0
        self.max_delay = base_delay_seconds * 10

    def wait_if_needed(self):
        with self.lock:
            current_time = time.time()
            time_since_last = current_time - self.last_request_time
            if time_since_last < self.current_delay:
                sleep_time = self.current_delay - time_since_last
                logger.debug(f"Rate limiting: sleeping {sleep_time:.2f}s (current delay: {self.current_delay:.2f}s)")
                time.sleep(sleep_time)
            self.last_request_time = time.time()

    def record_response_time(self, response_time: float):
        with self.lock:
            self.response_times.append(response_time)
            if len(self.response_times) > 10:
                self.response_times.pop(0)
            if response_time > 10:
                self.consecutive_slow_requests += 1
                if self.consecutive_slow_requests >= 2:
                    self.current_delay = min(self.current_delay * 1.5, self.max_delay)
                    logger.warning(f"Slow response ({response_time:.2f}s), increasing delay to {self.current_delay:.2f}s")
            else:
                self.consecutive_slow_requests = 0
                if response_time < 3 and self.current_delay > self.base_delay:
                    self.current_delay = max(self.current_delay * 0.9, self.base_delay)
                    logger.debug(f"Fast response, reducing delay to {self.current_delay:.2f}s")

rate_limiter = AdaptiveRateLimiter(CLINIC_DELAY_SECONDS)

# === REQUEST CACHING ===
def get_request_fingerprint(url: str, params: Dict[str, Any]) -> str:
    content = f"{url}:{json.dumps(params, sort_keys=True)}"
    return hashlib.md5(content.encode()).hexdigest()

def get_cached_response(fingerprint: str) -> Optional[Any]:
    if not ENABLE_CACHING:
        return None
    with _cache_lock:
        if fingerprint in _request_cache:
            timestamp, data = _request_cache[fingerprint]
            if datetime.datetime.now() - timestamp < datetime.timedelta(minutes=CACHE_EXPIRY_MINUTES):
                logger.debug(f"Cache hit for request {fingerprint[:8]}...")
                return data
            else:
                del _request_cache[fingerprint]
    return None

def cache_response(fingerprint: str, data: Any):
    if not ENABLE_CACHING:
        return
    with _cache_lock:
        _request_cache[fingerprint] = (datetime.datetime.now(), data)
        if len(_request_cache) > 100:
            cutoff_time = datetime.datetime.now() - timedelta(minutes=CACHE_EXPIRY_MINUTES)
            expired_keys = [k for k, (t, _) in _request_cache.items() if t < cutoff_time]
            for key in expired_keys:
                del _request_cache[key]

# === APPOINTMENT TYPES CACHE ===
def load_appointment_types_cache() -> Dict[str, Dict[int, str]]:
    if not os.path.exists(APPT_TYPES_CACHE_FILE):
        logger.info("No appointment types cache file found")
        return {'shared': {}}
    try:
        with open(APPT_TYPES_CACHE_FILE) as f:
            data = json.load(f)
            logger.info(f"Loaded appointment types cache from {APPT_TYPES_CACHE_FILE} with {len(data.get('appointment_types', {}).get('shared', {}))} entries")
            return {'shared': {int(k): v for k, v in data.get('appointment_types', {}).get('shared', {}).items()}}
    except Exception as e:
        logger.error(f"Failed to load appointment types cache: {e}")
        return {'shared': {}}

def save_appointment_types_cache(appointment_types: Dict[str, Dict[int, str]]):
    temp_file = None
    try:
        with tempfile.NamedTemporaryFile(mode='w', delete=False, suffix='.tmp', dir=os.path.dirname(APPT_TYPES_CACHE_FILE) or '.') as f:
            cache_data = {
                'cache_date': datetime.datetime.now(CLINIC_TIMEZONE).isoformat(),
                'appointment_types': {'shared': {str(k): v for k, v in appointment_types.get('shared', {}).items()}}
            }
            json.dump(cache_data, f, indent=2)
            temp_file = f.name
        shutil.move(temp_file, APPT_TYPES_CACHE_FILE)
        logger.info(f"Saved appointment types cache to {APPT_TYPES_CACHE_FILE}")
    except Exception as e:
        logger.error(f"Failed to save appointment types cache: {e}")
        if temp_file and os.path.exists(temp_file):
            try:
                os.unlink(temp_file)
            except:
                pass

def get_appointment_types(clinic_num: int, force_refresh: bool = False) -> Dict[int, str]:
    global _appointment_types_cache, SHARED_CLINIC_NUM
    if not force_refresh and _appointment_types_cache.get('shared'):
        logger.debug(f"Using shared in-memory appointment types cache for clinic {clinic_num}")
        return _appointment_types_cache['shared']
    
    if not force_refresh:
        _appointment_types_cache = load_appointment_types_cache()
        if _appointment_types_cache.get('shared'):
            logger.debug(f"Using file-based shared appointment types cache for clinic {clinic_num}")
            return _appointment_types_cache['shared']
    
    fetch_clinic = SHARED_CLINIC_NUM if SHARED_CLINIC_NUM else clinic_num
    if not SHARED_CLINIC_NUM:
        SHARED_CLINIC_NUM = clinic_num
    
    try:
        logger.info(f"Fetching appointment types for clinic {fetch_clinic} (shared for all clinics)")
        params = {
            'ClinicNum': fetch_clinic,
            'limit': 400,
            'fields': 'AppointmentTypeNum,AppointmentTypeName'
        }
        appt_types = make_optimized_request('appointmenttypes', params)
        if appt_types is None:
            logger.error(f"Failed to fetch appointment types for clinic {fetch_clinic}")
            _appointment_types_cache['shared'] = {}
            save_appointment_types_cache(_appointment_types_cache)
            return _appointment_types_cache['shared']
        
        clinic_cache = {int(apt_type['AppointmentTypeNum']): apt_type['AppointmentTypeName'] 
                        for apt_type in appt_types if 'AppointmentTypeNum' in apt_type and 'AppointmentTypeName' in apt_type}
        logger.info(f"Loaded {len(clinic_cache)} appointment types for clinic {fetch_clinic} in single request")
        _appointment_types_cache['shared'] = clinic_cache
        save_appointment_types_cache(_appointment_types_cache)
        return clinic_cache
    except Exception as e:
        logger.error(f"Failed to fetch appointment types for clinic {fetch_clinic}: {e}")
        _appointment_types_cache['shared'] = {}
        save_appointment_types_cache(_appointment_types_cache)
        return _appointment_types_cache['shared']

def get_appointment_type_name(appointment: Dict[str, Any], clinic_num: int) -> str:
    apt_type_num = appointment.get('AppointmentTypeNum')
    if apt_type_num is None:
        apt_type_direct = appointment.get('AptType', '') or appointment.get('AppointmentType', '')
        if not apt_type_direct:
            logger.debug(f"No AppointmentTypeNum or AptType for appointment {appointment.get('AptNum')}")
        return apt_type_direct

    appointment_types = get_appointment_types(clinic_num)
    apt_type_name = appointment_types.get(int(apt_type_num), '')
    if apt_type_name:
        return apt_type_name
    else:
        logger.warning(f"No AppointmentTypeName for AppointmentTypeNum {apt_type_num} in clinic {clinic_num}")
    return apt_type_name

def has_ghl_tag(appointment: Dict[str, Any]) -> bool:
    return '[fromGHL]' in (appointment.get('Note', '') or '')

# === PATIENT CACHE ===
def load_patient_cache() -> Dict[int, Dict[str, Any]]:
    mega = Mega()
    try:
        m = mega.login(os.environ.get('MEGA_EMAIL'), os.environ.get('MEGA_PASSWORD'))
        logger.info("Logging in user...")
        # Get or create the patient_cache.json folder
        folder = None
        files = m.get_files()
        for file_id, file_info in files.items():
            # Check for folder (t=1) with name 'patient_cache.json'
            if file_info.get('t') == 1 and file_info.get('n') == 'patient_cache.json':
                folder = file_id
                break
        if not folder:
            folder = m.create_folder('patient_cache.json')
            logger.info("Created MEGA folder: patient_cache.json")
        
        # Find patient.txt in the folder
        file = None
        for file_id, file_info in files.items():
            # Check for file (t=0) with name 'patient.txt' in the folder
            if (file_info.get('t') == 0 and 
                file_info.get('n') == 'patient.txt' and 
                file_info.get('p') == folder):
                file = file_id
                break
        
        if file:
            m.download(file, dest_path=PATIENT_CACHE_FILE)
            logger.info(f"Downloaded patient cache from MEGA: patient_cache.json/patient.txt to {PATIENT_CACHE_FILE}")
        else:
            logger.info("No patient cache file found on MEGA")
    except Exception as e:
        logger.error(f"Failed to download patient cache from MEGA: {e}")
    
    if not os.path.exists(PATIENT_CACHE_FILE):
        logger.info("No patient cache file found")
        return {}
    
    try:
        with open(PATIENT_CACHE_FILE) as f:
            data = json.load(f)
            logger.info(f"Loaded patient cache from {PATIENT_CACHE_FILE} with {len(data.get('patients', {}))} patients")
            return {int(k): v for k, v in data.get('patients', {}).items()}
    except Exception as e:
        logger.error(f"Failed to load patient cache: {e}")
        return {}

def save_patient_cache(patient_cache: Dict[int, Dict[str, Any]]):
    temp_file = None
    try:
        # Save locally first
        with tempfile.NamedTemporaryFile(mode='w', delete=False, suffix='.tmp', dir=os.path.dirname(PATIENT_CACHE_FILE) or '.') as f:
            cache_data = {
                'cache_date': datetime.datetime.now(CLINIC_TIMEZONE).isoformat(),  # Fixed datetime.now
                'patients': {str(k): v for k, v in patient_cache.items()}
            }
            json.dump(cache_data, f, indent=2)
            temp_file = f.name
        shutil.move(temp_file, PATIENT_CACHE_FILE)
        logger.info(f"Saved patient cache to {PATIENT_CACHE_FILE} with {len(patient_cache)} patients")
        logger.debug(f"Verified patient_cache.json exists at {PATIENT_CACHE_FILE}")

        # Upload to MEGA
        mega = Mega()
        m = mega.login(os.environ.get('MEGA_EMAIL'), os.environ.get('MEGA_PASSWORD'))
        logger.info("Logging in user...")
        # Get or create the patient_cache.json folder
        folder = None
        files = m.get_files()
        for file_id, file_info in files.items():
            # Check for folder (t=1) with name 'patient_cache.json'
            if file_info.get('t') == 1 and file_info.get('n') == 'patient_cache.json':
                folder = file_id
                break
        if not folder:
            folder = m.create_folder('patient_cache.json')
            logger.info("Created MEGA folder: patient_cache.json")
        
        # Upload patient.txt to the folder
        m.upload(PATIENT_CACHE_FILE, folder, dest_filename='patient.txt')
        logger.info(f"Uploaded patient cache to MEGA: patient_cache.json/patient.txt")
    except Exception as e:
        logger.error(f"Failed to upload patient cache to MEGA: {e}")
    finally:
        if temp_file and os.path.exists(temp_file):
            try:
                os.unlink(temp_file)
            except Exception as e:
                logger.error(f"Failed to clean up temp file {temp_file}: {e}")
        # Create empty file as fallback
        logger.warning(f"Creating empty patient_cache.json due to error")
        with open(PATIENT_CACHE_FILE, 'w') as f:
            json.dump({'cache_date': datetime.datetime.now(CLINIC_TIMEZONE).isoformat(), 'patients': {}}, f, indent=2)
        if os.path.exists(PATIENT_CACHE_FILE):
            logger.debug(f"Created empty patient_cache.json at {PATIENT_CACHE_FILE}")

    # Upload to MEGA as patient.txt
    try:
        folder = m.find(folder_path)
        if not folder:
            m.create_folder(folder_path)
            folder = m.find(folder_path)
        file = m.find(file_name, folder=folder[0])
        if file:
            m.delete(file[0])
            logger.debug(f"Deleted existing patient cache in MEGA: {cache_path}")
        m.upload(PATIENT_CACHE_FILE, dest=folder[0], dest_filename=file_name)
        logger.info(f"Uploaded patient cache to MEGA: {cache_path}")
    except Exception as e:
        logger.error(f"Failed to upload patient cache to MEGA: {e}")

# === PROVIDER CACHE ===
def load_provider_cache() -> Dict[int, Dict[str, Any]]:
    if not os.path.exists(PROVIDER_CACHE_FILE):
        logger.info("No provider cache file found")
        return {}
    try:
        with open(PROVIDER_CACHE_FILE) as f:
            data = json.load(f)
            logger.info(f"Loaded provider cache from {PROVIDER_CACHE_FILE} with {len(data.get('providers', {}))} providers")
            return {int(k): v for k, v in data.get('providers', {}).items()}
    except Exception as e:
        logger.error(f"Failed to load provider cache: {e}")
        return {}

def save_provider_cache(provider_cache: Dict[int, Dict[str, Any]]):
    temp_file = None
    try:
        with tempfile.NamedTemporaryFile(mode='w', delete=False, suffix='.tmp', dir=os.path.dirname(PROVIDER_CACHE_FILE) or '.') as f:
            cache_data = {
                'cache_date': datetime.datetime.now(CLINIC_TIMEZONE).isoformat(),
                'providers': {str(k): v for k, v in provider_cache.items()}
            }
            json.dump(cache_data, f, indent=2)
            temp_file = f.name
        shutil.move(temp_file, PROVIDER_CACHE_FILE)
        logger.info(f"Saved provider cache to {PROVIDER_CACHE_FILE} with {len(provider_cache)} providers")
    except Exception as e:
        logger.error(f"Failed to save provider cache: {e}")
        if temp_file and os.path.exists(temp_file):
            try:
                os.unlink(temp_file)
            except:
                pass

def get_all_providers(force_refresh: bool = False) -> Dict[int, Dict[str, Any]]:
    global _provider_cache
    if not force_refresh and _provider_cache:
        return _provider_cache
    
    _provider_cache = load_provider_cache()
    if _provider_cache:
        return _provider_cache
    
    try:
        data = make_optimized_request('providers', {'limit': 1000, 'fields': 'ProvNum,FName,LName'})
        if data is None:
            logger.warning("Failed to fetch providers: No data returned")
            return {}
        _provider_cache = {int(p['ProvNum']): p for p in data if 'ProvNum' in p}
        save_provider_cache(_provider_cache)
        logger.info(f"Fetched and cached {len(_provider_cache)} providers")
        return _provider_cache
    except Exception as e:
        logger.warning(f"Failed to fetch providers: {e}")
        return {}

def get_provider_details(prov_num: int) -> Dict[str, Any]:
    providers = get_all_providers()
    return providers.get(prov_num, {})

# === EMPLOYEE CACHE ===
def load_employee_cache() -> Dict[int, Dict[str, Any]]:
    if not os.path.exists(EMPLOYEE_CACHE_FILE):
        logger.info("No employee cache file found")
        return {}
    try:
        with open(EMPLOYEE_CACHE_FILE) as f:
            data = json.load(f)
            logger.info(f"Loaded employee cache from {EMPLOYEE_CACHE_FILE} with {len(data.get('employees', {}))} employees")
            return {int(k): v for k, v in data.get('employees', {}).items()}
    except Exception as e:
        logger.error(f"Failed to load employee cache: {e}")
        return {}

def save_employee_cache(employee_cache: Dict[int, Dict[str, Any]]):
    temp_file = None
    try:
        with tempfile.NamedTemporaryFile(mode='w', delete=False, suffix='.tmp', dir=os.path.dirname(EMPLOYEE_CACHE_FILE) or '.') as f:
            cache_data = {
                'cache_date': datetime.datetime.now(CLINIC_TIMEZONE).isoformat(),
                'employees': {str(k): v for k, v in employee_cache.items()}
            }
            json.dump(cache_data, f, indent=2)
            temp_file = f.name
        shutil.move(temp_file, EMPLOYEE_CACHE_FILE)
        logger.info(f"Saved employee cache to {EMPLOYEE_CACHE_FILE} with {len(employee_cache)} employees")
    except Exception as e:
        logger.error(f"Failed to save employee cache: {e}")
        if temp_file and os.path.exists(temp_file):
            try:
                os.unlink(temp_file)
            except:
                pass

def get_all_employees(force_refresh: bool = False) -> Dict[int, Dict[str, Any]]:
    global _employee_cache
    if not force_refresh and _employee_cache:
        return _employee_cache
    
    _employee_cache = load_employee_cache()
    if _employee_cache:
        return _employee_cache
    
    try:
        data = make_optimized_request('employees', {'limit': 1000, 'fields': 'EmployeeNum,FName,LName'})
        if data is None:
            logger.warning("Failed to fetch employees: No data returned")
            return {}
        _employee_cache = {int(e['EmployeeNum']): e for e in data if 'EmployeeNum' in e}
        save_employee_cache(_employee_cache)
        logger.info(f"Fetched and cached {len(_employee_cache)} employees")
        return _employee_cache
    except Exception as e:
        logger.warning(f"Failed to fetch employees: {e}")
        return {}

def get_employee_details(emp_num: int) -> Dict[str, Any]:
    employees = get_all_employees()
    return employees.get(emp_num, {})

# === OPERATORY CACHE ===
def load_operatory_cache() -> Dict[int, List[Dict[str, Any]]]:
    if not os.path.exists(OPERATORY_CACHE_FILE):
        logger.info("No operatory cache file found")
        return {}
    try:
        with open(OPERATORY_CACHE_FILE) as f:
            data = json.load(f)
            cache = {}
            for clinic_str, ops in data.get('operatories', {}).items():
                clinic = int(clinic_str)
                cache[clinic] = ops
            logger.info(f"Loaded operatory cache from {OPERATORY_CACHE_FILE} with {sum(len(ops) for ops in cache.values())} operatories across {len(cache)} clinics")
            return cache
    except Exception as e:
        logger.error(f"Failed to load operatory cache: {e}")
        return {}

def save_operatory_cache(operatory_cache: Dict[int, List[Dict[str, Any]]]):
    temp_file = None
    try:
        with tempfile.NamedTemporaryFile(mode='w', delete=False, suffix='.tmp', dir=os.path.dirname(OPERATORY_CACHE_FILE) or '.') as f:
            cache_data = {
                'cache_date': datetime.datetime.now(CLINIC_TIMEZONE).isoformat(),
                'operatories': {str(k): v for k, v in operatory_cache.items()}
            }
            json.dump(cache_data, f, indent=2)
            temp_file = f.name
        shutil.move(temp_file, OPERATORY_CACHE_FILE)
        logger.info(f"Saved operatory cache to {OPERATORY_CACHE_FILE} with {sum(len(ops) for ops in operatory_cache.values())} operatories across {len(operatory_cache)} clinics")
    except Exception as e:
        logger.error(f"Failed to save operatory cache: {e}")
        if temp_file and os.path.exists(temp_file):
            try:
                os.unlink(temp_file)
            except:
                pass

def get_operatories(clinic_num: int, force_refresh: bool = False) -> List[Dict[str, Any]]:
    global _operatory_cache
    if not _operatory_cache:
        _operatory_cache = load_operatory_cache()
    
    if not force_refresh and clinic_num in _operatory_cache:
        logger.debug(f"Using cached operatories for clinic {clinic_num}")
        return _operatory_cache[clinic_num]
    
    try:
        data = make_optimized_request_paginated('operatories', {'ClinicNum': clinic_num, 'limit': PAGE_SIZE})
        if data is None:
            logger.warning(f"Failed to fetch operatories for clinic {clinic_num}: No data returned")
            return []
        _operatory_cache[clinic_num] = data
        save_operatory_cache(_operatory_cache)
        logger.info(f"Fetched and cached {len(data)} operatories for clinic {clinic_num}")
        return data
    except Exception as e:
        logger.warning(f"Failed to fetch operatories for clinic {clinic_num}: {e}")
        return []

# === STATE MANAGEMENT ===
def load_last_sync_times() -> Dict[int, Optional[datetime.datetime]]:
    state: Dict[int, Optional[datetime.datetime]] = {c: None for c in CLINIC_NUMS}
    if os.path.exists(STATE_FILE):
        try:
            with open(STATE_FILE) as f:
                data = json.load(f)
                if not isinstance(data, dict):
                    logger.error(f"Invalid state file format: expected dict, got {type(data)}")
                    return state
                for c in CLINIC_NUMS:
                    ts = data.get(str(c))
                    if ts:
                        dt = parse_time(ts)
                        if dt:
                            state[c] = dt
                        else:
                            logger.warning(f"Invalid timestamp for clinic {c}: {ts}")
        except json.JSONDecodeError as e:
            logger.error(f"Corrupted state file: {e}")
        except Exception as e:
            logger.error(f"Error reading state file: {e}")
    return state

def validate_sync_time_update(clinic: int, old_time: Optional[datetime.datetime], 
                             new_time: Optional[datetime.datetime]) -> bool:
    if not old_time:
        return True
    if not new_time:
        logger.warning(f"Clinic {clinic}: Attempted to set null sync time")
        return False
    if old_time.tzinfo is None:
        old_time = old_time.replace(tzinfo=CLINIC_TIMEZONE)
    if new_time.tzinfo is None:
        new_time = new_time.replace(tzinfo=CLINIC_TIMEZONE)
    if new_time < old_time:
        logger.warning(f"Clinic {clinic}: Attempted to move sync time backwards")
        return False
    return True

def save_state_atomically(sync_times: Dict[int, Optional[datetime.datetime]], 
                         sent_ids: Set[str]) -> None:
    temp_sync_file = None
    temp_sent_file = None
    try:
        sync_data = {str(c): dt.isoformat() if dt else None for c, dt in sync_times.items()}
        with tempfile.NamedTemporaryFile(mode='w', delete=False, suffix='.tmp', 
                                        dir=os.path.dirname(STATE_FILE) or '.') as f:
            json.dump(sync_data, f, indent=2)
            temp_sync_file = f.name
        with tempfile.NamedTemporaryFile(mode='w', delete=False, suffix='.tmp',
                                        dir=os.path.dirname(SENT_APPTS_FILE) or '.') as f:
            json.dump(list(sent_ids), f, indent=2)
            temp_sent_file = f.name
        shutil.move(temp_sync_file, STATE_FILE)
        shutil.move(temp_sent_file, SENT_APPTS_FILE)
        logger.info(f"Saved state files with {len(sent_ids)} sent appointment IDs")
    except Exception as e:
        logger.error(f"Error in atomic save: {e}")
        for temp_file in [temp_sync_file, temp_sent_file]:
            if temp_file and os.path.exists(temp_file):
                try:
                    os.unlink(temp_file)
                except:
                    pass
        raise

def load_sent_appointments() -> Set[str]:
    if os.path.exists(SENT_APPTS_FILE):
        try:
            with open(SENT_APPTS_FILE) as f:
                data = json.load(f)
                if not isinstance(data, list):
                    logger.error(f"Invalid sent appointments file format: expected list, got {type(data)}")
                    return set()
                return set(str(x) for x in data)
        except json.JSONDecodeError as e:
            logger.error(f"Corrupted sent appointments file: {e}")
        except Exception as e:
            logger.error(f"Error reading sent appointments file: {e}")
    return set()

# === UTILITIES ===
def parse_time(s: Optional[str]) -> Optional[datetime.datetime]:
    if not s:
        return None
    try:
        dt = datetime.datetime.fromisoformat(s.replace('Z', '+00:00'))
        if dt.tzinfo is None:
            logger.debug(f"Naive datetime received: {s}, assuming {CLINIC_TIMEZONE}")
            dt = dt.replace(tzinfo=CLINIC_TIMEZONE)
        else:
            dt = dt.astimezone(CLINIC_TIMEZONE)
        return dt
    except ValueError:
        logger.warning(f"Unrecognized time format: {s}")
        return None

def make_auth_header() -> Dict[str, str]:
    return {
        'Authorization': f'ODFHIR {DEVELOPER_KEY}/{CUSTOMER_KEY}',
        'Content-Type': 'application/json'
    }

def calculate_pattern_duration(pattern: str, minutes_per_slot: int = 5) -> int:
    return sum(1 for ch in (pattern or '').upper() if ch in ('X', '/')) * minutes_per_slot

def calculate_end_time(start_dt: datetime.datetime, pattern: str) -> datetime.datetime:
    dur = calculate_pattern_duration(pattern)
    if dur <= 0:
        logger.warning(f"No slots in pattern '{pattern}', defaulting to 60 minutes")
        dur = 60
    return start_dt + datetime.timedelta(minutes=dur)

# === REQUEST HANDLING ===
def make_optimized_request_paginated(endpoint: str, params: Dict[str, Any], method: str = 'GET', use_cache: bool = True) -> Optional[List[Any]]:
    logger.info(f"Using PAGE_SIZE={PAGE_SIZE} for endpoint {endpoint}")
    if not ENABLE_PAGINATION:
        return make_optimized_request(endpoint, params, method, use_cache)
    
    session = get_optimized_session()
    headers = make_auth_header()
    url = f"{API_BASE_URL}/{endpoint}"
    all_data = []
    params = params.copy()
    params['limit'] = PAGE_SIZE
    offset = 0
    
    fingerprint = get_request_fingerprint(url, params) if use_cache and method == 'GET' else None
    if use_cache and method == 'GET':
        cached_result = get_cached_response(fingerprint)
        if cached_result is not None:
            logger.info(f"Cache hit for {endpoint} with params {params}: fetched {len(cached_result)} records")
            return cached_result
    
    while True:
        params['offset'] = offset
        logger.debug(f"Fetching {endpoint} with params {params}")
        rate_limiter.wait_if_needed()
        start_time = time.time()
        try:
            if method == 'GET':
                response = session.get(url, headers=headers, params=params, timeout=REQUEST_TIMEOUT)
            else:
                response = session.post(url, headers=headers, json=params, timeout=REQUEST_TIMEOUT)
            
            logger.debug(f"API request: {response.request.url}")
            response.raise_for_status()
            response_time = time.time() - start_time
            rate_limiter.record_response_time(response_time)
            
            try:
                data = response.json()
                logger.debug(f"Raw API response: {json.dumps(data, indent=2)}")
            except ValueError:
                logger.error(f"Invalid JSON response from {endpoint}: {response.text}")
                return None
            
            if isinstance(data, dict) and data.get('error'):
                logger.error(f"API error for {endpoint}: {data.get('error')}")
                return None
            
            data_list = data if isinstance(data, list) else [data]
            logger.debug(f"Fetched {len(data_list)} records at offset={offset}: AptNums={[str(item.get('AptNum', 'N/A')) for item in data_list if isinstance(item, dict)]}")
            all_data.extend(data_list)
            
            if endpoint == 'appointments':
                filename = f'appointments_op_{params.get("Op", "unknown")}_{params.get("AptStatus", "unknown")}.json'
                with open(filename, 'w') as f:
                    json.dump(all_data, f, indent=2)
                logger.info(f"Saved {len(all_data)} records to {filename}")
            
            if len(data_list) < PAGE_SIZE:
                break
            offset += PAGE_SIZE
        except requests.exceptions.RequestException as e:
            response_time = time.time() - start_time
            rate_limiter.record_response_time(response_time)
            logger.error(f"Request to {endpoint} failed at offset={offset}: {e}")
            return None
    
    logger.info(f"Completed pagination for {endpoint} with params {params}: fetched {len(all_data)} records")
    if method == 'GET' and use_cache and all_data:
        cache_response(fingerprint, all_data)
    
    return all_data

def make_optimized_request(endpoint: str, params: Dict[str, Any], method: str = 'GET', use_cache: bool = True) -> Optional[List[Any]]:
    session = get_optimized_session()
    headers = make_auth_header()
    url = f"{API_BASE_URL}/{endpoint}"
    fingerprint = get_request_fingerprint(url, params) if use_cache and method == 'GET' else None
    
    if use_cache and method == 'GET':
        cached_result = get_cached_response(fingerprint)
        if cached_result is not None:
            return cached_result
    
    rate_limiter.wait_if_needed()
    start_time = time.time()
    try:
        if method == 'GET':
            response = session.get(url, headers=headers, params=params, timeout=REQUEST_TIMEOUT)
        else:
            response = session.post(url, headers=headers, json=params, timeout=REQUEST_TIMEOUT)
        
        logger.debug(f"API request: {response.request.url}")
        response.raise_for_status()
        response_time = time.time() - start_time
        rate_limiter.record_response_time(response_time)
        
        try:
            data = response.json()
            logger.debug(f"Raw API response: {json.dumps(data, indent=2)}")
        except ValueError:
            logger.error(f"Invalid JSON response from {endpoint}: {response.text}")
            return None
        
        if isinstance(data, dict) and data.get('error'):
            logger.error(f"API error for {endpoint}: {data.get('error')}")
            return None
        
        all_data = data if isinstance(data, list) else [data]
        
        if method == 'GET' and use_cache and all_data:
            cache_response(fingerprint, all_data)
        
        logger.debug(f"Request completed in {response_time:.2f}s, total records: {len(all_data)}")
        return all_data
    except requests.exceptions.RequestException as e:
        response_time = time.time() - start_time
        rate_limiter.record_response_time(response_time)
        logger.error(f"Request to {endpoint} failed: {e}")
        return None

# === PATIENT DATA ===
def get_patient_details(pat_num: int) -> Dict[str, Any]:
    global _patient_cache
    if not pat_num:
        return {}
    
    if pat_num in _patient_cache:
        logger.debug(f"Patient {pat_num}: Using in-memory cache")
        return _patient_cache[pat_num]
    
    if not _patient_cache:
        _patient_cache = load_patient_cache()
    
    if pat_num in _patient_cache:
        logger.debug(f"Patient {pat_num}: Using persistent cache")
        return _patient_cache[pat_num]
    
    try:
        data = make_optimized_request(f'patients/{pat_num}', {})
        if data is None:
            logger.warning(f"Failed to fetch patient {pat_num}: No data returned")
            return {}
        if isinstance(data, list) and len(data) == 1:
            patient_data = data[0]
        elif isinstance(data, dict):
            patient_data = data
        else:
            logger.warning(f"Unexpected response format for patient {pat_num}: {type(data)}")
            return {}
        
        _patient_cache[pat_num] = patient_data
        save_patient_cache(_patient_cache)
        logger.info(f"Patient {pat_num}: Fetched and cached")
        return patient_data
    except Exception as e:
        logger.warning(f"Failed to fetch patient {pat_num}: {e}")
        return {}

def fetch_patients_paginated(pat_nums: List[int]) -> Dict[int, Dict[str, Any]]:
    global _patient_cache
    if not pat_nums:
        logger.info("No patients to fetch")
        return {}
    
    if not _patient_cache:
        _patient_cache = load_patient_cache()
    
    patient_data = {pn: _patient_cache.get(pn, {}) for pn in pat_nums}
    missing_pat_nums = [pn for pn in pat_nums if not patient_data[pn]]
    
    if not missing_pat_nums:
        logger.info(f"All {len(pat_nums)} patient details found in cache")
        return patient_data
    
    logger.info(f"Fetching details for {len(missing_pat_nums)} patients: {missing_pat_nums}")
    for pn in missing_pat_nums:
        try:
            patient = get_patient_details(pn)
            if patient:
                patient_data[pn] = patient
                logger.info(f"Fetched patient {pn}")
            else:
                logger.warning(f"No data returned for patient {pn}")
            time.sleep(0.5)
        except Exception as e:
            logger.error(f"Error fetching patient {pn}: {e}")
    
    save_patient_cache(_patient_cache)
    return patient_data

# === SMART SYNC WINDOW GENERATION ===
def generate_sync_windows(
    clinic_num: int,
    last_sync: Optional[datetime.datetime],
    force_deep_sync: bool = False
) -> List[SyncWindow]:
    now_utc = datetime.datetime.utcnow().replace(tzinfo=timezone.utc)
    windows = []
    # Force deep sync if state file doesn't exist
    if not os.path.exists(STATE_FILE):
        logger.info(f"Clinic {clinic_num}: No state file found, forcing deep sync")
        force_deep_sync = True
    
    if last_sync and not force_deep_sync:
        time_since_last = now_utc - last_sync
        if time_since_last < timedelta(hours=24):
            logger.info(f"Clinic {clinic_num}: Incremental sync (last sync {time_since_last} ago)")
            window_start = last_sync - timedelta(hours=SAFETY_OVERLAP_HOURS)
            window_end = now_utc + timedelta(minutes=INCREMENTAL_SYNC_MINUTES)
            windows.append(SyncWindow(
                start_time=window_start,
                end_time=window_end,
                is_incremental=True,
                clinic_num=clinic_num
            ))
        else:
            logger.info(f"Clinic {clinic_num}: Deep sync needed (last sync {time_since_last} ago)")
            force_deep_sync = True
    if not last_sync or force_deep_sync:
        logger.info(f"Clinic {clinic_num}: Deep sync with {DEEP_SYNC_HOURS}-hour window")
        start_time = now_utc - timedelta(hours=SAFETY_OVERLAP_HOURS)  # Small overlap
        end_time = now_utc + timedelta(hours=DEEP_SYNC_HOURS)         # Look ahead 720 hours
        windows.append(SyncWindow(
            start_time=start_time,
            end_time=end_time,
            is_incremental=False,
            clinic_num=clinic_num
        ))
    logger.info(f"Clinic {clinic_num}: Generated {len(windows)} sync window(s)")
    return windows

# === OPTIMIZED APPOINTMENT FETCHING ===
def fetch_appointments_for_window(
    window: SyncWindow,
    appointment_filter: AppointmentFilter
) -> List[Dict[str, Any]]:
    clinic = appointment_filter.clinic_num
    start_time_utc = window.start_time.astimezone(timezone.utc).replace(hour=0, minute=0, second=0, microsecond=0)
    end_time_utc = window.end_time.astimezone(timezone.utc).replace(hour=23, minute=59, second=59, microsecond=999999)
    params = {
        'ClinicNum': str(clinic),
        'dateStart': start_time_utc.strftime('%Y-%m-%d'),
        'dateEnd': end_time_utc.strftime('%Y-%m-%d'),
        'limit': PAGE_SIZE,
        'fields': ','.join(REQUIRED_APPOINTMENT_FIELDS)
    }
    
    # Bypass cache for incremental syncs or data within 30 minutes
    use_cache = not window.is_incremental and (window.end_time - datetime.datetime.now(timezone.utc)).total_seconds() > 1800  # 30 minutes
    
    try:
        operatory_data = get_operatories(clinic)
        if operatory_data:
            valid_operatories = set(appointment_filter.operatory_nums)
            operatory_info = [
                {'OperatoryNum': op.get('OperatoryNum'), 'OperatoryName': op.get('OpName', 'Unknown')}
                for op in operatory_data if op.get('OperatoryNum') in valid_operatories
            ]
            logger.info(f"Clinic {clinic}: Valid operatories - {operatory_info}")
        else:
            logger.warning(f"Clinic {clinic}: No operatory data retrieved")
            return []
    except Exception as e:
        logger.error(f"Clinic {clinic}: Failed to fetch operatory data: {e}")
        return []
    
    all_appointments = []
    for status in appointment_filter.valid_statuses:
        for op_num in appointment_filter.operatory_nums:
            single_params = params.copy()
            single_params['AptStatus'] = status
            single_params['Op'] = str(op_num)
            try:
                appointments = make_optimized_request_paginated('appointments', single_params, use_cache=use_cache)
                logger.debug(f"Clinic {clinic}: Raw API response for AptStatus={status}, Op={op_num}: {len(appointments or [])} records")
                if appointments:
                    # Additional operatory validation
                    filtered_appointments = [
                        appt for appt in appointments 
                        if appt.get('Op') in appointment_filter.operatory_nums or appt.get('OperatoryNum') in appointment_filter.operatory_nums
                    ]
                    logger.debug(f"Clinic {clinic}: AptStatus={status}, Op={op_num} returned {len(appointments)} appointments, {len(filtered_appointments)} after operatory filter")
                    for appt in filtered_appointments:
                        logger.debug(f"Appt {appt.get('AptNum')}: Op={appt.get('Op')}, DateTStamp={appt.get('DateTStamp')}")
                    all_appointments.extend(filtered_appointments)
                else:
                    logger.debug(f"Client {clinic}: AptStatus={status}, Op={op_num} returned no appointments")
            except Exception as e:
                logger.error(f"Clinic {clinic}: Error with AptStatus={status}, Op={op_num}: {e}")
            time.sleep(1)
    
    if not all_appointments:
        logger.warning(f"Clinic {clinic}: No appointments found for specified operatories and statuses")
    
    logger.debug(f"Clinic {clinic}: {'Incremental' if window.is_incremental else 'Full'} sync returned {len(all_appointments)} appointments")
    return all_appointments

def apply_appointment_filters(
    appointments: List[Dict[str, Any]],
    appointment_filter: AppointmentFilter
) -> List[Dict[str, Any]]:
    clinic = appointment_filter.clinic_num
    valid = []
    for appt in appointments:
        apt_num = str(appt.get('AptNum', ''))
        status = str(appt.get('AptStatus', ''))
        op_num = appt.get('Op') or appt.get('OperatoryNum')
        pat_num = appt.get('PatNum')
        
        logger.debug(f"Checking appointment {apt_num}: Op={op_num}, DateTStamp={appt.get('DateTStamp')}")
        
        if not pat_num:
            logger.error(f"Excluding appointment {apt_num}: missing PatNum")
            continue
        if not op_num:
            logger.error(f"Excluding appointment {apt_num}: missing Op or OperatoryNum")
            continue
        if appointment_filter.exclude_ghl_tagged and has_ghl_tag(appt):
            logger.debug(f"Excluding appointment {apt_num}: GHL tag")
            continue
        if appointment_filter.valid_statuses and status not in appointment_filter.valid_statuses:
            logger.debug(f"Excluding appointment {apt_num}: invalid status: {status}")
            continue
        if appointment_filter.operatory_nums:
            try:
                op_num_int = int(op_num)
                if op_num_int not in appointment_filter.operatory_nums:
                    logger.debug(f"Excluding appointment {apt_num}: invalid operatory: {op_num}")
                    continue
            except (ValueError, TypeError):
                logger.error(f"Excluding appointment {apt_num}: invalid operatory format: {op_num}")
                continue
        if status == 'Broken' and appointment_filter.broken_appointment_types:
            apt_type_name = get_appointment_type_name(appt, clinic)
            if apt_type_name not in appointment_filter.broken_appointment_types:
                logger.debug(f"Excluding broken appointment {apt_num}: invalid type: {apt_type_name}")
                continue
        valid.append(appt)
    logger.info(f"Clinic {clinic}: Filtered {len(appointments)} → {len(valid)} appointments")
    return valid

def deduplicate_appointments(appointments: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    unique_appointments = {}
    for appt in appointments:
        apt_num = str(appt.get('AptNum', ''))
        pat_num = str(appt.get('PatNum', ''))
        apt_date_time = appt.get('AptDateTime', '')
        key = (apt_num, pat_num, apt_date_time)
        unique_appointments[key] = appt
    deduplicated = list(unique_appointments.values())
    logger.info(f"Deduplicated {len(appointments)} → {len(deduplicated)} appointments")
    return deduplicated

def fetch_appointments_optimized(
    clinic: int,
    since: Optional[datetime.datetime],
    force_deep_sync: bool = False
) -> List[Dict[str, Any]]:
    logger.info(f"Clinic {clinic}: Starting optimized fetch")
    appointment_filter = AppointmentFilter(
        clinic_num=clinic,
        operatory_nums=CLINIC_OPERATORY_FILTERS.get(clinic, []),
        valid_statuses=VALID_STATUSES,
        broken_appointment_types=CLINIC_BROKEN_APPOINTMENT_TYPE_FILTERS.get(clinic, []),
        exclude_ghl_tagged=True
    )
    windows = generate_sync_windows(clinic, since, force_deep_sync)
    window = windows[0]
    window_type = 'incremental' if window.is_incremental else 'full'
    logger.info(f"Clinic {clinic}: Processing {window_type} sync from {window.start_time} to {window.end_time}")
    
    all_appointments = []
    params = {
        'ClinicNum': str(clinic),
        'dateStart': window.start_time.strftime('%Y-%m-%d'),
        'dateEnd': window.end_time.strftime('%Y-%m-%d'),
        'limit': PAGE_SIZE,
        'fields': ','.join(REQUIRED_APPOINTMENT_FIELDS)
    }
    
    try:
        operatory_data = get_operatories(clinic)
        if operatory_data:
            valid_operatories = set(appointment_filter.operatory_nums)
            operatory_info = [
                {'OperatoryNum': op.get('OperatoryNum'), 'OperatoryName': op.get('OpName', 'Unknown')}
                for op in operatory_data if op.get('OperatoryNum') in valid_operatories
            ]
            logger.info(f"Clinic {clinic}: Valid operatories - {operatory_info}")
        else:
            logger.warning(f"Clinic {clinic}: No operatory data retrieved")
            return []
    except Exception as e:
        logger.error(f"Clinic {clinic}: Failed to fetch operatory data: {e}")
        return []
    
    for status in appointment_filter.valid_statuses:
        for op_num in appointment_filter.operatory_nums:
            single_params = params.copy()
            single_params['AptStatus'] = status
            single_params['Op'] = str(op_num)
            try:
                appointments = make_optimized_request_paginated('appointments', single_params)
                if appointments is None:
                    logger.error(f"Clinic {clinic}: No appointments returned for AptStatus={status}, Op={op_num}")
                    continue
                # Additional operatory validation
                filtered_appointments = [
                    appt for appt in appointments 
                    if appt.get('Op') in appointment_filter.operatory_nums or appt.get('OperatoryNum') in appointment_filter.operatory_nums
                ]
                logger.debug(f"Clinic {clinic}: AptStatus={status}, Op={op_num} returned {len(appointments)} appointments, {len(filtered_appointments)} after operatory filter")
                all_appointments.extend(filtered_appointments)
            except Exception as e:
                logger.error(f"Clinic {clinic}: Error fetching appointments for AptStatus={status}, Op={op_num}: {e}")
            time.sleep(1)
    
    if not all_appointments:
        logger.warning(f"Clinic {clinic}: No appointments found for specified operatories and statuses")
    
    time_filtered_appointments = [
        appt for appt in all_appointments
        if window.start_time <= parse_time(appt.get('AptDateTime' if not window.is_incremental else 'DateTStamp', '')) <= window.end_time
    ]
    filtered_appointments = apply_appointment_filters(time_filtered_appointments, appointment_filter)
    deduplicated_appointments = deduplicate_appointments(filtered_appointments)
    logger.info(
        f"Clinic {clinic}: {len(all_appointments)} raw → "
        f"{len(time_filtered_appointments)} time-filtered → "
        f"{len(filtered_appointments)} filtered → "
        f"{len(deduplicated_appointments)} deduplicated appointments"
    )
    return deduplicated_appointments

# === KERAGON INTEGRATION ===
def validate_keragon_payload(payload: Dict[str, Any]) -> bool:
    appt = payload.get('appointment', {})
    patient = payload.get('patient', {})
    required_appt_fields = ['AptNum', 'AptStatus', 'AptDateTime', 'ClinicNum', 'PatNum']
    required_patient_fields = ['FName', 'LName', 'PatNum']
    
    missing_appt = [f for f in required_appt_fields if not appt.get(f)]
    missing_patient = [f for f in required_patient_fields if not patient.get(f)]
    
    if missing_appt or missing_patient:
        logger.error(f"Invalid Keragon payload: Missing appointment fields {missing_appt}, patient fields {missing_patient}")
        return False
    return True

def send_to_keragon(appointment: Dict[str, Any], clinic: int, patient_data: Dict[int, Dict[str, Any]], dry_run: bool = False) -> bool:
    try:
        apt_num = str(appointment.get('AptNum', ''))
        pat_num = appointment.get('PatNum')
        if not pat_num:
            logger.error(f"Appointment {apt_num}: Missing PatNum")
            return False
        
        patient = patient_data.get(pat_num, {})
        start_time_str = appointment.get('AptDateTime', '')
        start_time = parse_time(start_time_str)
        if start_time is None:
            logger.error(f"Invalid AptDateTime for appointment {apt_num}: {start_time_str}")
            return False
        
        start_time = start_time.astimezone(CLINIC_TIMEZONE)
        pattern = appointment.get('Pattern', '')
        end_time = calculate_end_time(start_time, pattern) if start_time and pattern else start_time + timedelta(minutes=60)
        
        # Build staff string
        parts = []
        prov_num = appointment.get('ProvNum')
        if prov_num:
            prov = get_provider_details(int(prov_num))
            if prov:
                prov_name = f"Dr.%20{prov.get('LName', '').strip()}"
                if prov_name != "Dr.":
                    parts.append(prov_name)
        prov_hyg = appointment.get('ProvHyg')
        if prov_hyg:
            hyg = get_provider_details(int(prov_hyg))
            if hyg:
                hyg_name = f"{hyg.get('FName', '').strip()}%20{hyg.get('LName', '').strip()}".strip()
                if hyg_name:
                    parts.append(hyg_name)
        asst = appointment.get('Asst')
        if asst:
            asst_emp = get_employee_details(int(asst))
            if asst_emp:
                asst_name = f"{asst_emp.get('FName', '').strip()}%20{asst_emp.get('LName', '').strip()}".strip()
                if asst_name:
                    parts.append(asst_name)
        staff = ",".join(parts)
        
        payload = {
            'appointment': {
                'AptNum': apt_num,
                'AptStatus': appointment.get('AptStatus', ''),
                'AptDateTime': start_time.isoformat(),
                'EndTime': end_time.isoformat(),
                'Note': appointment.get('Note', ''),
                'AppointmentTypeNum': appointment.get('AppointmentTypeNum', ''),
                'AppointmentTypeName': get_appointment_type_name(appointment, clinic),
                'ClinicNum': str(clinic),
                'PatNum': str(pat_num),
                'OperatoryNum': str(appointment.get('Op') or appointment.get('OperatoryNum', '')),
                'Staff': staff
            },
            'patient': {
                'FName': patient.get('FName', ''),
                'LName': patient.get('LName', ''),
                'PatNum': str(pat_num),
                'Email': patient.get('Email', ''),
                'Address': patient.get('Address', ''),
                'HmPhone': patient.get('HmPhone', ''),
                'WkPhone': patient.get('WkPhone', ''),
                'Birthdate': patient.get('Birthdate', ''),
                'State': patient.get('State', ''),
                'Zip': patient.get('Zip', ''),
                'WirelessPh': patient.get('WirelessPhone', ''),
                'Gender': patient.get('Gender', '')
            }
        }
        
        if not validate_keragon_payload(payload):
            logger.error(f"Skipping appointment {apt_num} due to invalid payload")
            return False
        
        if dry_run:
            logger.info(f"Dry run: Would send appointment {apt_num} to Keragon: {json.dumps(payload, indent=2)}")
            return True
        
        logger.info(f"Sending payload for appointment {apt_num} to Keragon: {json.dumps(payload, indent=2)}")
        session = get_optimized_session()
        headers = {'Content-Type': 'application/json'}
        for attempt in range(RETRY_ATTEMPTS):
            try:
                response = session.post(KERAGON_WEBHOOK_URL, json=payload, headers=headers, timeout=30)
                logger.debug(f"Keragon request for {apt_num}: {response.request.url}")
                if response.status_code in (200, 201, 202, 204):
                    logger.info(f"✓ Successfully sent appointment {apt_num} (HTTP {response.status_code})")
                    return True
                else:
                    logger.error(f"Failed to send appointment {apt_num}: HTTP {response.status_code} - {response.text}")
            except requests.exceptions.RequestException as e:
                logger.error(f"Attempt {attempt + 1} failed for appointment {apt_num}: {e}")
            time.sleep(BACKOFF_FACTOR * (2 ** attempt))
        logger.error(f"Failed to send appointment {apt_num} after {RETRY_ATTEMPTS} attempts")
        return False
    except Exception as e:
        logger.error(f"Failed to send appointment {apt_num} to Keragon: {e}")
        return False

def send_batch_to_keragon(appointments: List[Dict[str, Any]], clinic: int, patient_data: Dict[int, Dict[str, Any]], dry_run: bool = False) -> Tuple[int, List[str]]:
    if not appointments:
        logger.info("No appointments to send to Keragon")
        return 0, []
    
    batch_size = BATCH_SIZE
    successful_sends = 0
    sent_appointment_ids = []
    for i in range(0, len(appointments), batch_size):
        batch = appointments[i:i + batch_size]
        for appt in batch:
            if send_to_keragon(appt, clinic, patient_data, dry_run):
                successful_sends += 1
                sent_appointment_ids.append(str(appt.get('AptNum', '')))
            time.sleep(0.5)
        if i + batch_size < len(appointments):
            time.sleep(2)
    return successful_sends, sent_appointment_ids

# === MAIN SYNC LOGIC ===
def process_clinic_optimized(
    clinic: int,
    last_syncs: Dict[int, Optional[datetime.datetime]],
    sent_appointments: Set[str],
    dry_run: bool = False,
    force_deep_sync: bool = False
) -> Tuple[int, List[str], datetime.datetime]:
    logger.info(f"Processing clinic {clinic}")
    since = last_syncs.get(clinic)
    appointment_types = get_appointment_types(clinic, force_refresh=force_deep_sync)
    appointments = fetch_appointments_optimized(clinic, since, force_deep_sync)
    
    new_appointments = []
    pat_nums = set()
    for appt in appointments:
        apt_num = str(appt.get('AptNum', ''))
        pat_num = appt.get('PatNum')
        if not pat_num:
            logger.warning(f"Skipping appointment {apt_num} with missing PatNum")
            continue
        
        # Check if the appointment is new or has been updated since last sync
        date_tstamp = parse_time(appt.get('DateTStamp'))
        if not date_tstamp:
            logger.warning(f"Skipping appointment {apt_num} with invalid or missing DateTStamp")
            continue
        
        # Strict check to prevent sending unchanged appointments
        if apt_num in sent_appointments:
            if since and date_tstamp <= since:
                logger.debug(f"Skipping already sent appointment {apt_num}: DateTStamp {date_tstamp} not newer than last sync {since}")
                continue
            else:
                logger.debug(f"Appointment {apt_num} has been updated: DateTStamp {date_tstamp} > last sync {since or 'None'}")
        
        pat_nums.add(pat_num)
        new_appointments.append(appt)
    
    logger.info(f"Clinic {clinic}: Found {len(new_appointments)} new or updated appointments for {len(pat_nums)} patients")
    
    patient_data = fetch_patients_paginated(list(pat_nums))
    
    sent_count, sent_ids = send_batch_to_keragon(new_appointments, clinic, patient_data, dry_run)
    logger.info(f"Clinic {clinic}: Successfully sent {sent_count} appointments")
    
    sync_time = datetime.datetime.utcnow().replace(tzinfo=timezone.utc)
    return sent_count, sent_ids, sync_time

def main_loop(dry_run: bool = False, force_deep_sync: bool = False, once: bool = False):
    if not validate_configuration():
        logger.error("Configuration validation failed, exiting")
        sys.exit(1)
    
    get_all_providers()
    get_all_employees()
    
    last_syncs = load_last_sync_times()
    sent_appointments = load_sent_appointments()
    
    while True:
        now = datetime.datetime.now(tz=timezone.utc)
        next_run = get_next_run_time(now, force_deep_sync)
        time_to_next = (next_run.astimezone(timezone.utc) - now).total_seconds()
        
        if time_to_next > 0 and not once:
            logger.info(f"Next run at {next_run.astimezone(CLINIC_TIMEZONE)} ({time_to_next/60:.1f} minutes from now)")
            time.sleep(time_to_next)
        
        total_sent = 0
        new_sent_ids = []
        sync_type = 'deep' if force_deep_sync or is_deep_sync_time(now) else 'incremental'
        logger.info(f"Starting {sync_type} sync for {len(CLINIC_NUMS)} clinics")
        
        for i, clinic in enumerate(CLINIC_NUMS):
            if i > 0:
                time.sleep(CLINIC_DELAY_SECONDS)
            sent_count, sent_ids, sync_time = process_clinic_optimized(
                clinic, last_syncs, sent_appointments, dry_run, force_deep_sync or is_deep_sync_time(now)
            )
            total_sent += sent_count
            new_sent_ids.extend(sent_ids)
            if validate_sync_time_update(clinic, last_syncs.get(clinic), sync_time):
                last_syncs[clinic] = sync_time
            else:
                logger.warning(f"Clinic {clinic}: Skipping sync time update due to invalid timestamp")
        
        if new_sent_ids:
            sent_appointments.update(new_sent_ids)
            try:
                save_state_atomically(last_syncs, sent_appointments)
                logger.info("Committed state and sent appointments")
            except Exception as e:
                logger.error(f"Failed to save state: {e}")
        
        logger.info(f"Sync complete: sent {total_sent} appointments across {len(CLINIC_NUMS)} clinics")
        # === Final safety: ensure cache exists even if no appointments were sent ===
        if not os.path.exists(PATIENT_CACHE_FILE):
            logger.warning("No appointments sent and no patient cache found — creating empty persistent patient_cache.json")
            save_patient_cache({})
        else:
            logger.info("Patient cache exists — no fallback needed")
        if once:
            break
        # Ensure the sync is fully complete before calculating the next run time
        now = datetime.datetime.now(tz=timezone.utc)
        next_run = get_next_run_time(now, force_deep_sync)
        time_to_next = (next_run.astimezone(timezone.utc) - now).total_seconds()
        if time_to_next > 0:
            logger.info(f"Sync completed, waiting for next run at {next_run.astimezone(CLINIC_TIMEZONE)}")
            time.sleep(time_to_next)

if __name__ == "__main__":
    import argparse
    parser = argparse.ArgumentParser(description="Open Dental API Sync Script")
    parser.add_argument('--dry-run', action='store_true', help="Run without sending to Keragon")
    parser.add_argument('--force-deep-sync', action='store_true', help="Force a deep sync")
    parser.add_argument('--once', action='store_true', help="Run once and exit")
    parser.add_argument('--verbose', action='store_true', help="Enable verbose logging")
    args = parser.parse_args()
    
    if args.verbose:
        logger.setLevel(logging.DEBUG)
    
    main_loop(dry_run=args.dry_run, force_deep_sync=args.force_deep_sync, once=args.once)
