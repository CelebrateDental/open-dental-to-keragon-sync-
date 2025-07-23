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

# === CONFIGURATION ===
API_BASE_URL        = os.environ.get('OPEN_DENTAL_API_URL', 'https://api.opendental.com/api/v1')
DEVELOPER_KEY       = os.environ.get('OPEN_DENTAL_DEVELOPER_KEY')
CUSTOMER_KEY        = os.environ.get('OPEN_DENTAL_CUSTOMER_KEY')
KERAGON_WEBHOOK_URL = os.environ.get('KERAGON_WEBHOOK_URL')

STATE_FILE      = 'last_sync_state.json'
SENT_APPTS_FILE = 'sent_appointments.json'
CACHE_FILE      = 'appointment_cache.json'
LOG_LEVEL       = os.environ.get('LOG_LEVEL', 'INFO')

# === DATABASE PERFORMANCE OPTIMIZATIONS ===
BATCH_SIZE = int(os.environ.get('BATCH_SIZE', '1'))  # Process 1 clinic at a time
CLINIC_DELAY_SECONDS = float(os.environ.get('CLINIC_DELAY_SECONDS', '5.0'))  # 5-second delays
MAX_CONCURRENT_CLINICS = int(os.environ.get('MAX_CONCURRENT_CLINICS', '1'))  # No parallel processing
REQUEST_TIMEOUT = int(os.environ.get('REQUEST_TIMEOUT', '120'))  # 2-minute timeout
RETRY_ATTEMPTS = int(os.environ.get('RETRY_ATTEMPTS', '5'))  # More retries
BACKOFF_FACTOR = float(os.environ.get('BACKOFF_FACTOR', '3.0'))  # Exponential backoff

# === SMART SYNC OPTIMIZATION ===
INCREMENTAL_SYNC_MINUTES = int(os.environ.get('INCREMENTAL_SYNC_MINUTES', '15'))  # 15-minute incremental sync
DEEP_SYNC_HOURS = int(os.environ.get('DEEP_SYNC_HOURS', '720'))  # 720-hour (30-day) look-ahead
SAFETY_OVERLAP_HOURS = int(os.environ.get('SAFETY_OVERLAP_HOURS', '2'))  # Safety overlap

# === SCHEDULING CONFIGURATION ===
CLINIC_TIMEZONE = ZoneInfo('America/Chicago')  # GMT-5 Central Time (CST/CDT)
CLINIC_OPEN_HOUR = 8  # 8 AM
CLINIC_CLOSE_HOUR = 20  # 8 PM
DEEP_SYNC_HOUR = 2  # 2 AM Central Time for deep sync
INCREMENTAL_INTERVAL_MINUTES = 60  # Incremental sync every 60 minutes during clinic hours

# === CACHING AND OPTIMIZATION ===
ENABLE_CACHING = os.environ.get('ENABLE_CACHING', 'true').lower() == 'true'
CACHE_EXPIRY_MINUTES = int(os.environ.get('CACHE_EXPIRY_MINUTES', '30'))
USE_SPECIFIC_FIELDS = os.environ.get('USE_SPECIFIC_FIELDS', 'true').lower() == 'true'
ENABLE_PAGINATION = os.environ.get('ENABLE_PAGINATION', 'true').lower() == 'true'
PAGE_SIZE = int(os.environ.get('PAGE_SIZE', '50'))  # Used for internal logic, not API
MAX_RECORDS_PER_REQUEST = int(os.environ.get('MAX_RECORDS_PER_REQUEST', '100'))

CLINIC_NUMS = [int(x) for x in os.environ.get('CLINIC_NUMS', '').split(',') if x.strip().isdigit()]

CLINIC_OPERATORY_FILTERS: Dict[int, List[int]] = {
    9034: [11579, 11580, 11588],
    9035: [11574, 11576, 11577],
}

CLINIC_BROKEN_APPOINTMENT_TYPE_FILTERS: Dict[int, List[str]] = {
    9034: ["COMP EX", "COMP EX CHILD"],
    9035: ["CASH CONSULT", "INSURANCE CONSULT"]
}

VALID_STATUSES = {'Scheduled', 'Complete', 'Broken'}  # String names for API compatibility

REQUIRED_APPOINTMENT_FIELDS = [
    'AptNum', 'AptDateTime', 'AptStatus', 'PatNum', 'Op', 'OperatoryNum',
    'Pattern', 'AppointmentTypeNum', 'Note', 'DateTStamp', 'FName', 'LName'
]

_appointment_types_cache: Dict[int, Dict[int, str]] = {}
_session = None
_session_lock = threading.Lock()
_request_cache: Dict[str, Tuple[datetime.datetime, Any]] = {}
_cache_lock = threading.Lock()

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
    """Check if the current time is within clinic hours (8 AM–8 PM Central Time)."""
    now_clinic_tz = now.astimezone(CLINIC_TIMEZONE)
    hour = now_clinic_tz.hour
    return CLINIC_OPEN_HOUR <= hour < CLINIC_CLOSE_HOUR

def is_deep_sync_time(now: datetime.datetime) -> bool:
    """Check if the current time is the scheduled deep sync time (2 AM Central Time)."""
    now_clinic_tz = now.astimezone(CLINIC_TIMEZONE)
    return now_clinic_tz.hour == DEEP_SYNC_HOUR and now_clinic_tz.minute == 0

def get_next_run_time(now: datetime.datetime) -> datetime.datetime:
    """Calculate the next time to run an incremental sync or deep sync."""
    now_clinic_tz = now.astimezone(CLINIC_TIMEZONE)
    next_run = now_clinic_tz.replace(second=0, microsecond=0)
    
    if is_deep_sync_time(now):
        return now_clinic_tz
    elif is_clinic_open(now):
        minutes = (now_clinic_tz.minute // INCREMENTAL_INTERVAL_MINUTES + 1) * INCREMENTAL_INTERVAL_MINUTES
        next_run = next_run.replace(minute=0, hour=now_clinic_tz.hour) + timedelta(minutes=minutes)
        if next_run.hour >= CLINIC_CLOSE_HOUR:
            next_run = next_run.replace(hour=DEEP_SYNC_HOUR, minute=0) + timedelta(days=1)
    else:
        if now_clinic_tz.hour >= DEEP_SYNC_HOUR:
            next_run = next_run.replace(hour=DEEP_SYNC_HOUR, minute=0) + timedelta(days=1)
        else:
            next_run = next_run.replace(hour=DEEP_SYNC_HOUR, minute=0)
    
    return next_run

# === CONFIG VALIDATION ===
def validate_configuration() -> bool:
    """Validate required configuration parameters."""
    if not DEVELOPER_KEY or not CUSTOMER_KEY:
        logger.error("Missing Open Dental API credentials")
        return False
    if not KERAGON_WEBHOOK_URL:
        logger.error("Missing Keragon webhook URL")
        return False
    if not CLINIC_NUMS:
        logger.error("No valid clinic numbers provided")
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
                    'User-Agent': 'OpenDental-Sync-DB-Optimized/3.0',
                    'Accept': 'application/json',
                    'Accept-Encoding': 'gzip, deflate',
                    'Connection': 'keep-alive'
                })
                logger.info("Initialized database-optimized session")
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
                    logger.warning(f"Database slow response ({response_time:.2f}s), increasing delay to {self.current_delay:.2f}s")
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
            cutoff_time = datetime.datetime.now() - datetime.timedelta(minutes=CACHE_EXPIRY_MINUTES)
            expired_keys = [k for k, (t, _) in _request_cache.items() if t < cutoff_time]
            for key in expired_keys:
                del _request_cache[key]

# === OPTIMIZED API REQUESTS ===
def make_optimized_request(
    endpoint: str,
    params: Dict[str, Any],
    method: str = 'GET',
    use_cache: bool = True
) -> Optional[List[Any]]:
    fingerprint = get_request_fingerprint(endpoint, params)
    if use_cache and method == 'GET':
        cached_result = get_cached_response(fingerprint)
        if cached_result is not None:
            return cached_result
    rate_limiter.wait_if_needed()
    all_data = []
    offset = 0
    params = params.copy()
    while True:
        start_time = time.time()
        try:
            session = get_optimized_session()
            url = f"{API_BASE_URL}/{endpoint.lstrip('/')}"
            if ENABLE_PAGINATION:
                params['Offset'] = offset  # Capitalized Offset
            logger.debug(f"Making request to {endpoint} with params: {params}")
            if method == 'GET':
                response = session.get(url, headers=make_auth_header(), params=params, timeout=REQUEST_TIMEOUT)
            else:
                response = session.post(url, headers=make_auth_header(), json=params, timeout=REQUEST_TIMEOUT)
            response_time = time.time() - start_time
            rate_limiter.record_response_time(response_time)
            if response.status_code == 429:
                retry_after = response.headers.get('Retry-After')
                wait_time = float(retry_after) if retry_after and retry_after.isdigit() else 10
                logger.warning(f"Rate limited by API, waiting {wait_time}s...")
                time.sleep(wait_time)
                continue
            if response.status_code >= 400:
                logger.error(f"Request to {endpoint} failed: {response.status_code} {response.reason}, Response: {response.text}")
                if response.status_code == 400:
                    try:
                        logger.error(f"Full API response: {response.json()}")
                    except:
                        logger.error("Failed to parse JSON response")
                return None
            response.raise_for_status()
            try:
                data = response.json()
            except json.JSONDecodeError as e:
                logger.error(f"Invalid JSON response from {endpoint}: {e}")
                return None
            page_data = data if isinstance(data, list) else [data]
            all_data.extend(page_data)
            if len(page_data) < PAGE_SIZE:
                break
            offset += PAGE_SIZE
            logger.debug(f"Fetched {len(page_data)} records, advancing to offset {offset}")
        except requests.exceptions.Timeout:
            logger.error(f"Request to {endpoint} timed out after {REQUEST_TIMEOUT}s")
            return None
        except requests.exceptions.RequestException as e:
            logger.error(f"Request to {endpoint} failed: {e}")
            return None
        except Exception as e:
            logger.error(f"Unexpected error in request to {endpoint}: {e}")
            return None
    if method == 'GET' and use_cache and all_data:
        cache_response(fingerprint, all_data)
    logger.debug(f"Request completed in {response_time:.2f}s, total records: {len(all_data)}")
    return all_data

# === APPOINTMENT TYPES CACHE ===
def get_appointment_types(clinic_num: int) -> Dict[int, str]:
    global _appointment_types_cache
    if clinic_num in _appointment_types_cache:
        return _appointment_types_cache[clinic_num]
    try:
        logger.info(f"Fetching appointment types for clinic {clinic_num}")
        params = {'ClinicNum': clinic_num}
        data = make_optimized_request('appointmenttypes', params)
        if data is None:
            logger.error(f"Failed to fetch appointment types for clinic {clinic_num}")
            _appointment_types_cache[clinic_num] = {}
            return _appointment_types_cache[clinic_num]
        appt_types = data if isinstance(data, list) else [data]
        clinic_cache = {}
        for apt_type in appt_types:
            type_num = apt_type.get('AppointmentTypeNum')
            type_name = apt_type.get('AppointmentTypeName', '')
            if type_num is not None:
                clinic_cache[type_num] = type_name
        _appointment_types_cache[clinic_num] = clinic_cache
        logger.info(f"Loaded {len(clinic_cache)} appointment types for clinic {clinic_num}")
        return clinic_cache
    except Exception as e:
        logger.error(f"Failed to fetch appointment types for clinic {clinic_num}: {e}")
        _appointment_types_cache[clinic_num] = {}
        return _appointment_types_cache[clinic_num]

def get_appointment_type_name(appointment: Dict[str, Any], clinic_num: int) -> str:
    apt_type_num = appointment.get('AppointmentTypeNum')
    if apt_type_num is not None:
        appointment_types = get_appointment_types(clinic_num)
        apt_type_name = appointment_types.get(apt_type_num, '')
        if apt_type_name:
            return apt_type_name
    apt_type_direct = appointment.get('AptType', '') or appointment.get('AppointmentType', '')
    return apt_type_direct if apt_type_direct else ''

def get_appointment_type_num(appointment: Dict[str, Any]) -> Optional[int]:
    return appointment.get('AppointmentTypeNum')

def has_ghl_tag(appointment: Dict[str, Any]) -> bool:
    return '[fromGHL]' in (appointment.get('Note', '') or '')

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
        old_time = old_time.replace(tzinfo=timezone.utc)
    if new_time.tzinfo is None:
        new_time = new_time.replace(tzinfo=timezone.utc)
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
        logger.debug(f"Saved state files with {len(sent_ids)} sent appointment IDs")
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
            logger.warning(f"Naive datetime received: {s}, assuming UTC")
            dt = dt.replace(tzinfo=timezone.utc)
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

# === SMART SYNC WINDOW GENERATION ===
def generate_sync_windows(
    clinic_num: int,
    last_sync: Optional[datetime.datetime],
    force_deep_sync: bool = False
) -> List[SyncWindow]:
    now_utc = datetime.datetime.utcnow().replace(tzinfo=timezone.utc)
    windows = []
    if last_sync and not force_deep_sync:
        time_since_last = now_utc - last_sync
        if time_since_last < datetime.timedelta(hours=24):  # Allow 24 hours for incremental
            logger.info(f"Clinic {clinic_num}: Incremental sync (last sync {time_since_last} ago)")
            window_start = last_sync - datetime.timedelta(hours=SAFETY_OVERLAP_HOURS)
            window_end = now_utc + datetime.timedelta(minutes=INCREMENTAL_SYNC_MINUTES)
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
        if last_sync:
            start_time = max(
                last_sync - datetime.timedelta(hours=SAFETY_OVERLAP_HOURS), 
                now_utc - datetime.timedelta(hours=24)
            )
        else:
            start_time = now_utc - datetime.timedelta(hours=24)
        end_time = now_utc + datetime.timedelta(hours=DEEP_SYNC_HOURS)
        windows.append(SyncWindow(
            start_time=start_time,
            end_time=end_time,
            is_incremental=False,
            clinic_num=clinic_num
        ))
    logger.info(f"Clinic {clinic_num}: Generated {len(windows)} sync window")
    return windows

# === OPTIMIZED APPOINTMENT FETCHING ===
def fetch_appointments_for_window(
    window: SyncWindow,
    appointment_filter: AppointmentFilter,
    since: Optional[datetime.datetime] = None
) -> List[Dict[str, Any]]:
    clinic = appointment_filter.clinic_num
    start_time_utc = window.start_time.astimezone(timezone.utc).replace(hour=0, minute=0, second=0, microsecond=0)
    end_time_utc = window.end_time.astimezone(timezone.utc).replace(hour=23, minute=59, second=59, microsecond=999999)
    params = {
        'ClinicNum': clinic,
        'dateStart': start_time_utc.strftime('%Y-%m-%d'),
        'dateEnd': end_time_utc.strftime('%Y-%m-%d'),
    }
    # Log operatory names and numbers for the clinic
    try:
        operatory_data = make_optimized_request('operatories', {'ClinicNum': clinic})
        if operatory_data:
            operatory_info = [
                {'OperatoryNum': op.get('OperatoryNum'), 'OperatoryName': op.get('OpName', 'Unknown')}
                for op in operatory_data if op.get('OperatoryNum') in appointment_filter.operatory_nums
            ]
            logger.info(f"Clinic {clinic}: Operatories - {operatory_info}")
        else:
            logger.warning(f"Clinic {clinic}: No operatory data retrieved")
    except Exception as e:
        logger.error(f"Clinic {clinic}: Failed to fetch operatory data: {e}")
    
    if window.is_incremental:
        if since is None:
            since = datetime.datetime.utcnow().replace(tzinfo=timezone.utc) - datetime.timedelta(days=30)
        if since.tzinfo is None:
            since = since.replace(tzinfo=timezone.utc)
        eff_utc = (since + timedelta(seconds=1)).astimezone(timezone.utc)
        params['DateTStamp'] = eff_utc.strftime('%Y-%m-%d %H:%M:%S')
        logger.debug(f"Clinic {clinic}: Applying serverDateTime {params['DateTStamp']} for incremental sync")
    else:
        logger.debug(f"Clinic {clinic}: No serverDateTime filter applied for deep sync")
    
    logger.debug(f"Clinic {clinic}: Fetching {start_time_utc} to {end_time_utc} (UTC, effective dates {params['dateStart']} to {params['dateEnd']})")
    
    all_appointments = []
    for status in appointment_filter.valid_statuses:
        for op_num in appointment_filter.operatory_nums:
            single_params = params.copy()
            single_params['AptStatus'] = status
            single_params['Op'] = str(op_num)  # Individual operatory number
            try:
                appointments = make_optimized_request('appointments', single_params)
                logger.debug(f"Clinic {clinic}: Raw API response for AptStatus={status}, Op={op_num}: {appointments}")
                if appointments is not None:
                    logger.debug(f"Clinic {clinic}: AptStatus={status}, Op={op_num} returned {len(appointments)} appointments")
                    all_appointments.extend(appointments)
                else:
                    logger.warning(f"Clinic {clinic}: AptStatus={status}, Op={op_num} request failed")
            except Exception as e:
                logger.error(f"Clinic {clinic}: Error with AptStatus={status}, Op={op_num}: {e}")
            time.sleep(1)  # Small delay between individual requests
    
    if not all_appointments:
        logger.warning(f"Clinic {clinic}: All AptStatus/Op attempts failed, retrying without status or operatory filter")
        params.pop('AptStatus', None)
        params.pop('Op', None)
        try:
            appointments = make_optimized_request('appointments', params)
            logger.debug(f"Clinic {clinic}: Raw API response for fallback: {appointments}")
            if appointments is not None:
                logger.debug(f"Clinic {clinic}: Fallback request returned {len(appointments)} appointments")
                all_appointments.extend(appointments)
            else:
                logger.error(f"Clinic {clinic}: Fallback request failed")
        except Exception as e:
            logger.error(f"Clinic {clinic}: Error in fallback request: {e}")
    
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
        status = str(appt.get('AptStatus', ''))  # Convert to string for comparison
        # Use an explicit None check instead of “or”
        op_num = appt.get('Op')
        if op_num is None:
            op_num = appt.get('OperatoryNum')

        if appointment_filter.exclude_ghl_tagged and has_ghl_tag(appt):
            continue
        if appointment_filter.valid_statuses and status not in appointment_filter.valid_statuses:
            continue
        if appointment_filter.operatory_nums and op_num not in appointment_filter.operatory_nums:
            continue
        if status == 'Broken' and appointment_filter.broken_appointment_types:
            apt_type_name = get_appointment_type_name(appt, clinic)
            if apt_type_name not in appointment_filter.broken_appointment_types:
                continue
        valid.append(appt)
    return valid

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
    all_appointments = []
    window = windows[0]  # Single window
    window_type = 'incremental' if window.is_incremental else 'full'
    logger.info(f"Clinic {clinic}: Processing {window_type} sync")
    try:
        window_appointments = fetch_appointments_for_window(window, appointment_filter, since)
        if window_appointments:
            filtered_appointments = apply_appointment_filters(window_appointments, appointment_filter)
            all_appointments.extend(filtered_appointments)
            logger.info(f"Clinic {clinic}: {len(window_appointments)} raw → {len(filtered_appointments)} filtered appointments")
        else:
            logger.info(f"Clinic {clinic}: No appointments found")
    except Exception as e:
        logger.error(f"Error processing sync for clinic {clinic}: {e}")
    unique_appointments = {}
    for appt in all_appointments:
        apt_num = str(appt.get('AptNum', ''))
        if apt_num not in unique_appointments:
            unique_appointments[apt_num] = appt
    final_appointments = list(unique_appointments.values())
    logger.info(f"Clinic {clinic}: {len(final_appointments)} unique appointments after deduplication")
    return final_appointments

# === APPOINTMENT VERIFICATION SYSTEM ===
def verify_appointment_coverage(
    clinic: int,
    appointments: List[Dict[str, Any]],
    sync_window_start: datetime.datetime,
    sync_window_end: datetime.datetime
) -> Dict[str, Any]:
    verification_report = {
        'clinic': clinic,
        'total_appointments': len(appointments),
        'coverage_gaps': [],
        'recommendations': [],
        'time_range_covered': {
            'start': sync_window_start.isoformat(),
            'end': sync_window_end.isoformat()
        }
    }
    if not appointments:
        verification_report['coverage_gaps'].append("No appointments found - possible gap")
        verification_report['recommendations'].append("Consider running with --force-deep-sync")
        return verification_report
    appointment_times = []
    for appt in appointments:
        apt_time = parse_time(appt.get('AptDateTime'))
        if apt_time:
            appointment_times.append(apt_time)
    if not appointment_times:
        verification_report['coverage_gaps'].append("No appointments with valid times")
        return verification_report
    appointment_times.sort()
    for i in range(1, len(appointment_times)):
        gap_hours = (appointment_times[i] - appointment_times[i-1]).total_seconds() / 3600
        if gap_hours > 24:
            verification_report['coverage_gaps'].append(
                f"Large gap: {gap_hours:.1f} hours between appointments"
            )
    now_utc = datetime.datetime.utcnow().replace(tzinfo=timezone.utc)
    latest_appt = max(appointment_times)
    hours_since_latest = (now_utc - latest_appt).total_seconds() / 3600
    if hours_since_latest > 48:
        verification_report['coverage_gaps'].append(
            f"No appointments in last {hours_since_latest:.1f} hours"
        )
        verification_report['recommendations'].append("Consider --force-deep-sync")
    status_counts = {}
    for appt in appointments:
        status = appt.get('AptStatus', 'Unknown')
        status_counts[status] = status_counts.get(status, 0) + 1
    verification_report['appointment_status_distribution'] = status_counts
    logger.info(f"📊 Coverage Report - Clinic {clinic}: {len(appointments)} appointments")
    if verification_report['coverage_gaps']:
        logger.warning(f"⚠️ Coverage gaps detected for clinic {clinic}")
    return verification_report

def save_verification_report(clinic_reports: List[Dict[str, Any]]):
    report_file = 'appointment_coverage_report.json'
    try:
        full_report = {
            'timestamp': datetime.datetime.now().isoformat(),
            'clinic_reports': clinic_reports,
            'summary': {
                'total_clinics': len(clinic_reports),
                'total_appointments': sum(r['total_appointments'] for r in clinic_reports),
                'clinics_with_gaps': sum(1 for r in clinic_reports if r['coverage_gaps']),
            }
        }
        with open(report_file, 'w') as f:
            json.dump(full_report, f, indent=2)
        logger.info(f"📄 Verification report saved to {report_file}")
    except Exception as e:
        logger.error(f"Failed to save verification report: {e}")

# === PATIENT DETAILS ===
def get_patient_details(pat_num: int) -> Dict[str, Any]:
    if not pat_num:
        return {}
    try:
        data = make_optimized_request(f'patients/{pat_num}', {})
        return data if data else {}
    except Exception as e:
        logger.warning(f"Failed to fetch patient {pat_num}: {e}")
        return {}

# === KERAGON SENDING ===
def send_to_keragon(appt: Dict[str, Any], clinic: int, dry_run: bool = False) -> bool:
    apt_num = str(appt.get('AptNum', ''))
    try:
        patient = get_patient_details(appt.get('PatNum'))
        first = patient.get('FName') or appt.get('FName', '')
        last = patient.get('LName') or appt.get('LName', '')
        name = f"{first} {last}".strip() or 'Unknown'
        provider_name = 'Dr. Gharbi' if clinic == 9034 else 'Dr. Ensley'
        apt_type_num = get_appointment_type_num(appt)
        st_utc = parse_time(appt.get('AptDateTime'))
        if not st_utc:
            logger.error(f"Invalid start time for appointment {apt_num}")
            return False
        st_clinic = st_utc.replace(tzinfo=None).replace(tzinfo=CLINIC_TIMEZONE)
        duration_minutes = calculate_pattern_duration(appt.get('Pattern', ''))
        if duration_minutes <= 0:
            duration_minutes = 60
        en_clinic = st_clinic + datetime.timedelta(minutes=duration_minutes)
        st_payload = st_clinic.isoformat(timespec='seconds')
        en_payload = en_clinic.isoformat(timespec='seconds')
        payload = {
            'appointmentId': apt_num,
            'appointmentTime': st_payload,
            'appointmentEndTime': en_payload,
            'appointmentDurationMinutes': duration_minutes,
            'appointmentTypeNum': apt_type_num,
            'status': appt.get('AptStatus'),
            'notes': appt.get('Note', ''),
            'patientId': str(appt.get('PatNum')),
            'firstName': first,
            'lastName': last,
            'providerName': provider_name,
            'email': patient.get('Email', ''),
            'phone': patient.get('WirelessPhone', '') or patient.get('HmPhone', ''),
            'gender': patient.get('Gender', ''),
            'birthdate': patient.get('Birthdate', ''),
            'address': patient.get('Address', ''),
            'city': patient.get('City', ''),
            'state': patient.get('State', ''),
            'zipCode': patient.get('Zip', ''),
            'balance': patient.get('Balance', 0)
        }
        logger.info(f"📤 Preparing to send appointment {apt_num} to Keragon:\n{json.dumps(payload, indent=2)}")
        if dry_run:
            logger.info(f"DRY RUN: Would send appointment {apt_num}")
            return True
        max_retries = 3
        for attempt in range(max_retries):
            try:
                session = get_optimized_session()
                r = session.post(KERAGON_WEBHOOK_URL, json=payload, timeout=30)
                r.raise_for_status()
                logger.info(f"✓ Successfully sent appointment {apt_num}")
                return True
            except requests.exceptions.RequestException as e:
                if attempt == max_retries - 1:
                    logger.error(f"✗ Failed to send appointment {apt_num} after {max_retries} attempts: {e}")
                    return False
                logger.warning(f"Retry {attempt + 1} for appointment {apt_num}")
                time.sleep(2 ** attempt)
        return False
    except Exception as e:
        logger.error(f"✗ Error processing appointment {apt_num}: {e}")
        return False

def send_batch_to_keragon(appointments: List[Dict[str, Any]], clinic: int, dry_run: bool = False) -> Tuple[int, List[str]]:
    if not appointments:
        return 0, []
    successful_sends = 0
    sent_appointment_ids = []
    batch_size = 5
    for i in range(0, len(appointments), batch_size):
        batch = appointments[i:i + batch_size]
        logger.info(f"Clinic {clinic}: Processing batch {i//batch_size + 1} ({len(batch)} appointments)")
        for appt in batch:
            if send_to_keragon(appt, clinic, dry_run):
                successful_sends += 1
                sent_appointment_ids.append(str(appt.get('AptNum', '')))
            time.sleep(0.5)
        if i + batch_size < len(appointments):
            time.sleep(2)
    return successful_sends, sent_appointment_ids

# === CLINIC PROCESSING ===
def process_clinic_optimized(
    clinic: int,
    last_syncs: Dict[int, Optional[datetime.datetime]],
    sent_appointments: Set[str],
    dry_run: bool = False,
    force_deep_sync: bool = False
) -> Dict[str, Any]:
    logger.info(f"=== Processing Clinic {clinic} ===")
    try:
        appointment_types = get_appointment_types(clinic)
        logger.info(f"Clinic {clinic}: Loaded {len(appointment_types)} appointment types")
        appointments = fetch_appointments_optimized(clinic, last_syncs.get(clinic), force_deep_sync)
        if not appointments:
            logger.info(f"Clinic {clinic}: No appointments to process")
            return {
                'clinic': clinic,
                'sent': 0,
                'skipped': 0,
                'processed': 0,
                'latest_timestamp': last_syncs.get(clinic),
                'appointments_sent': []
            }
        new_appointments = []
        skipped_count = 0
        for appt in appointments:
            apt_num = str(appt.get('AptNum', ''))
            if apt_num in sent_appointments:
                skipped_count += 1
                continue
            new_appointments.append(appt)
        logger.info(f"Clinic {clinic}: {len(new_appointments)} new, {skipped_count} already sent")
        if not new_appointments:
            return {
                'clinic': clinic,
                'sent': 0,
                'skipped': skipped_count,
                'processed': len(appointments),
                'latest_timestamp': last_syncs.get(clinic),
                'appointments_sent': []
            }
        sent_count, sent_ids = send_batch_to_keragon(new_appointments, clinic, dry_run)
        latest_timestamp = last_syncs.get(clinic)
        for appt in appointments:
            appt_timestamp = parse_time(appt.get('DateTStamp'))
            if appt_timestamp:
                if not latest_timestamp or appt_timestamp > latest_timestamp:
                    latest_timestamp = appt_timestamp
        logger.info(f"Clinic {clinic}: ✓ Sent {sent_count}/{len(new_appointments)} successfully")
        return {
            'clinic': clinic,
            'sent': sent_count,
            'skipped': skipped_count,
            'processed': len(appointments),
            'latest_timestamp': latest_timestamp,
            'appointments_sent': sent_ids
        }
    except Exception as e:
        logger.error(f"Error processing clinic {clinic}: {e}")
        return {
            'clinic': clinic,
            'sent': 0,
            'skipped': 0,
            'processed': 0,
            'latest_timestamp': last_syncs.get(clinic),
            'appointments_sent': [],
            'error': str(e)
        }

# === SAFETY CHECKS ===
def run_safety_checks(clinic_results: List[Dict[str, Any]]) -> List[str]:
    safety_warnings = []
    for result in clinic_results:
        clinic = result['clinic']
        if result['processed'] == 0:
            safety_warnings.append(f"Clinic {clinic}: No appointments processed")
        if result['processed'] > 0 and result['latest_timestamp'] is None:
            safety_warnings.append(f"Clinic {clinic}: Sync time not updated")
        if result['sent'] == 0 and result['skipped'] > 0:
            safety_warnings.append(f"Clinic {clinic}: All appointments skipped")
    return safety_warnings

# === MAIN SYNC FUNCTION ===
def run_sync(dry_run: bool = False, force_deep_sync: bool = False):
    if not validate_configuration():
        sys.exit(1)
    logger.info("=== ULTRA-OPTIMIZED OpenDental → Keragon Sync ===")
    logger.info("🚀 DATABASE PERFORMANCE OPTIMIZATIONS:")
    logger.info(f"  • Sequential processing (no parallel DB hits)")
    logger.info(f"  • Single date range per sync")
    logger.info(f"  • Adaptive rate limiting: {CLINIC_DELAY_SECONDS}s base delay")
    logger.info(f"  • Deep sync window: {DEEP_SYNC_HOURS} hours")
    logger.info(f"  • Request caching: {'enabled' if ENABLE_CACHING else 'disabled'}")
    logger.info(f"  • Safety overlap: {SAFETY_OVERLAP_HOURS} hours")
    if dry_run:
        logger.info("🔍 DRY RUN MODE")
    if force_deep_sync:
        logger.info("🔄 FORCE DEEP SYNC")
    last_syncs = load_last_sync_times()
    sent_appointments = load_sent_appointments()
    new_syncs = last_syncs.copy()
    total_sent = 0
    total_skipped = 0
    total_processed = 0
    state_changed = False
    verification_reports = []
    logger.info(f"📊 Processing {len(CLINIC_NUMS)} clinics SEQUENTIALLY")
    clinic_results = []
    for i, clinic in enumerate(CLINIC_NUMS):
        logger.info(f"🏥 Processing clinic {clinic} ({i+1}/{len(CLINIC_NUMS)})")
        if i > 0:
            logger.info(f"⏱️ Waiting {CLINIC_DELAY_SECONDS}s...")
            time.sleep(CLINIC_DELAY_SECONDS)
        try:
            result = process_clinic_optimized(clinic, last_syncs, sent_appointments, dry_run, force_deep_sync)
            clinic_results.append(result)
            total_sent += result['sent']
            total_skipped += result['skipped']
            total_processed += result['processed']
            if result['processed'] > 0:
                if result['latest_timestamp'] and validate_sync_time_update(
                    clinic, last_syncs.get(clinic), result['latest_timestamp']
                ):
                    new_syncs[clinic] = result['latest_timestamp']
                    state_changed = True
                if not dry_run:
                    for apt_id in result['appointments_sent']:
                        sent_appointments.add(apt_id)
                        state_changed = True
            if 'error' in result:
                logger.error(f"Clinic {clinic} error: {result['error']}")
            else:
                logger.info(f"✅ Clinic {clinic} completed")
        except Exception as e:
            logger.error(f"❌ Failed to process clinic {clinic}: {e}")
    if state_changed and not dry_run:
        try:
            save_state_atomically(new_syncs, sent_appointments)
            logger.info("💾 State saved successfully")
        except Exception as e:
            logger.error(f"❌ Failed to save state: {e}")
    safety_warnings = run_safety_checks(clinic_results)
    if safety_warnings:
        logger.warning("⚠️ SAFETY WARNINGS:")
        for warning in safety_warnings:
            logger.warning(f"  • {warning}")
    logger.info("=== 📊 PERFORMANCE SUMMARY ===")
    logger.info(f"Total sent: {total_sent}")
    logger.info(f"Total skipped: {total_skipped}")
    logger.info(f"Total processed: {total_processed}")
    logger.info(f"Tracked appointments: {len(sent_appointments)}")
    logger.info(f"Current delay: {rate_limiter.current_delay:.2f}s")
    logger.info(f"=== 🏥 CLINIC SUMMARY ===")
    for result in clinic_results:
        clinic = result['clinic']
        status = "✅ SUCCESS" if 'error' not in result else "❌ ERROR"
        logger.info(f"Clinic {clinic}: {status}")
        logger.info(f"  📤 Sent: {result['sent']}")
        logger.info(f"  ⏭️ Skipped: {result['skipped']}")
        logger.info(f"  🔄 Processed: {result['processed']}")
    logger.info("=== 🛡️ SAFETY RECOMMENDATIONS ===")
    logger.info("1. Run --force-deep-sync daily")
    logger.info("2. Monitor appointment_coverage_report.json")
    logger.info("3. Set SAFETY_OVERLAP_HOURS=4 for extra safety")
    logger.info("4. Compare counts with OpenDental directly")
    logger.info("=== 🎉 SYNC COMPLETE ===")

# === AUDIT FUNCTION ===
def run_appointment_audit(clinic_num: int = None, hours_back: int = 24):
    logger.info("=== 🔍 APPOINTMENT AUDIT ===")
    if not validate_configuration():
        sys.exit(1)
    clinics_to_audit = [clinic_num] if clinic_num else CLINIC_NUMS
    for clinic in clinics_to_audit:
        logger.info(f"Auditing clinic {clinic}")
        try:
            now_utc = datetime.datetime.utcnow().replace(tzinfo=timezone.utc)
            start_time = now_utc - datetime.timedelta(hours=hours_back)
            windows = [SyncWindow(
                start_time=start_time,
                end_time=now_utc,
                is_incremental=False,
                clinic_num=clinic
            )]
            appointment_filter = AppointmentFilter(
                clinic_num=clinic,
                operatory_nums=CLINIC_OPERATORY_FILTERS.get(clinic, []),
                valid_statuses=VALID_STATUSES,
                broken_appointment_types=CLINIC_BROKEN_APPOINTMENT_TYPE_FILTERS.get(clinic, []),
                exclude_ghl_tagged=True
            )
            appointments = []
            for window in windows:
                window_appointments = fetch_appointments_for_window(window, appointment_filter)
                if window_appointments:
                    filtered_appointments = apply_appointment_filters(window_appointments, appointment_filter)
                    appointments.extend(filtered_appointments)
            verification_report = verify_appointment_coverage(clinic, appointments, start_time, now_utc)
            logger.info(f"Clinic {clinic}: Audit complete, {len(appointments)} appointments found")
        except Exception as e:
            logger.error(f"Failed to audit clinic {clinic}: {e}")

# === MAIN ===
if __name__ == "__main__":
    import argparse
    parser = argparse.ArgumentParser(description="OpenDental to Keragon Sync")
    parser.add_argument('--dry-run', action='store_true', help="Run without sending to Keragon or saving state")
    parser.add_argument('--force-deep-sync', action='store_true', help="Force a deep sync")
    parser.add_argument('--verbose', action='store_true', help="Enable verbose logging")
    parser.add_argument('--audit', action='store_true', help="Run appointment audit")
    parser.add_argument('--audit-clinic', type=int, help="Clinic number for audit")
    parser.add_argument('--audit-hours', type=int, default=24, help="Hours back for audit")
    args = parser.parse_args()
    if args.verbose:
        logger.setLevel(logging.DEBUG)
    if args.audit:
        run_appointment_audit(args.audit_clinic, args.audit_hours)
    else:
        run_sync(dry_run=args.dry_run, force_deep_sync=args.force_deep_sync)
