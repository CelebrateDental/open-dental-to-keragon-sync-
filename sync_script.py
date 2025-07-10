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
from concurrent.futures import ThreadPoolExecutor, as_completed
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
# These settings are specifically designed to minimize database load
BATCH_SIZE = int(os.environ.get('BATCH_SIZE', '1'))  # Process 1 clinic at a time
TIME_WINDOW_HOURS = int(os.environ.get('TIME_WINDOW_HOURS', '4'))  # Micro windows: 4 hours instead of 24
CLINIC_DELAY_SECONDS = float(os.environ.get('CLINIC_DELAY_SECONDS', '5.0'))  # 5 second delays between requests
MAX_CONCURRENT_CLINICS = int(os.environ.get('MAX_CONCURRENT_CLINICS', '1'))  # No parallel processing
REQUEST_TIMEOUT = int(os.environ.get('REQUEST_TIMEOUT', '120'))  # 2 minute timeout for slow queries
RETRY_ATTEMPTS = int(os.environ.get('RETRY_ATTEMPTS', '5'))  # More retries for stability
BACKOFF_FACTOR = float(os.environ.get('BACKOFF_FACTOR', '3.0'))  # Exponential backoff

# === SMART SYNC OPTIMIZATION ===
# Instead of 720 hour lookahead, we use intelligent sync strategies
INCREMENTAL_SYNC_MINUTES = int(os.environ.get('INCREMENTAL_SYNC_MINUTES', '15'))  # Only 15 minutes for frequent runs
DEEP_SYNC_HOURS = int(os.environ.get('DEEP_SYNC_HOURS', '24'))  # Only 24 hours for first run (was 720!)
SAFETY_OVERLAP_HOURS = int(os.environ.get('SAFETY_OVERLAP_HOURS', '2'))  # Safety overlap to prevent missed appointments

# === CACHING AND OPTIMIZATION ===
ENABLE_CACHING = os.environ.get('ENABLE_CACHING', 'true').lower() == 'true'
CACHE_EXPIRY_MINUTES = int(os.environ.get('CACHE_EXPIRY_MINUTES', '5'))
USE_SPECIFIC_FIELDS = os.environ.get('USE_SPECIFIC_FIELDS', 'true').lower() == 'true'
ENABLE_PAGINATION = os.environ.get('ENABLE_PAGINATION', 'true').lower() == 'true'
PAGE_SIZE = int(os.environ.get('PAGE_SIZE', '50'))  # Smaller page sizes
MAX_RECORDS_PER_REQUEST = int(os.environ.get('MAX_RECORDS_PER_REQUEST', '100'))

CLINIC_NUMS = [int(x) for x in os.environ.get('CLINIC_NUMS', '').split(',') if x.strip().isdigit()]

# America/Chicago timezone for GHL (GMT-6 CST / GMT-5 CDT)
GHL_TIMEZONE = ZoneInfo('America/Chicago')

CLINIC_OPERATORY_FILTERS: Dict[int, List[int]] = {
    9034: [11579, 11580, 11588],
    9035: [11574, 11576, 11577],
}

# Clinic-specific broken appointment type filters
CLINIC_BROKEN_APPOINTMENT_TYPE_FILTERS: Dict[int, List[str]] = {
    9034: ["COMP EX", "COMP EX CHILD"],
    9035: ["CASH CONSULT", "INSURANCE CONSULT"]
}

VALID_STATUSES = {'Scheduled', 'Complete', 'Broken'}

# Only request essential fields to reduce data transfer
REQUIRED_APPOINTMENT_FIELDS = [
    'AptNum', 'AptDateTime', 'AptStatus', 'PatNum', 'Op', 'OperatoryNum',
    'Pattern', 'AppointmentTypeNum', 'Note', 'DateTStamp', 'FName', 'LName'
]

# Global cache for appointment types per clinic
_appointment_types_cache: Dict[int, Dict[int, str]] = {}

# Global session with connection pooling and retry strategy
_session = None
_session_lock = threading.Lock()

# Request fingerprinting for caching
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
    """Represents a time window for syncing"""
    start_time: datetime.datetime
    end_time: datetime.datetime
    is_incremental: bool = False
    clinic_num: Optional[int] = None

@dataclass
class AppointmentFilter:
    """Encapsulates all filtering criteria"""
    clinic_num: int
    operatory_nums: List[int]
    valid_statuses: Set[str]
    broken_appointment_types: List[str]
    exclude_ghl_tagged: bool = True

# === OPTIMIZED SESSION MANAGEMENT ===
def get_optimized_session():
    """Get a session with aggressive connection pooling and retry strategy"""
    global _session
    if _session is None:
        with _session_lock:
            if _session is None:
                _session = requests.Session()
                
                # Configure aggressive retry strategy
                retry_strategy = Retry(
                    total=RETRY_ATTEMPTS,
                    backoff_factor=BACKOFF_FACTOR,
                    status_forcelist=[408, 429, 500, 502, 503, 504, 520, 521, 522, 523, 524],
                    allowed_methods=["HEAD", "GET", "POST"],
                    raise_on_status=False
                )
                
                # Configure connection pooling optimized for database performance
                adapter = HTTPAdapter(
                    max_retries=retry_strategy,
                    pool_connections=5,  # Reduced to minimize DB connections
                    pool_maxsize=10,     # Reduced pool size
                    pool_block=True
                )
                
                _session.mount("http://", adapter)
                _session.mount("https://", adapter)
                
                # Set persistent headers
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
    """Adaptive rate limiter that adjusts based on response times"""
    
    def __init__(self, base_delay_seconds: float):
        self.base_delay = base_delay_seconds
        self.current_delay = base_delay_seconds
        self.last_request_time = 0
        self.response_times = []
        self.lock = threading.Lock()
        self.consecutive_slow_requests = 0
        self.max_delay = base_delay_seconds * 10  # Cap at 10x base delay
    
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
        """Record response time and adapt delay accordingly"""
        with self.lock:
            self.response_times.append(response_time)
            if len(self.response_times) > 10:
                self.response_times.pop(0)
            
            # If response time is slow (>10s), increase delay
            if response_time > 10:
                self.consecutive_slow_requests += 1
                if self.consecutive_slow_requests >= 2:
                    self.current_delay = min(self.current_delay * 1.5, self.max_delay)
                    logger.warning(f"Database slow response ({response_time:.2f}s), increasing delay to {self.current_delay:.2f}s")
            else:
                self.consecutive_slow_requests = 0
                # Gradually reduce delay if responses are fast
                if response_time < 3 and self.current_delay > self.base_delay:
                    self.current_delay = max(self.current_delay * 0.9, self.base_delay)
                    logger.debug(f"Fast response, reducing delay to {self.current_delay:.2f}s")

# Global adaptive rate limiter
rate_limiter = AdaptiveRateLimiter(CLINIC_DELAY_SECONDS)

# === REQUEST CACHING ===
def get_request_fingerprint(url: str, params: Dict[str, Any]) -> str:
    """Generate a unique fingerprint for a request"""
    content = f"{url}:{json.dumps(params, sort_keys=True)}"
    return hashlib.md5(content.encode()).hexdigest()

def get_cached_response(fingerprint: str) -> Optional[Any]:
    """Get cached response if still valid"""
    if not ENABLE_CACHING:
        return None
    
    with _cache_lock:
        if fingerprint in _request_cache:
            timestamp, data = _request_cache[fingerprint]
            if datetime.datetime.now() - timestamp < datetime.timedelta(minutes=CACHE_EXPIRY_MINUTES):
                logger.debug(f"Cache hit for request {fingerprint[:8]}...")
                return data
            else:
                # Remove expired cache entry
                del _request_cache[fingerprint]
    
    return None

def cache_response(fingerprint: str, data: Any):
    """Cache a response"""
    if not ENABLE_CACHING:
        return
    
    with _cache_lock:
        _request_cache[fingerprint] = (datetime.datetime.now(), data)
        
        # Clean up old cache entries if cache is getting large
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
) -> Optional[Any]:
    """Make an optimized API request with caching and performance monitoring"""
    
    # Generate request fingerprint for caching
    fingerprint = get_request_fingerprint(endpoint, params)
    
    # Check cache first
    if use_cache and method == 'GET':
        cached_result = get_cached_response(fingerprint)
        if cached_result is not None:
            return cached_result
    
    # Rate limit the request
    rate_limiter.wait_if_needed()
    
    # Make the request
    start_time = time.time()
    try:
        session = get_optimized_session()
        url = f"{API_BASE_URL}/{endpoint.lstrip('/')}"
        
        # Add field selection if supported to reduce data transfer
        if USE_SPECIFIC_FIELDS and 'appointments' in endpoint:
            params['fields'] = ','.join(REQUIRED_APPOINTMENT_FIELDS)
        
        # Add pagination if enabled
        if ENABLE_PAGINATION and 'limit' not in params:
            params['limit'] = PAGE_SIZE
        
        logger.debug(f"Making request to {endpoint} with params: {params}")
        
        if method == 'GET':
            response = session.get(url, headers=make_auth_header(), params=params, timeout=REQUEST_TIMEOUT)
        else:
            response = session.post(url, headers=make_auth_header(), json=params, timeout=REQUEST_TIMEOUT)
        
        response_time = time.time() - start_time
        rate_limiter.record_response_time(response_time)
        
        if response.status_code == 429:
            logger.warning(f"Rate limited by API, waiting extra time...")
            time.sleep(10)
            return make_optimized_request(endpoint, params, method, use_cache=False)
        
        response.raise_for_status()
        data = response.json()
        
        # Cache successful GET requests
        if method == 'GET' and use_cache:
            cache_response(fingerprint, data)
        
        logger.debug(f"Request completed in {response_time:.2f}s")
        return data
        
    except requests.exceptions.Timeout:
        logger.error(f"Request to {endpoint} timed out after {REQUEST_TIMEOUT}s")
        return None
    except requests.exceptions.RequestException as e:
        logger.error(f"Request to {endpoint} failed: {e}")
        return None
    except Exception as e:
        logger.error(f"Unexpected error in request to {endpoint}: {e}")
        return None

# === APPOINTMENT TYPES CACHE ===
def get_appointment_types(clinic_num: int) -> Dict[int, str]:
    """Fetch and cache appointment types mapping for a specific clinic"""
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
        
        # Handle both single object and list responses
        appt_types = data if isinstance(data, list) else [data]
        
        # Create mapping from AppointmentTypeNum to AppointmentTypeName
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
    """Get the appointment type name for an appointment in a specific clinic"""
    apt_type_num = appointment.get('AppointmentTypeNum')
    if apt_type_num is not None:
        appointment_types = get_appointment_types(clinic_num)
        apt_type_name = appointment_types.get(apt_type_num, '')
        if apt_type_name:
            return apt_type_name
    
    # Fallback to direct fields
    apt_type_direct = appointment.get('AptType', '') or appointment.get('AppointmentType', '')
    if apt_type_direct:
        return apt_type_direct
    
    return ''

def get_appointment_type_num(appointment: Dict[str, Any]) -> Optional[int]:
    """Get the appointment type number for an appointment"""
    return appointment.get('AppointmentTypeNum')

def has_ghl_tag(appointment: Dict[str, Any]) -> bool:
    """Check if appointment has [fromGHL] tag in the Note field"""
    note = appointment.get('Note', '') or ''
    return '[fromGHL]' in note

# === STATE MANAGEMENT ===
def load_last_sync_times() -> Dict[int, Optional[datetime.datetime]]:
    state: Dict[int, Optional[datetime.datetime]] = {}
    if os.path.exists(STATE_FILE):
        try:
            with open(STATE_FILE) as f:
                data = json.load(f)
            for c in CLINIC_NUMS:
                ts = data.get(str(c))
                if ts:
                    dt = datetime.datetime.fromisoformat(ts)
                    if dt.tzinfo is None:
                        dt = dt.replace(tzinfo=timezone.utc)
                    state[c] = dt
                else:
                    state[c] = None
        except Exception as e:
            logger.error(f"Error reading state file: {e}")
            state = {c: None for c in CLINIC_NUMS}
    else:
        state = {c: None for c in CLINIC_NUMS}
    return state

def validate_sync_time_update(clinic: int, old_time: Optional[datetime.datetime], 
                             new_time: Optional[datetime.datetime]) -> bool:
    """Ensure we don't accidentally move sync time backwards"""
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
    """Save both state files atomically to avoid corruption"""
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
    """Load set of already sent appointment IDs"""
    if os.path.exists(SENT_APPTS_FILE):
        try:
            with open(SENT_APPTS_FILE) as f:
                data = json.load(f)
                return set(str(x) for x in data)
        except Exception as e:
            logger.error(f"Error reading sent appointments file: {e}")
    return set()

# === UTILITIES ===
def parse_time(s: Optional[str]) -> Optional[datetime.datetime]:
    if not s:
        return None
    try:
        if s.endswith('Z'):
            s = s[:-1] + '+00:00'
        dt = datetime.datetime.fromisoformat(s)
        if dt.tzinfo is None:
            dt = dt.replace(tzinfo=timezone.utc)
        return dt
    except ValueError:
        logger.warning(f"Unrecognized time format: {s}")
        return None

def make_auth_header() -> Dict[str, str]:
    return {'Authorization': f'ODFHIR {DEVELOPER_KEY}/{CUSTOMER_KEY}', 'Content-Type': 'application/json'}

# === DURATION CALCULATION ===
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
    """Generate optimized sync windows with safety overlaps - NO MORE 720 HOURS!"""
    
    now_utc = datetime.datetime.utcnow().replace(tzinfo=timezone.utc)
    windows = []
    
    # Determine sync strategy
    if last_sync and not force_deep_sync:
        time_since_last = now_utc - last_sync
        
        # If last sync was recent (< 1 hour), do incremental sync
        if time_since_last < datetime.timedelta(hours=1):
            logger.info(f"Clinic {clinic_num}: Incremental sync (last sync {time_since_last} ago)")
            
            # Safety overlap to catch any missed appointments
            window_start = last_sync - datetime.timedelta(hours=SAFETY_OVERLAP_HOURS)
            window_end = now_utc + datetime.timedelta(hours=4)
            
            # Create dual windows for safety:
            # 1. Incremental sync using DateTStamp
            windows.append(SyncWindow(
                start_time=window_start,
                end_time=window_end,
                is_incremental=True,
                clinic_num=clinic_num
            ))
            
            # 2. Full appointment date window (catches appointments that don't update DateTStamp)
            appt_window_start = last_sync - datetime.timedelta(minutes=30)
            appt_window_end = now_utc + datetime.timedelta(hours=24)
            
            windows.append(SyncWindow(
                start_time=appt_window_start,
                end_time=appt_window_end,
                is_incremental=False,
                clinic_num=clinic_num
            ))
            
        else:
            logger.info(f"Clinic {clinic_num}: Deep sync needed (last sync {time_since_last} ago)")
            force_deep_sync = True
    
    # Deep sync - ONLY 24 hours instead of 720!
    if not last_sync or force_deep_sync:
        logger.info(f"Clinic {clinic_num}: Deep sync with {DEEP_SYNC_HOURS}-hour window")
        
        # Start from only 24 hours ago (not 720!)
        if last_sync:
            start_time = max(
                last_sync - datetime.timedelta(hours=SAFETY_OVERLAP_HOURS), 
                now_utc - datetime.timedelta(hours=DEEP_SYNC_HOURS)
            )
        else:
            start_time = now_utc - datetime.timedelta(hours=DEEP_SYNC_HOURS)
        
        # Look ahead 24 hours for upcoming appointments
        end_time = now_utc + datetime.timedelta(hours=24)
        
        # Break into micro windows to reduce database load
        current_start = start_time
        while current_start < end_time:
            window_end = min(current_start + datetime.timedelta(hours=TIME_WINDOW_HOURS), end_time)
            
            windows.append(SyncWindow(
                start_time=current_start,
                end_time=window_end,
                is_incremental=False,
                clinic_num=clinic_num
            ))
            
            current_start = window_end
    
    logger.info(f"Clinic {clinic_num}: Generated {len(windows)} sync windows")
    return windows

# === OPTIMIZED APPOINTMENT FETCHING ===
def fetch_appointments_for_window(
    window: SyncWindow,
    appointment_filter: AppointmentFilter,
    since: Optional[datetime.datetime] = None
) -> List[Dict[str, Any]]:
    """Fetch appointments for a specific window with database optimization"""
    
    clinic = appointment_filter.clinic_num
    
    # Build highly specific parameters to minimize database scanning
    params = {
        'ClinicNum': clinic,
        'dateStart': window.start_time.strftime('%Y-%m-%d'),
        'dateEnd': window.end_time.strftime('%Y-%m-%d'),
    }
    
    # Add filters at API level to reduce database load
    if appointment_filter.operatory_nums:
        params['Op'] = ','.join(map(str, appointment_filter.operatory_nums))
    
    if appointment_filter.valid_statuses:
        params['AptStatus'] = ','.join(appointment_filter.valid_statuses)
    
    # Only add timestamp filter for incremental windows
    if since and window.is_incremental:
        if since.tzinfo is None:
            since = since.replace(tzinfo=timezone.utc)
        eff_utc = (since + datetime.timedelta(seconds=1)).astimezone(timezone.utc)
        params['DateTStamp'] = eff_utc.strftime('%Y-%m-%d %H:%M:%S')
        logger.debug(f"Clinic {clinic}: Incremental sync from {params['DateTStamp']}")
    
    # Limit records to reduce database load
    params['limit'] = min(MAX_RECORDS_PER_REQUEST, PAGE_SIZE)
    
    logger.debug(f"Clinic {clinic}: Fetching {window.start_time} to {window.end_time}")
    
    try:
        data = make_optimized_request('appointments', params)
        
        if data is None:
            return []
        
        appts = data if isinstance(data, list) else [data]
        
        if window.is_incremental:
            logger.debug(f"Clinic {clinic}: Incremental window returned {len(appts)} appointments")
        else:
            logger.debug(f"Clinic {clinic}: Full window returned {len(appts)} appointments")
        
        return appts
        
    except Exception as e:
        logger.error(f"Error fetching appointments for clinic {clinic}: {e}")
        return []

def apply_appointment_filters(
    appointments: List[Dict[str, Any]],
    appointment_filter: AppointmentFilter
) -> List[Dict[str, Any]]:
    """Apply client-side filtering to appointments"""
    
    clinic = appointment_filter.clinic_num
    valid = []
    
    for appt in appointments:
        apt_num = str(appt.get('AptNum', ''))
        status = appt.get('AptStatus', '')
        op_num = appt.get('Op') or appt.get('OperatoryNum')
        
        # Skip GHL tagged appointments
        if appointment_filter.exclude_ghl_tagged and has_ghl_tag(appt):
            continue
        
        # Check status
        if status not in appointment_filter.valid_statuses:
            continue
        
        # Check operatory filter
        if appointment_filter.operatory_nums and op_num not in appointment_filter.operatory_nums:
            continue
        
        # Check broken appointment type filter
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
    """Fetch appointments using all optimization strategies"""
    
    logger.info(f"Clinic {clinic}: Starting optimized fetch")
    
    # Create appointment filter
    appointment_filter = AppointmentFilter(
        clinic_num=clinic,
        operatory_nums=CLINIC_OPERATORY_FILTERS.get(clinic, []),
        valid_statuses=VALID_STATUSES,
        broken_appointment_types=CLINIC_BROKEN_APPOINTMENT_TYPE_FILTERS.get(clinic, []),
        exclude_ghl_tagged=True
    )
    
    # Generate sync windows
    windows = generate_sync_windows(clinic, since, force_deep_sync)
    
    all_appointments = []
    
    # Process each window sequentially (no parallel processing to reduce DB load)
    for i, window in enumerate(windows):
        window_type = 'incremental' if window.is_incremental else 'full'
        logger.info(f"Clinic {clinic}: Processing window {i+1}/{len(windows)} ({window_type})")
        
        try:
            window_appointments = fetch_appointments_for_window(window, appointment_filter, since)
            
            if window_appointments:
                filtered_appointments = apply_appointment_filters(window_appointments, appointment_filter)
                all_appointments.extend(filtered_appointments)
                
                logger.info(f"Clinic {clinic}: Window {i+1} - {len(window_appointments)} raw → {len(filtered_appointments)} filtered")
            else:
                logger.info(f"Clinic {clinic}: Window {i+1} - No appointments found")
            
            # Add delay between windows to reduce database pressure
            if i < len(windows) - 1:
                time.sleep(2)
                
        except Exception as e:
            logger.error(f"Error processing window {i+1} for clinic {clinic}: {e}")
            continue
    
    # Deduplicate appointments (in case of overlapping windows)
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
    """Verify we haven't missed any appointments"""
    
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
    
    # Analyze appointment distribution
    appointment_times = []
    for appt in appointments:
        apt_time = parse_time(appt.get('AptDateTime'))
        if apt_time:
            appointment_times.append(apt_time)
    
    if not appointment_times:
        verification_report['coverage_gaps'].append("No appointments with valid times")
        return verification_report
    
    appointment_times.sort()
    
    # Check for large gaps
    for i in range(1, len(appointment_times)):
        gap_hours = (appointment_times[i] - appointment_times[i-1]).total_seconds() / 3600
        if gap_hours > 24:
            verification_report['coverage_gaps'].append(
                f"Large gap: {gap_hours:.1f} hours between appointments"
            )
    
    # Check if we're missing recent appointments
    now_utc = datetime.datetime.utcnow().replace(tzinfo=timezone.utc)
    latest_appt = max(appointment_times)
    hours_since_latest = (now_utc - latest_appt).total_seconds() / 3600
    
    if hours_since_latest > 48:
        verification_report['coverage_gaps'].append(
            f"No appointments in last {hours_since_latest:.1f} hours"
        )
        verification_report['recommendations'].append("Consider --force-deep-sync")
    
    # Status distribution
    status_counts = {}
    for appt in appointments:
        status = appt.get('AptStatus', 'Unknown')
        status_counts[status] = status_counts.get(status, 0) + 1
    
    verification_report['appointment_status_distribution'] = status_counts
    
    # Log report
    logger.info(f"📊 Coverage Report - Clinic {clinic}: {len(appointments)} appointments")
    if verification_report['coverage_gaps']:
        logger.warning(f"⚠️  Coverage gaps detected for clinic {clinic}")
    
    return verification_report

def save_verification_report(clinic_reports: List[Dict[str, Any]]):
    """Save verification reports to file"""
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
    """Get patient details with caching"""
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
    """Send appointment to Keragon with enhanced error handling"""
    apt_num = str(appt.get('AptNum', ''))
    
    try:
        # Get patient details
        patient = get_patient_details(appt.get('PatNum'))
        first = patient.get('FName') or appt.get('FName', '')
        last = patient.get('LName') or appt.get('LName', '')
        name = f"{first} {last}".strip() or 'Unknown'
        provider_name = 'Dr. Gharbi' if clinic == 9034 else 'Dr. Ensley'

        # Get appointment type number
        apt_type_num = get_appointment_type_num(appt)

        # Parse time from OpenDental
        st_utc = parse_time(appt.get('AptDateTime'))
        if not st_utc:
            logger.error(f"Invalid start time for appointment {apt_num}")
            return False
        
        # Convert to GHL timezone while preserving time values
        st_ghl = st_utc.replace(tzinfo=None)
        st_ghl = st_ghl.replace(tzinfo=GHL_TIMEZONE)
        
        # Calculate duration and end time
        duration_minutes = calculate_pattern_duration(appt.get('Pattern', ''))
        if duration_minutes <= 0:
            duration_minutes = 60
        
        en_ghl = st_ghl + datetime.timedelta(minutes=duration_minutes)

        # Format for payload
        st_payload = st_ghl.isoformat(timespec='seconds')
        en_payload = en_ghl.isoformat(timespec='seconds')

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

        if dry_run:
            logger.info(f"DRY RUN: Would send appointment {apt_num}")
            return True

        # Send with retry logic
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
                else:
                    logger.warning(f"Retry {attempt + 1} for appointment {apt_num}")
                    time.sleep(2 ** attempt)
        
        return False
        
    except Exception as e:
        logger.error(f"✗ Error processing appointment {apt_num}: {e}")
        return False

def send_batch_to_keragon(appointments: List[Dict[str, Any]], clinic: int, dry_run: bool = False) -> Tuple[int, List[str]]:
    """Send batch of appointments with optimized processing"""
    
    if not appointments:
        return 0, []
    
    successful_sends = 0
    sent_appointment_ids = []
    
    # Process in smaller batches
    batch_size = 5
    for i in range(0, len(appointments), batch_size):
        batch = appointments[i:i + batch_size]
        
        logger.info(f"Clinic {clinic}: Processing batch {i//batch_size + 1} ({len(batch)} appointments)")
        
        for appt in batch:
            if send_to_keragon(appt, clinic, dry_run):
                successful_sends += 1
                sent_appointment_ids.append(str(appt.get('AptNum', '')))
            
            time.sleep(0.5)  # Small delay between sends
        
        # Delay between batches
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
    """Process a single clinic with all optimizations"""
    
    logger.info(f"=== Processing Clinic {clinic} ===")
    
    try:
        # Pre-load appointment types
        appointment_types = get_appointment_types(clinic)
        logger.info(f"Clinic {clinic}: Loaded {len(appointment_types)} appointment types")
        
        # Fetch appointments
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
        
        # Filter out already sent appointments
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
        
        # Send appointments
        sent_count, sent_ids = send_batch_to_keragon(new_appointments, clinic, dry_run)
        
        # Calculate latest timestamp
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
    """Run safety checks after sync"""
    
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
    """Main sync function with database optimizations"""
    
    if not (DEVELOPER_KEY and CUSTOMER_KEY and KERAGON_WEBHOOK_URL and CLINIC_NUMS):
        logger.critical("Missing configuration")
        sys.exit(1)

    logger.info("=== ULTRA-OPTIMIZED OpenDental → Keragon Sync ===")
    logger.info("🚀 DATABASE PERFORMANCE OPTIMIZATIONS:")
    logger.info(f"  • Sequential processing (no parallel DB hits)")
    logger.info(f"  • Micro time windows: {TIME_WINDOW_HOURS} hours")
    logger.info(f"  • Adaptive rate limiting: {CLINIC_DELAY_SECONDS}s base delay")
    logger.info(f"  • Deep sync window: {DEEP_SYNC_HOURS} hours (was 720!)")
    logger.info(f"  • Request caching: {'enabled' if ENABLE_CACHING else 'disabled'}")
    logger.info(f"  • Field selection: {'enabled' if USE_SPECIFIC_FIELDS else 'disabled'}")
    logger.info(f"  • Safety overlap: {SAFETY_OVERLAP_HOURS} hours")
    
    if dry_run:
        logger.info("🔍 DRY RUN MODE")
    if force_deep_sync:
        logger.info("🔄 FORCE DEEP SYNC")

    # Load state
    last_syncs = load_last_sync_times()
    sent_appointments = load_sent_appointments()
    new_syncs = last_syncs.copy()
    
    total_sent = 0
    total_skipped = 0
    total_processed = 0
    state_changed = False
    verification_reports = []

    # Process clinics SEQUENTIALLY
    logger.info(f"📊 Processing {len(CLINIC_NUMS)} clinics SEQUENTIALLY")
    
    clinic_results = []
    
    for i, clinic in enumerate(CLINIC_NUMS):
        logger.info(f"🏥 Processing clinic {clinic} ({i+1}/{len(CLINIC_NUMS)})")
        
        # Delay between clinics
        if i > 0:
            logger.info(f"⏱️  Waiting {CLINIC_DELAY_SECONDS}s...")
            time.sleep(CLINIC_DELAY_SECONDS)
        
        try:
            result = process_clinic_optimized(clinic, last_syncs, sent_appointments, dry_run, force_deep_sync)
            clinic_results.append(result)
            
            # Update tracking
            total_sent += result['sent']
            total_skipped += result['skipped']
            total_processed += result['processed']
            
            # Update state
            if result['processed'] > 0:
                if result['latest_timestamp'] and validate_sync_time_update(
                    clinic, last_syncs.get(clinic), result['latest_timestamp']
                ):
                    new_syncs[clinic] = result['latest_timestamp']
                    state_changed = True
                
                # Add sent appointments
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

    # Save state
    if state_changed and not dry_run:
        try:
            save_state_atomically(new_syncs, sent_appointments)
            logger.info("💾 State saved successfully")
        except Exception as e:
            logger.error(f"❌ Failed to save state: {e}")
    
    # Run safety checks
    safety_warnings = run_safety_checks(clinic_results)
    
    if safety_warnings:
        logger.warning("⚠️  SAFETY WARNINGS:")
        for warning in safety_warnings:
            logger.warning(f"  • {warning}")
    
    # Summary
    logger.info("=== 📊 PERFORMANCE SUMMARY ===")
    logger.info(f"Total sent: {total_sent}")
    logger.info(f"Total skipped: {total_skipped}")
    logger.info(f"Total processed: {total_processed}")
    logger.info(f"Tracked appointments: {len(sent_appointments)}")
    logger.info(f"Current delay: {rate_limiter.current_delay:.2f}s")
    
    # Per-clinic summary
    logger.info("=== 🏥 CLINIC SUMMARY ===")
    for result in clinic_results:
        clinic = result['clinic']
        status = "✅ SUCCESS" if 'error' not in result else "❌ ERROR"
        logger.info(f"Clinic {clinic}: {status}")
        logger.info(f"  📤 Sent: {result['sent']}")
        logger.info(f"  ⏭️  Skipped: {result['skipped']}")
        logger.info(f"  🔄 Processed: {result['processed']}")
    
    # Recommendations
    logger.info("=== 🛡️  SAFETY RECOMMENDATIONS ===")
    logger.info("1. Run --force-deep-sync daily")
    logger.info("2. Monitor appointment_coverage_report.json")
    logger.info("3. Set SAFETY_OVERLAP_HOURS=4 for extra safety")
    logger.info("4. Compare counts with OpenDental directly")
    
    logger.info("=== 🎉 SYNC COMPLETE ===")

# === AUDIT FUNCTION ===
def run_appointment_audit(clinic_num: int = None, hours_back: int = 24):
    """Run comprehensive audit to verify no appointments are missed"""
    
    logger.info("=== 🔍 APPOINTMENT AUDIT ===")
    
    if clinic_num:
        clinics_to_audit = [clinic_num]
    else:
        clinics_to_audit = CLINIC_NUMS
    
    for clinic in clinics_to_audit:
        logger.info(f"🏥 Auditing Clinic {clinic}...")
        
        now_utc = datetime.datetime.utcnow().replace(tzinfo=timezone.utc)
        audit_start = now_utc - datetime.timedelta(hours=hours_back)
        
        # Create audit window
        audit_window = SyncWindow(
            start_time=audit_start,
            end_time=now_utc + datetime.timedelta(hours=4),
            is_incremental=False,
            clinic_num=clinic
        )
        
        # Create permissive filter
        audit_filter = AppointmentFilter(
            clinic_num=clinic,
            operatory_nums=CLINIC_OPERATORY_FILTERS.get(clinic, []),
            valid_statuses=VALID_STATUSES,
            broken_appointment_types=CLINIC_BROKEN_APPOINTMENT_TYPE_FILTERS.get(clinic, []),
            exclude_ghl_tagged=False  # Include all appointments
        )
        
        try:
            all_appointments = fetch_appointments_for_window(audit_window, audit_filter)
            
            logger.info(f"📊 Audit Results - Clinic {clinic}:")
            logger.info(f"  Total appointments: {len(all_appointments)}")
            
            if all_appointments:
                # Categorize appointments
                categories = {
                    'with_ghl_tag': 0,
                    'without_ghl_tag': 0,
                    'in_operatory_filter': 0,
                    'outside_operatory_filter': 0
                }
                
                operatory_filter = CLINIC_OPERATORY_FILTERS.get(clinic, [])
                
                for appt in all_appointments:
                    if has_ghl_tag(appt):
                        categories['with_ghl_tag'] += 1
                    else:
                        categories['without_ghl_tag'] += 1
                    
                    op_num = appt.get('Op') or appt.get('OperatoryNum')
                    if operatory_filter and op_num in operatory_filter:
                        categories['in_operatory_filter'] += 1
                    else:
                        categories['outside_operatory_filter'] += 1
                
                logger.info(f"  📋 Breakdown:")
                logger.info(f"    With [fromGHL]: {categories['with_ghl_tag']}")
                logger.info(f"    Without [fromGHL]: {categories['without_ghl_tag']}")
                logger.info(f"    In operatory filter: {categories['in_operatory_filter']}")
                logger.info(f"    Outside filter: {categories['outside_operatory_filter']}")
                
                # Check sent appointments
                sent_appointments = load_sent_appointments()
                already_sent = sum(1 for appt in all_appointments 
                                 if str(appt.get('AptNum', '')) in sent_appointments)
                
                logger.info(f"  ✅ Already sent: {already_sent}")
                
        except Exception as e:
            logger.error(f"❌ Audit failed for clinic {clinic}: {e}")

# === UTILITY FUNCTIONS ===
def validate_configuration():
    """Validate configuration"""
    issues = []
    
    if not DEVELOPER_KEY:
        issues.append("OPEN_DENTAL_DEVELOPER_KEY missing")
    if not CUSTOMER_KEY:
        issues.append("OPEN_DENTAL_CUSTOMER_KEY missing")
    if not KERAGON_WEBHOOK_URL:
        issues.append("KERAGON_WEBHOOK_URL missing")
    if not CLINIC_NUMS:
        issues.append("CLINIC_NUMS missing")
    
    if TIME_WINDOW_HOURS > 24:
        issues.append(f"TIME_WINDOW_HOURS ({TIME_WINDOW_HOURS}) too large")
    
    if MAX_CONCURRENT_CLINICS > 1:
        issues.append(f"MAX_CONCURRENT_CLINICS ({MAX_CONCURRENT_CLINICS}) > 1 may overload database")
    
    if issues:
        logger.error("❌ Configuration issues:")
        for issue in issues:
            logger.error(f"  • {issue}")
        return False
    
    return True

def show_performance_stats():
    """Show performance statistics"""
    logger.info("=== 📊 PERFORMANCE STATS ===")
    logger.info(f"Rate limiter delay: {rate_limiter.current_delay:.2f}s")
    logger.info(f"Slow requests: {rate_limiter.consecutive_slow_requests}")
    logger.info(f"Cache entries: {len(_request_cache)}")
    logger.info(f"Appointment types cached: {len(_appointment_types_cache)}")

if __name__ == '__main__':
    import argparse
    
    parser = argparse.ArgumentParser(description='Ultra-Optimized OpenDental → Keragon Sync')
    parser.add_argument('--reset', action='store_true', help='Clear all saved state')
    parser.add_argument('--reset-sent', action='store_true', help='Clear sent appointments')
    parser.add_argument('--reset-cache', action='store_true', help='Clear request cache')
    parser.add_argument('--verbose', action='store_true', help='Enable DEBUG logging')
    parser.add_argument('--dry-run', action='store_true', help='Show what would be sent')
    parser.add_argument('--force-deep-sync', action='store_true', help='Force deep sync')
    parser.add_argument('--show-appt-types', action='store_true', help='Show appointment types')
    parser.add_argument('--show-stats', action='store_true', help='Show performance stats')
    parser.add_argument('--validate-config', action='store_true', help='Validate configuration')
    parser.add_argument('--audit', action='store_true', help='Run appointment audit')
    parser.add_argument('--audit-clinic', type=int, help='Audit specific clinic')
    parser.add_argument('--audit-hours', type=int, default=24, help='Hours back to audit')
    
    args = parser.parse_args()

    if args.verbose:
        logger.setLevel(logging.DEBUG)

    if args.validate_config:
        sys.exit(0 if validate_configuration() else 1)

    if args.show_stats:
        show_performance_stats()
        sys.exit(0)

    if args.reset_cache:
        _request_cache.clear()
        logger.info("🗑️  Cache cleared")
        sys.exit(0)

    if args.audit:
        if not validate_configuration():
            sys.exit(1)
        run_appointment_audit(args.audit_clinic, args.audit_hours)
        sys.exit(0)

    if args.show_appt_types:
        if not validate_configuration():
            sys.exit(1)
        
        print("=== 📋 APPOINTMENT TYPES BY CLINIC ===")
        for clinic in CLINIC_NUMS:
            appt_types = get_appointment_types(clinic)
            print(f"\n🏥 Clinic {clinic}:")
            if appt_types:
                for type_num, type_name in sorted(appt_types.items()):
                    print(f"  {type_num}: {type_name}")
            else:
                print("  No appointment types found")
        sys.exit(0)

    if args.reset:
        files_to_remove = [STATE_FILE, SENT_APPTS_FILE, CACHE_FILE]
        for file_path in files_to_remove:
            if os.path.exists(file_path):
                os.remove(file_path)
        _request_cache.clear()
        logger.info("🗑️  All state cleared")
        sys.exit(0)

    if args.reset_sent:
        if os.path.exists(SENT_APPTS_FILE):
            os.remove(SENT_APPTS_FILE)
        logger.info("🗑️  Sent appointments cleared")
        sys.exit(0)

    # Validate and run
    if not validate_configuration():
        sys.exit(1)

    run_sync(dry_run=args.dry_run, force_deep_sync=args.force_deep_sync)
