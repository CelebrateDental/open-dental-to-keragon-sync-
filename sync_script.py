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
from typing import List, Dict, Any, Optional, Set
from concurrent.futures import ThreadPoolExecutor, as_completed
from urllib3.util.retry import Retry
from requests.adapters import HTTPAdapter

# === CONFIGURATION ===
API_BASE_URL        = os.environ.get('OPEN_DENTAL_API_URL', 'https://api.opendental.com/api/v1')
DEVELOPER_KEY       = os.environ.get('OPEN_DENTAL_DEVELOPER_KEY')
CUSTOMER_KEY        = os.environ.get('OPEN_DENTAL_CUSTOMER_KEY')
KERAGON_WEBHOOK_URL = os.environ.get('KERAGON_WEBHOOK_URL')

STATE_FILE      = 'last_sync_state.json'
SENT_APPTS_FILE = 'sent_appointments.json'
LOG_LEVEL       = os.environ.get('LOG_LEVEL', 'INFO')

# PERFORMANCE OPTIMIZATIONS
BATCH_SIZE = int(os.environ.get('BATCH_SIZE', '3'))  # Process 3 clinics at a time
TIME_WINDOW_HOURS = int(os.environ.get('TIME_WINDOW_HOURS', '24'))  # Process 24 hours at a time instead of 720
CLINIC_DELAY_SECONDS = float(os.environ.get('CLINIC_DELAY_SECONDS', '2.0'))  # Delay between clinic requests
MAX_CONCURRENT_CLINICS = int(os.environ.get('MAX_CONCURRENT_CLINICS', '2'))  # Max parallel clinic processing
REQUEST_TIMEOUT = int(os.environ.get('REQUEST_TIMEOUT', '45'))  # Increased timeout
RETRY_ATTEMPTS = int(os.environ.get('RETRY_ATTEMPTS', '3'))
BACKOFF_FACTOR = float(os.environ.get('BACKOFF_FACTOR', '2.0'))

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

# Global cache for appointment types per clinic
_appointment_types_cache: Dict[int, Dict[int, str]] = {}

# Global session with connection pooling and retry strategy
_session = None
_session_lock = threading.Lock()

# === LOGGING ===
logging.basicConfig(
    level=getattr(logging, LOG_LEVEL),
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[logging.StreamHandler(sys.stdout)]
)
logger = logging.getLogger('opendental_sync')

# === OPTIMIZED SESSION MANAGEMENT ===
def get_optimized_session():
    """Get a session with connection pooling and retry strategy"""
    global _session
    if _session is None:
        with _session_lock:
            if _session is None:
                _session = requests.Session()
                
                # Configure retry strategy
                retry_strategy = Retry(
                    total=RETRY_ATTEMPTS,
                    backoff_factor=BACKOFF_FACTOR,
                    status_forcelist=[429, 500, 502, 503, 504],
                    allowed_methods=["HEAD", "GET", "POST"]
                )
                
                # Configure connection pooling
                adapter = HTTPAdapter(
                    max_retries=retry_strategy,
                    pool_connections=10,
                    pool_maxsize=20,
                    pool_block=True
                )
                
                _session.mount("http://", adapter)
                _session.mount("https://", adapter)
                
                logger.info("Initialized optimized session with connection pooling and retry strategy")
    
    return _session

# === RATE LIMITING ===
class RateLimiter:
    def __init__(self, delay_seconds: float):
        self.delay_seconds = delay_seconds
        self.last_request_time = 0
        self.lock = threading.Lock()
    
    def wait_if_needed(self):
        with self.lock:
            current_time = time.time()
            time_since_last = current_time - self.last_request_time
            
            if time_since_last < self.delay_seconds:
                sleep_time = self.delay_seconds - time_since_last
                logger.debug(f"Rate limiting: sleeping {sleep_time:.2f}s")
                time.sleep(sleep_time)
            
            self.last_request_time = time.time()

# Global rate limiter
rate_limiter = RateLimiter(CLINIC_DELAY_SECONDS)

# === APPOINTMENT TYPES CACHE ===
def get_appointment_types(clinic_num: int) -> Dict[int, str]:
    """Fetch and cache appointment types mapping for a specific clinic"""
    global _appointment_types_cache
    
    if clinic_num in _appointment_types_cache:
        return _appointment_types_cache[clinic_num]
    
    try:
        logger.info(f"Fetching appointment types for clinic {clinic_num} from OpenDental API...")
        
        # Rate limit the request
        rate_limiter.wait_if_needed()
        
        # Try clinic-specific endpoint first
        session = get_optimized_session()
        params = {'ClinicNum': clinic_num}
        r = session.get(f"{API_BASE_URL}/appointmenttypes", 
                       headers=make_auth_header(), 
                       params=params, 
                       timeout=REQUEST_TIMEOUT)
        r.raise_for_status()
        data = r.json()
        
        # Handle both single object and list responses
        appt_types = data if isinstance(data, list) else [data]
        
        # Create mapping from AppointmentTypeNum to AppointmentTypeName for this clinic
        clinic_cache = {}
        for apt_type in appt_types:
            type_num = apt_type.get('AppointmentTypeNum')
            type_name = apt_type.get('AppointmentTypeName', '')
            if type_num is not None:
                clinic_cache[type_num] = type_name
        
        _appointment_types_cache[clinic_num] = clinic_cache
        
        logger.info(f"Loaded {len(clinic_cache)} appointment types for clinic {clinic_num}")
        logger.debug(f"Clinic {clinic_num} appointment types: {clinic_cache}")
        
        return clinic_cache
        
    except Exception as e:
        logger.error(f"Failed to fetch appointment types for clinic {clinic_num}: {e}")
        # Return empty dict as fallback
        _appointment_types_cache[clinic_num] = {}
        return _appointment_types_cache[clinic_num]

def get_appointment_type_name(appointment: Dict[str, Any], clinic_num: int) -> str:
    """Get the appointment type name for an appointment in a specific clinic"""
    # Get clinic-specific appointment types
    apt_type_num = appointment.get('AppointmentTypeNum')
    if apt_type_num is not None:
        appointment_types = get_appointment_types(clinic_num)
        apt_type_name = appointment_types.get(apt_type_num, '')
        if apt_type_name:
            logger.debug(f"Found appointment type via AppointmentTypeNum {apt_type_num} for clinic {clinic_num}: '{apt_type_name}'")
            return apt_type_name
    
    # Fallback to direct fields
    apt_type_direct = appointment.get('AptType', '') or appointment.get('AppointmentType', '')
    if apt_type_direct:
        logger.debug(f"Found appointment type via direct field: '{apt_type_direct}'")
        return apt_type_direct
    
    logger.debug(f"No appointment type found for appointment {appointment.get('AptNum')} in clinic {clinic_num}")
    return ''

def get_appointment_type_num(appointment: Dict[str, Any]) -> Optional[int]:
    """Get the appointment type number for an appointment"""
    apt_type_num = appointment.get('AppointmentTypeNum')
    if apt_type_num is not None:
        return apt_type_num
    
    logger.debug(f"No appointment type number found for appointment {appointment.get('AptNum')}")
    return None

def has_ghl_tag(appointment: Dict[str, Any]) -> bool:
    """Check if appointment has [fromGHL] tag in the Note field"""
    note = appointment.get('Note', '') or ''
    return '[fromGHL]' in note

# === IMPROVED STATE MANAGEMENT ===
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
                    # Ensure timezone info is present
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
        return True  # First sync is always valid
    
    if not new_time:
        logger.warning(f"Clinic {clinic}: Attempted to set null sync time")
        return False
    
    # Ensure both have timezone info
    if old_time.tzinfo is None:
        old_time = old_time.replace(tzinfo=timezone.utc)
    if new_time.tzinfo is None:
        new_time = new_time.replace(tzinfo=timezone.utc)
    
    if new_time < old_time:
        logger.warning(f"Clinic {clinic}: Attempted to move sync time backwards from {old_time} to {new_time}")
        return False
    
    return True

def save_state_atomically(sync_times: Dict[int, Optional[datetime.datetime]], 
                         sent_ids: Set[str]) -> None:
    """Save both state files atomically to avoid corruption"""
    temp_sync_file = None
    temp_sent_file = None
    
    try:
        # Save sync times
        sync_data = {str(c): dt.isoformat() if dt else None for c, dt in sync_times.items()}
        with tempfile.NamedTemporaryFile(mode='w', delete=False, suffix='.tmp', 
                                        dir=os.path.dirname(STATE_FILE) or '.') as f:
            json.dump(sync_data, f, indent=2)
            temp_sync_file = f.name
        
        # Save sent appointments
        with tempfile.NamedTemporaryFile(mode='w', delete=False, suffix='.tmp',
                                        dir=os.path.dirname(SENT_APPTS_FILE) or '.') as f:
            json.dump(list(sent_ids), f, indent=2)
            temp_sent_file = f.name
        
        # Atomic moves
        shutil.move(temp_sync_file, STATE_FILE)
        shutil.move(temp_sent_file, SENT_APPTS_FILE)
        
        logger.debug(f"Atomically saved state files - sync times: {sync_data}")
        logger.debug(f"Saved {len(sent_ids)} sent appointment IDs")
        
    except Exception as e:
        logger.error(f"Error in atomic save: {e}")
        # Clean up temp files if they exist
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
                return set(str(x) for x in data)  # Ensure all IDs are strings
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
        # Ensure timezone info is present
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

# === OPTIMIZED APPOINTMENT FETCHING WITH TIME WINDOWING ===
def get_filtered_ops(clinic: int) -> List[int]:
    return CLINIC_OPERATORY_FILTERS.get(clinic, [])

def fetch_appointments_in_window(
    clinic: int,
    start_time: datetime.datetime,
    end_time: datetime.datetime,
    since: Optional[datetime.datetime] = None,
    ops: List[int] = None
) -> List[Dict[str, Any]]:
    """Fetch appointments for a specific time window to reduce database load"""
    
    if ops is None:
        ops = get_filtered_ops(clinic)
    
    params = {
        'ClinicNum': clinic,
        'dateStart': start_time.strftime('%Y-%m-%d'),
        'dateEnd': end_time.strftime('%Y-%m-%d')
    }
    
    if since:
        if since.tzinfo is None:
            since = since.replace(tzinfo=timezone.utc)
        eff_utc = (since + datetime.timedelta(seconds=1)).astimezone(timezone.utc)
        params['DateTStamp'] = eff_utc.strftime('%Y-%m-%d %H:%M:%S')
        logger.debug(f"Clinic {clinic}: fetching appointments modified after UTC {params['DateTStamp']}")
    
    logger.debug(f"Clinic {clinic}: fetching appointments for window {start_time.strftime('%Y-%m-%d')} to {end_time.strftime('%Y-%m-%d')}")
    logger.debug(f"→ GET /appointments with params: {params}")
    
    try:
        # Rate limit the request
        rate_limiter.wait_if_needed()
        
        session = get_optimized_session()
        r = session.get(f"{API_BASE_URL}/appointments",
                       headers=make_auth_header(), 
                       params=params, 
                       timeout=REQUEST_TIMEOUT)
        r.raise_for_status()
        
        data = r.json()
        appts = data if isinstance(data, list) else [data]
        logger.debug(f"← Raw API response: {len(appts)} appointments for clinic {clinic}")
        
        return appts
        
    except Exception as e:
        logger.error(f"Error fetching appointments for clinic {clinic} in window {start_time} to {end_time}: {e}")
        return []

def fetch_appointments_optimized(
    clinic: int,
    since: Optional[datetime.datetime],
    ops: List[int]
) -> List[Dict[str, Any]]:
    """Fetch appointments using time windowing to reduce database load"""
    
    now_utc = datetime.datetime.utcnow().replace(tzinfo=timezone.utc)
    
    # Use smaller time windows instead of the full lookahead period
    all_appointments = []
    window_start = now_utc
    
    # Calculate how many windows we need
    total_hours = TIME_WINDOW_HOURS
    windows_needed = 1  # Start with just the current window
    
    logger.info(f"Clinic {clinic}: Using {TIME_WINDOW_HOURS}-hour time windows for optimized fetching")
    
    for window_num in range(windows_needed):
        window_end = window_start + datetime.timedelta(hours=TIME_WINDOW_HOURS)
        
        logger.debug(f"Clinic {clinic}: Processing window {window_num + 1}/{windows_needed}")
        
        try:
            window_appointments = fetch_appointments_in_window(
                clinic, window_start, window_end, since, ops
            )
            
            if window_appointments:
                all_appointments.extend(window_appointments)
                logger.debug(f"Clinic {clinic}: Window {window_num + 1} returned {len(window_appointments)} appointments")
            
            # Move to next window
            window_start = window_end
            
        except Exception as e:
            logger.error(f"Error in window {window_num + 1} for clinic {clinic}: {e}")
            continue
    
    logger.info(f"Clinic {clinic}: Total appointments fetched across all windows: {len(all_appointments)}")
    
    # Filter appointments
    valid = []
    for a in all_appointments:
        apt_num = str(a.get('AptNum', ''))
        status = a.get('AptStatus', '')
        op_num = a.get('Op') or a.get('OperatoryNum')
        
        # Check for [fromGHL] tag - skip if present
        if has_ghl_tag(a):
            logger.debug(f"Skipping apt {apt_num}: contains [fromGHL] tag")
            continue
        
        # Check status
        if status not in VALID_STATUSES:
            logger.debug(f"Skipping apt {apt_num}: invalid status '{status}'")
            continue
            
        # Check operatory filter - MUST be from specified operatories
        if not ops:
            logger.warning(f"Clinic {clinic}: No operatory filter configured")
            continue
            
        if op_num not in ops:
            logger.debug(f"Skipping apt {apt_num}: operatory {op_num} not in allowed operatories {ops}")
            continue
        
        # Check broken appointment type filter
        if status == 'Broken':
            clinic_broken_filters = CLINIC_BROKEN_APPOINTMENT_TYPE_FILTERS.get(clinic, [])
            if clinic_broken_filters:  # Only apply filter if configured for this clinic
                apt_type_name = get_appointment_type_name(a, clinic)
                
                if apt_type_name not in clinic_broken_filters:
                    logger.debug(f"Skipping broken apt {apt_num}: appointment type '{apt_type_name}' not in allowed broken types {clinic_broken_filters} for clinic {clinic}")
                    continue
                else:
                    logger.info(f"✓ Broken apt {apt_num}: appointment type '{apt_type_name}' matches allowed broken types for clinic {clinic}")
        
        valid.append(a)
    
    logger.info(f"Clinic {clinic}: {len(valid)} valid appointment(s) found after filtering")
    if valid:
        logger.debug(f"Valid appointment IDs: {[a.get('AptNum') for a in valid]}")
    
    return valid

# === PATIENT ===
def get_patient_details(pat_num: int) -> Dict[str, Any]:
    if not pat_num:
        return {}
    try:
        logger.debug(f"Fetching patient details for PatNum {pat_num}")
        
        # Rate limit the request
        rate_limiter.wait_if_needed()
        
        session = get_optimized_session()
        r = session.get(f"{API_BASE_URL}/patients/{pat_num}", 
                       headers=make_auth_header(), 
                       timeout=REQUEST_TIMEOUT)
        r.raise_for_status()
        return r.json()
    except Exception as e:
        logger.warning(f"Failed to fetch patient {pat_num}: {e}")
        return {}

# === KERAGON SENDING ===
def send_to_keragon(appt: Dict[str, Any], clinic: int, dry_run: bool = False) -> bool:
    apt_num = str(appt.get('AptNum', ''))
    
    # Get patient details
    patient = get_patient_details(appt.get('PatNum'))
    first = patient.get('FName') or appt.get('FName', '')
    last = patient.get('LName') or appt.get('LName', '')
    name = f"{first} {last}".strip() or 'Unknown'
    provider_name = 'Dr. Gharbi' if clinic == 9034 else 'Dr. Ensley'

    # Get appointment type number
    apt_type_num = get_appointment_type_num(appt)

    # Parse time from OpenDental (comes as UTC) and preserve the exact time values
    st_utc = parse_time(appt.get('AptDateTime'))
    if not st_utc:
        logger.error(f"Invalid start time for appointment {apt_num}")
        return False
    
    logger.debug(f"Appointment {apt_num} - Raw time from OpenDental: {appt.get('AptDateTime')}")
    logger.debug(f"Appointment {apt_num} - Parsed UTC time: {st_utc}")
    
    # Extract the time components from UTC and recreate in GHL timezone
    st_ghl = st_utc.replace(tzinfo=None)  # Remove UTC timezone
    st_ghl = st_ghl.replace(tzinfo=GHL_TIMEZONE)  # Apply GHL timezone to same time values
    
    # Calculate duration and end time
    duration_minutes = calculate_pattern_duration(appt.get('Pattern', ''))
    if duration_minutes <= 0:
        logger.warning(f"No slots in pattern '{appt.get('Pattern', '')}', defaulting to 60 minutes")
        duration_minutes = 60
    
    # Calculate end time in GHL timezone
    en_ghl = st_ghl + datetime.timedelta(minutes=duration_minutes)

    # Format for payload (ISO with timezone offset)
    st_payload = st_ghl.isoformat(timespec='seconds')
    en_payload = en_ghl.isoformat(timespec='seconds')

    logger.info(f"Appointment {apt_num} for {name} ({provider_name})")
    logger.info(f"  OpenDental UTC: {st_utc}")
    logger.info(f"  GHL time (matching values): {st_ghl}")
    logger.info(f"  Duration: {duration_minutes} minutes")
    logger.info(f"  Appointment Type Number: {apt_type_num}")
    logger.info(f"  Payload: {st_payload} to {en_payload}")

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
        logger.debug(f"DRY RUN payload: {json.dumps(payload, indent=2)}")
        return True

    try:
        logger.debug(f"Sending to Keragon: {json.dumps(payload, indent=2)}")
        
        session = get_optimized_session()
        r = session.post(KERAGON_WEBHOOK_URL, json=payload, timeout=REQUEST_TIMEOUT)
        r.raise_for_status()
        
        logger.info(f"✓ Successfully sent appointment {apt_num} to Keragon")
        return True
    except Exception as e:
        logger.error(f"✗ Failed to send appointment {apt_num} to Keragon: {e}")
        if hasattr(e, 'response') and e.response is not None:
            try:
                error_detail = e.response.text
                logger.error(f"  Response: {error_detail}")
            except:
                pass
        return False

# === PARALLEL CLINIC PROCESSING ===
def process_clinic(clinic: int, last_syncs: Dict[int, Optional[datetime.datetime]], 
                  sent_appointments: Set[str], dry_run: bool = False) -> Dict[str, Any]:
    """Process a single clinic and return results"""
    
    logger.info(f"--- Processing Clinic {clinic} ---")
    
    try:
        # Fetch appointments with optimized windowing
        appointments = fetch_appointments_optimized(clinic, last_syncs.get(clinic), get_filtered_ops(clinic))
        
        # Only update sync time if we actually process appointments
        if not appointments:
            logger.info(f"Clinic {clinic}: No appointments to process - sync time unchanged")
            return {
                'clinic': clinic,
                'sent': 0,
                'skipped': 0,
                'processed': 0,
                'latest_timestamp': last_syncs.get(clinic),
                'appointments_sent': []
            }
        
        clinic_sent = 0
        clinic_skipped = 0
        clinic_processed = 0
        clinic_appointments_sent = []
        
        # Track the latest timestamp we've processed
        latest_processed_timestamp = last_syncs.get(clinic)

        # Sort appointments by DateTStamp to process in chronological order
        appointments.sort(key=lambda x: parse_time(x.get('DateTStamp')) or datetime.datetime.min.replace(tzinfo=timezone.utc))

        for appt in appointments:
            apt_num = str(appt.get('AptNum', ''))
            clinic_processed += 1
            
            # Update latest processed timestamp for ANY appointment we process
            appt_timestamp = parse_time(appt.get('DateTStamp'))
            if appt_timestamp:
                if not latest_processed_timestamp or appt_timestamp > latest_processed_timestamp:
                    latest_processed_timestamp = appt_timestamp
                    logger.debug(f"Updated latest processed timestamp to {latest_processed_timestamp}")
            
            # Skip if already sent
            if apt_num in sent_appointments:
                logger.debug(f"Skipping appointment {apt_num}: already sent")
                clinic_skipped += 1
                continue

            # Try to send
            if send_to_keragon(appt, clinic, dry_run):
                clinic_sent += 1
                clinic_appointments_sent.append(apt_num)
            else:
                logger.warning(f"Failed to send appointment {apt_num}")
                # Continue processing other appointments instead of stopping

        logger.info(f"Clinic {clinic}: ✓ Sent {clinic_sent}, Skipped {clinic_skipped}, Total processed {clinic_processed}")
        
        return {
            'clinic': clinic,
            'sent': clinic_sent,
            'skipped': clinic_skipped,
            'processed': clinic_processed,
            'latest_timestamp': latest_processed_timestamp,
            'appointments_sent': clinic_appointments_sent
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

# === MAIN SYNC LOGIC WITH PARALLEL PROCESSING ===
def run_sync(dry_run: bool = False):
    if not (DEVELOPER_KEY and CUSTOMER_KEY and KERAGON_WEBHOOK_URL and CLINIC_NUMS):
        logger.critical("Missing configuration – check your environment variables")
        logger.critical("Required: OPEN_DENTAL_DEVELOPER_KEY, OPEN_DENTAL_CUSTOMER_KEY, KERAGON_WEBHOOK_URL, CLINIC_NUMS")
        sys.exit(1)

    logger.info("=== Starting OPTIMIZED OpenDental → Keragon Sync ===")
    logger.info(f"Performance optimizations enabled:")
    logger.info(f"  - Time windows: {TIME_WINDOW_HOURS} hours (instead of full lookahead)")
    logger.info(f"  - Max concurrent clinics: {MAX_CONCURRENT_CLINICS}")
    logger.info(f"  - Request timeout: {REQUEST_TIMEOUT}s")
    logger.info(f"  - Retry attempts: {RETRY_ATTEMPTS}")
    logger.info(f"  - Rate limiting: {CLINIC_DELAY_SECONDS}s between requests")
    logger.info("Time values will match exactly: 8 AM OpenDental → 8 AM GHL")
    logger.info("Appointments with [fromGHL] tag will be skipped")
    if dry_run:
        logger.info("DRY RUN MODE: No data will be sent to Keragon")

    # Load appointment types cache for each clinic
    logger.info("Loading appointment types for each clinic...")
    for clinic in CLINIC_NUMS:
        types = get_appointment_types(clinic)
        logger.info(f"Clinic {clinic}: Loaded {len(types)} appointment types")

    # Load state
    last_syncs = load_last_sync_times()
    sent_appointments = load_sent_appointments()
    new_syncs = last_syncs.copy()  # Start with existing sync times
    
    total_sent = 0
    total_skipped = 0
    total_processed = 0
    
    # Track if we need to save state (only if something actually changed)
    state_changed = False

    # Process clinics in parallel with controlled concurrency
    logger.info(f"Processing {len(CLINIC_NUMS)} clinics with max {MAX_CONCURRENT_CLINICS} concurrent")
    
    clinic_results = []
    
    # Use ThreadPoolExecutor for parallel processing
    with ThreadPoolExecutor(max_workers=MAX_CONCURRENT_CLINICS) as executor:
        # Submit all clinic processing tasks
        future_to_clinic = {
            executor.submit(process_clinic, clinic, last_syncs, sent_appointments, dry_run): clinic
            for clinic in CLINIC_NUMS
        }
        
        # Process completed tasks as they finish
        for future in as_completed(future_to_clinic):
            clinic = future_to_clinic[future]
            try:
                result = future.result()
                clinic_results.append(result)
                
                # Update tracking variables
                total_sent += result['sent']
                total_skipped += result['skipped']
                total_processed += result['processed']
                
                # Update state if clinic processed appointments
                if result['processed'] > 0:
                    if result['latest_timestamp'] and validate_sync_time_update(
                        clinic, last_syncs.get(clinic), result['latest_timestamp']
                    ):
                        new_syncs[clinic] = result['latest_timestamp']
                        state_changed = True
                        logger.info(f"Clinic {clinic}: Updated sync time to {result['latest_timestamp']}")
                    
                    # Add sent appointments to tracking set
                    if not dry_run:
                        for apt_id in result['appointments_sent']:
                            sent_appointments.add(apt_id)
                            state_changed = True
                
                if 'error' in result:
                    logger.error(f"Clinic {clinic} completed with error: {result['error']}")
                else:
                    logger.info(f"Clinic {clinic} completed successfully")
                    
            except Exception as e:
                logger.error(f"Failed to process clinic {clinic}: {e}")

    # Only save state if something actually changed
    if state_changed and not dry_run:
        try:
            save_state_atomically(new_syncs, sent_appointments)
            logger.info("Successfully saved state files")
        except Exception as e:
            logger.error(f"Failed to save state: {e}")
    elif not state_changed:
        logger.info("No state changes - state files not updated")
    else:
        logger.info("DRY RUN: State files not updated")
    
    # Summary report
    logger.info("=== Sync Complete ===")
    logger.info(f"Total sent: {total_sent}")
    logger.info(f"Total skipped: {total_skipped}")
    logger.info(f"Total processed: {total_processed}")
    logger.info(f"Total tracked appointments: {len(sent_appointments)}")
    
    # Per-clinic summary
    logger.info("=== Per-Clinic Summary ===")
    for result in clinic_results:
        clinic = result['clinic']
        logger.info(f"Clinic {clinic}: Sent {result['sent']}, Skipped {result['skipped']}, Processed {result['processed']}")

def cleanup_old_sent_appointments(days_to_keep: int = 30):
    """Remove old appointment IDs from the sent appointments file"""
    logger.info(f"Note: Sent appointments file contains {len(load_sent_appointments())} appointment IDs")
    logger.info("Consider implementing cleanup logic if this file grows too large")

if __name__ == '__main__':
    import argparse
    parser = argparse.ArgumentParser(description='Sync OpenDental → Keragon (Optimized)')
    parser.add_argument('--reset', action='store_true', help='Clear saved sync timestamps and sent appointments')
    parser.add_argument('--reset-sent', action='store_true', help='Clear only sent appointments tracking')
    parser.add_argument('--verbose', action='store_true', help='Enable DEBUG logging')
    parser.add_argument('--dry-run', action='store_true', help='Show what would be sent without actually sending')
    parser.add_argument('--show-appt-types', action='store_true', help='Display all appointment types for each clinic and exit')
    args = parser.parse_args()

    if args.verbose:
        logger.setLevel(logging.DEBUG)
        logger.debug("Debug logging enabled")

    if args.show_appt_types:
        if not (DEVELOPER_KEY and CUSTOMER_KEY):
            logger.critical("Missing API credentials - check OPEN_DENTAL_DEVELOPER_KEY and OPEN_DENTAL_CUSTOMER_KEY")
            sys.exit(1)
        print("=== Available Appointment Types by Clinic ===")
        for clinic in CLINIC_NUMS:
            appt_types = get_appointment_types(clinic)
            print(f"\nClinic {clinic}:")
            for type_num, type_name in sorted(appt_types.items()):
                print(f"  {type_num}: {type_name}")
        sys.exit(0)

    if args.reset:
        if os.path.exists(STATE_FILE):
            os.remove(STATE_FILE)
            logger.info("Cleared sync state file")
        if os.path.exists(SENT_APPTS_FILE):
            os.remove(SENT_APPTS_FILE)
            logger.info("Cleared sent appointments file")
        sys.exit(0)

    if args.reset_sent:
        if os.path.exists(SENT_APPTS_FILE):
            os.remove(SENT_APPTS_FILE)
            logger.info("Cleared sent appointments file")
        sys.exit(0)

    run_sync(dry_run=args.dry_run)
