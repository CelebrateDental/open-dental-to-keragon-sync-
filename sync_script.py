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
from typing import List, Dict, Any, Optional, Set, Tuple
from requests.exceptions import HTTPError, RequestException
from collections import defaultdict
from dataclasses import dataclass, asdict
from concurrent.futures import ThreadPoolExecutor, as_completed
import re

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
    lookahead_hours: int = int(os.environ.get('LOOKAHEAD_HOURS', '24'))
    first_run_lookahead_days: int = int(os.environ.get('FIRST_RUN_LOOKAHEAD_DAYS', '30'))
    max_workers: int = int(os.environ.get('MAX_WORKERS', '5'))
    request_timeout: int = int(os.environ.get('REQUEST_TIMEOUT', '30'))
    retry_attempts: int = int(os.environ.get('RETRY_ATTEMPTS', '3'))

    def __post_init__(self):
        # Parse clinic numbers
        clinic_nums_str = os.environ.get('CLINIC_NUMS', '')
        self.clinic_nums = [
            int(x) for x in clinic_nums_str.split(',') if x.strip().isdigit()
        ]
        
        # Parse operatory filters
        self.clinic_operatory_filters: Dict[int, List[int]] = {
            9034: [11579, 11580, 11588],
            9035: [11574, 11576, 11577],
        }

@dataclass
class AppointmentData:
    """Structured appointment data"""
    apt_num: int
    pat_num: int
    clinic_num: int
    operatory_num: int
    apt_date_time: datetime.datetime
    apt_end_time: datetime.datetime  # Calculated field
    pattern: str
    apt_status: str
    date_t_stamp: datetime.datetime
    provider_abbr: str = ''
    note: str = ''

    @classmethod
    def from_api_response(cls, data: Dict[str, Any]) -> 'AppointmentData':
        """Create AppointmentData from API response with proper time calculations"""
        apt_datetime = parse_datetime(data.get('AptDateTime'))
        pattern = data.get('Pattern', '')
        
        # Calculate end time based on pattern
        duration_minutes = calculate_appointment_duration(pattern)
        apt_end_time = apt_datetime + datetime.timedelta(minutes=duration_minutes) if apt_datetime else None
        
        return cls(
            apt_num=int(data.get('AptNum', 0)),
            pat_num=int(data.get('PatNum', 0)),
            clinic_num=int(data.get('ClinicNum', 0)),
            operatory_num=int(data.get('Op', 0)),
            apt_date_time=apt_datetime,
            apt_end_time=apt_end_time,
            pattern=pattern,
            apt_status=data.get('AptStatus', ''),
            date_t_stamp=parse_datetime(data.get('DateTStamp')),
            provider_abbr=data.get('provAbbr', ''),
            note=data.get('Note', '')
        )

# === GLOBAL CONFIG ===
config = Config()

# === LOGGER SETUP ===
def setup_logging():
    """Setup structured logging"""
    formatter = logging.Formatter(
        '%(asctime)s - %(name)s - %(levelname)s - [%(funcName)s:%(lineno)d] - %(message)s'
    )
    handler = logging.StreamHandler(sys.stdout)
    handler.setFormatter(formatter)
    
    logger = logging.getLogger('opendental_sync')
    logger.setLevel(getattr(logging, config.log_level))
    logger.addHandler(handler)
    
    # Suppress noisy libraries
    logging.getLogger('urllib3').setLevel(logging.WARNING)
    logging.getLogger('requests').setLevel(logging.WARNING)
    
    return logger

logger = setup_logging()

# === UTILITY FUNCTIONS ===
def parse_datetime(dt_str: Optional[str]) -> Optional[datetime.datetime]:
    """Parse datetime string with multiple format support"""
    if not dt_str:
        return None
    
    formats = [
        "%Y-%m-%dT%H:%M:%S.%f",  # ISO with microseconds
        "%Y-%m-%dT%H:%M:%S",     # ISO without microseconds
        "%Y-%m-%d %H:%M:%S.%f",  # Space separated with microseconds
        "%Y-%m-%d %H:%M:%S",     # Space separated without microseconds
        "%Y-%m-%d",              # Date only
        "%m/%d/%Y %H:%M:%S",     # US format
        "%m/%d/%Y"               # US date only
    ]
    
    for fmt in formats:
        try:
            return datetime.datetime.strptime(dt_str, fmt)
        except ValueError:
            continue
    
    logger.warning(f"Unable to parse datetime: {dt_str}")
    return None

def calculate_appointment_duration(pattern: str) -> int:
    """
    Calculate appointment duration in minutes from OpenDental pattern.
    OpenDental patterns use 'X' for scheduled time slots (typically 10-15 minute increments).
    """
    if not pattern:
        return 60  # Default 1 hour if no pattern
    
    # Count 'X' characters in pattern - each typically represents 10 minutes
    # But this can vary by practice configuration
    x_count = pattern.count('X')
    if x_count == 0:
        return 60  # Default if no X's found
    
    # Most practices use 10-minute increments, but some use 15
    # You may need to adjust this based on your practice configuration
    minutes_per_x = 10  # Adjust this value based on your OpenDental setup
    duration = x_count * minutes_per_x
    
    # Reasonable bounds checking
    if duration < 15:
        duration = 15  # Minimum 15 minutes
    elif duration > 480:  # 8 hours
        duration = 480  # Maximum 8 hours
    
    logger.debug(f"Pattern '{pattern}' -> {x_count} X's -> {duration} minutes")
    return duration

def make_auth_header() -> Dict[str, str]:
    """Create authentication header"""
    return {
        'Authorization': f'ODFHIR {config.developer_key}/{config.customer_key}',
        'Content-Type': 'application/json',
        'User-Agent': 'OpenDental-Keragon-Sync/1.0'
    }

def retry_request(func, max_attempts: int = None, backoff_factor: float = 1.5):
    """Retry decorator for API requests"""
    max_attempts = max_attempts or config.retry_attempts
    
    def wrapper(*args, **kwargs):
        last_exception = None
        for attempt in range(max_attempts):
            try:
                return func(*args, **kwargs)
            except (HTTPError, RequestException) as e:
                last_exception = e
                if attempt < max_attempts - 1:
                    wait_time = backoff_factor ** attempt
                    logger.warning(f"Request failed (attempt {attempt + 1}/{max_attempts}), retrying in {wait_time}s: {e}")
                    time.sleep(wait_time)
                else:
                    logger.error(f"Request failed after {max_attempts} attempts: {e}")
        raise last_exception
    return wrapper

# === STATE MANAGEMENT ===
def load_last_sync_state() -> Dict[int, datetime.datetime]:
    """Load last sync state from file"""
    if not os.path.exists(config.state_file):
        logger.info("No state file found, using 24-hour lookback for first run")
        cutoff = datetime.datetime.utcnow() - datetime.timedelta(hours=24)
        return {clinic: cutoff for clinic in config.clinic_nums}
    
    try:
        with open(config.state_file, 'r') as f:
            data = json.load(f)
        
        state = {}
        for k, v in data.items():
            try:
                state[int(k)] = datetime.datetime.fromisoformat(v)
            except (ValueError, TypeError) as e:
                logger.warning(f"Invalid state entry for clinic {k}: {e}")
                state[int(k)] = datetime.datetime.utcnow() - datetime.timedelta(hours=24)
        
        logger.info(f"Loaded sync state for {len(state)} clinics")
        return state
    except Exception as e:
        logger.error(f"Error reading state file: {e}, using fallback")
        cutoff = datetime.datetime.utcnow() - datetime.timedelta(hours=24)
        return {clinic: cutoff for clinic in config.clinic_nums}

def save_last_sync_state(state: Dict[int, datetime.datetime]) -> None:
    """Save last sync state to file"""
    try:
        # Create backup of existing state
        if os.path.exists(config.state_file):
            backup_file = f"{config.state_file}.backup"
            os.rename(config.state_file, backup_file)
        
        serialized = {str(k): v.isoformat() for k, v in state.items()}
        
        # Write to temporary file first, then rename (atomic write)
        temp_file = f"{config.state_file}.tmp"
        with open(temp_file, 'w') as f:
            json.dump(serialized, f, indent=2)
        os.rename(temp_file, config.state_file)
        
        logger.debug(f"Saved sync state for {len(state)} clinics")
    except Exception as e:
        logger.error(f"Error saving state file: {e}")

# === API OPERATIONS ===
@retry_request
def fetch_operatories_for_clinic(clinic_num: int) -> List[Dict[str, Any]]:
    """Fetch operatories for a clinic"""
    endpoint = f"{config.api_base_url}/operatories"
    headers = make_auth_header()
    
    response = requests.get(
        endpoint,
        headers=headers,
        params={'ClinicNum': clinic_num},
        timeout=config.request_timeout
    )
    response.raise_for_status()
    
    operatories = response.json()
    logger.info(f"Found {len(operatories)} operatories for clinic {clinic_num}")
    for op in operatories:
        logger.debug(f"  Op {op.get('OperatoryNum')}: {op.get('OpName', 'Unnamed')}")
    
    return operatories

@retry_request
def fetch_appointments_batch(clinic_num: int, date_start: str, date_end: str, status: str, operatory_num: Optional[int] = None) -> List[Dict[str, Any]]:
    """Fetch a batch of appointments"""
    endpoint = f"{config.api_base_url}/appointments"
    headers = make_auth_header()
    
    params = {
        'dateStart': date_start,
        'dateEnd': date_end,
        'ClinicNum': clinic_num,
        'AptStatus': status,
        'Limit': 1000
    }
    
    if operatory_num:
        params['Op'] = operatory_num
    
    response = requests.get(endpoint, headers=headers, params=params, timeout=config.request_timeout)
    response.raise_for_status()
    
    data = response.json()
    appointments = data if isinstance(data, list) else []
    
    # Ensure operatory number is set
    for apt in appointments:
        if operatory_num and 'Op' not in apt:
            apt['Op'] = operatory_num
    
    return appointments

def fetch_appointments_for_clinic(clinic_num: int, since: datetime.datetime, is_first_run: bool = False) -> List[AppointmentData]:
    """Fetch all relevant appointments for a clinic"""
    now = datetime.datetime.utcnow()

    # --- MODIFICATION APPLIED HERE ---
    date_start = now.strftime("%Y-%m-%d")
    date_end = (now + datetime.timedelta(days=30)).strftime("%Y-%m-%d")

    logger.info(f"Fetching appointments for clinic {clinic_num} for the next 30 days")
    logger.info(f"Date range: {date_start} to {date_end}")

    statuses = ['Scheduled', 'Complete', 'Broken']
    filtered_ops = config.clinic_operatory_filters.get(clinic_num, [])

    all_appointments = []

    if filtered_ops:
        logger.info(f"Filtering to operatories: {filtered_ops}")
        for op_num in filtered_ops:
            for status in statuses:
                try:
                    batch = fetch_appointments_batch(clinic_num, date_start, date_end, status, op_num)
                    all_appointments.extend(batch)
                except Exception as e:
                    logger.error(f"Failed to fetch {status} appointments for operatory {op_num}: {e}")
    else:
        logger.info("No operatory filter, fetching all appointments")
        for status in statuses:
            try:
                batch = fetch_appointments_batch(clinic_num, date_start, date_end, status)
                all_appointments.extend(batch)
            except Exception as e:
                logger.error(f"Failed to fetch {status} appointments: {e}")

    structured_appointments = []
    for apt_data in all_appointments:
        try:
            apt = AppointmentData.from_api_response(apt_data)
            if filtered_ops and apt.operatory_num not in filtered_ops:
                continue
            structured_appointments.append(apt)
        except Exception as e:
            logger.warning(f"Failed to parse appointment {apt_data.get('AptNum', 'unknown')}: {e}")

    logger.info(f"Parsed {len(structured_appointments)} valid appointments for clinic {clinic_num}")
    return structured_appointments
    
@retry_request
def fetch_patient_details(patient_num: int) -> Dict[str, Any]:
    """Fetch detailed patient information"""
    if not patient_num:
        return {}
    
    # Try primary endpoint first
    endpoint = f"{config.api_base_url}/patients/{patient_num}"
    headers = make_auth_header()
    
    try:
        response = requests.get(endpoint, headers=headers, timeout=config.request_timeout)
        response.raise_for_status()
        return response.json()
    except Exception:
        logger.debug(f"Primary patient fetch failed for {patient_num}, trying fallback")
    
    # Fallback to search endpoint
    try:
        response = requests.get(
            f"{config.api_base_url}/patients",
            headers=headers,
            params={'PatNum': patient_num},
            timeout=config.request_timeout
        )
        response.raise_for_status()
        data = response.json()
        if isinstance(data, list) and data:
            return data[0]
        elif isinstance(data, dict):
            return data
    except Exception as e:
        logger.error(f"All patient fetch attempts failed for {patient_num}: {e}")
    
    return {}

# === KERAGON INTEGRATION ===
@retry_request
def send_appointment_to_keragon(appointment: AppointmentData, patient_data: Dict[str, Any]) -> bool:
    """Send appointment to Keragon with proper timing"""
    # Build comprehensive payload
    payload = {
        # Patient info
        'firstName': patient_data.get('FName', ''),
        'lastName': patient_data.get('LName', ''),
        'email': patient_data.get('Email', ''),
        'phone': (
            patient_data.get('HmPhone') or 
            patient_data.get('WkPhone') or 
            patient_data.get('WirelessPhone') or 
            ''
        ),
        'birthdate': patient_data.get('Birthdate', ''),
        'gender': patient_data.get('Gender', ''),
        'patientId': str(appointment.pat_num),
        
        # Address info
        'address': patient_data.get('Address', ''),
        'address2': patient_data.get('Address2', ''),
        'city': patient_data.get('City', ''),
        'state': patient_data.get('State', ''),
        'zipCode': patient_data.get('Zip', ''),
        
        # Appointment info - KEY FIXES HERE
        'appointmentId': str(appointment.apt_num),
        'appointmentTime': appointment.apt_date_time.isoformat() if appointment.apt_date_time else '',
        'appointmentEndTime': appointment.apt_end_time.isoformat() if appointment.apt_end_time else '',  # NEW FIELD
        'appointmentDurationMinutes': calculate_appointment_duration(appointment.pattern),  # EXPLICIT DURATION
        'locationId': str(appointment.clinic_num),
        'calendarId': str(appointment.operatory_num),
        'status': appointment.apt_status,
        'providerName': appointment.provider_abbr,
        'appointmentLength': appointment.pattern,  # Keep original pattern too
        'notes': appointment.note,
        
        # Clinic info
        'clinicName': patient_data.get('ClinicName', ''),
        'balanceTotal': patient_data.get('BalTotal', 0.0),
        
        # Metadata
        'syncTimestamp': datetime.datetime.utcnow().isoformat(),
        'lastModified': appointment.date_t_stamp.isoformat() if appointment.date_t_stamp else ''
    }
    
    # Clean up empty values
    payload = {k: v for k, v in payload.items() if v not in [None, '', 0]}
    
    response = requests.post(
        config.keragon_webhook_url,
        json=payload,
        timeout=config.request_timeout,
        headers={'Content-Type': 'application/json'}
    )
    response.raise_for_status()
    
    logger.info(
        f"✓ Sent to Keragon: {payload.get('firstName', '')} {payload.get('lastName', '')} "
        f"- {payload.get('appointmentTime', '')} to {payload.get('appointmentEndTime', '')} "
        f"(Op {appointment.operatory_num})"
    )
    return True

def process_appointments_for_clinic(clinic_num: int, appointments: List[AppointmentData], since: datetime.datetime) -> Tuple[datetime.datetime, int, int]:
    """Process appointments for a clinic and send to Keragon"""
    if not appointments:
        logger.info(f"No appointments to process for clinic {clinic_num}")
        return since, 0, 0
    
    # Filter to new/modified appointments
    new_appointments = [
        apt for apt in appointments 
        if apt.date_t_stamp and apt.date_t_stamp >= since
    ]
    
    if not new_appointments:
        logger.info(f"No new appointments since {since.isoformat()} for clinic {clinic_num}")
        return since, 0, 0
    
    logger.info(f"Processing {len(new_appointments)} new/modified appointments for clinic {clinic_num}")
    
    # Track patient data to avoid duplicate fetches
    patient_cache = {}
    success_count = 0
    max_timestamp = since
    
    # Process appointments with threading for patient data fetches
    with ThreadPoolExecutor(max_workers=config.max_workers) as executor:
        # Submit patient fetch tasks
        patient_futures = {}
        for apt in new_appointments:
            if apt.pat_num not in patient_cache and apt.pat_num not in patient_futures:
                future = executor.submit(fetch_patient_details, apt.pat_num)
                patient_futures[apt.pat_num] = future
        
        # Collect patient data
        for pat_num, future in patient_futures.items():
            try:
                patient_cache[pat_num] = future.result()
            except Exception as e:
                logger.error(f"Failed to fetch patient {pat_num}: {e}")
                patient_cache[pat_num] = {}
    
    # Send appointments to Keragon
    for appointment in new_appointments:
        try:
            patient_data = patient_cache.get(appointment.pat_num, {})
            if send_appointment_to_keragon(appointment, patient_data):
                success_count += 1
            
            # Update max timestamp
            if appointment.date_t_stamp > max_timestamp:
                max_timestamp = appointment.date_t_stamp
        except Exception as e:
            logger.error(f"Failed to process appointment {appointment.apt_num}: {e}")
    
    logger.info(f"Clinic {clinic_num}: Successfully processed {success_count}/{len(new_appointments)} appointments")
    
    # Return the latest timestamp, bounded by current time
    return min(max_timestamp, datetime.datetime.utcnow()), len(new_appointments), success_count

# === MAIN SYNC LOGIC ===
def validate_configuration() -> bool:
    """Validate configuration before running sync"""
    errors = []
    
    if not config.developer_key:
        errors.append("OPEN_DENTAL_DEVELOPER_KEY not set")
    if not config.customer_key:
        errors.append("OPEN_DENTAL_CUSTOMER_KEY not set")
    if not config.keragon_webhook_url:
        errors.append("KERAGON_WEBHOOK_URL not set")
    if not config.clinic_nums:
        errors.append("CLINIC_NUMS not set or invalid")
    
    if errors:
        for error in errors:
            logger.critical(error)
        return False
    
    logger.info(f"Configuration valid. Syncing {len(config.clinic_nums)} clinics: {config.clinic_nums}")
    return True

def run_sync() -> bool:
    """Run the main synchronization process"""
    if not validate_configuration():
        return False
    
    logger.info("=== Starting OpenDental → Keragon Sync ===")
    start_time = datetime.datetime.utcnow()
    
    # Load previous sync state
    last_state = load_last_sync_state()
    new_state = last_state.copy()
    
    # Detect if this is a first run for any clinic
    is_first_run = not os.path.exists(config.state_file)
    if is_first_run:
        logger.info(f"First run detected - will fetch {config.first_run_lookahead_days} days ahead")
    
    total_processed = 0
    total_successful = 0
    
    for clinic_num in config.clinic_nums:
        try:
            logger.info(f"\n--- Processing Clinic {clinic_num} ---")
            
            # Get last sync time for this clinic
            since = last_state.get(clinic_num, datetime.datetime.utcnow() - datetime.timedelta(hours=24))
            logger.info(f"Last sync: {since.isoformat()}")
            
            # Fetch and verify operatories
            try:
                fetch_operatories_for_clinic(clinic_num)
            except Exception as e:
                logger.warning(f"Could not fetch operatories for clinic {clinic_num}: {e}")
            
            # Fetch appointments
            appointments = fetch_appointments_for_clinic(clinic_num, since, is_first_run)
            
            # Process appointments
            new_timestamp, processed, successful = process_appointments_for_clinic(
                clinic_num, appointments, since
            )
            
            # Update state
            new_state[clinic_num] = new_timestamp
            total_processed += processed
            total_successful += successful
            
            logger.info(f"Clinic {clinic_num} complete. New sync timestamp: {new_timestamp.isoformat()}")
            
        except Exception as e:
            logger.error(f"Failed to process clinic {clinic_num}: {e}")
            # Don't update timestamp for failed clinics
    
    # Save updated state
    save_last_sync_state(new_state)
    
    # Summary
    duration = datetime.datetime.utcnow() - start_time
    logger.info(f"\n=== Sync Complete ===")
    logger.info(f"Duration: {duration}")
    logger.info(f"Total appointments processed: {total_processed}")
    logger.info(f"Successfully sent to Keragon: {total_successful}")
    
    if total_processed > 0:
        success_rate = (total_successful / total_processed) * 100
        logger.info(f"Success rate: {success_rate:.1f}%")
    
    return total_processed == 0 or total_successful > 0

# === CLI INTERFACE ===
def main():
    """Main CLI interface"""
    import argparse
    
    parser = argparse.ArgumentParser(
        description='Sync OpenDental appointments to Keragon',
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  %(prog)s --once         Run sync once and exit
  %(prog)s --test         Test API connectivity
  %(prog)s --dump-state   Show current sync state
  %(prog)s --reset        Reset sync state
  %(prog)s --verbose      Enable debug logging
"""
    )
    
    parser.add_argument('--once', action='store_true', help='Run sync once and exit')
    parser.add_argument('--test', action='store_true', help='Test connectivity only')
    parser.add_argument('--dump-state', action='store_true', help='Print last sync state and exit')
    parser.add_argument('--reset', action='store_true', help='Reset state file and exit')
    parser.add_argument('--verbose', action='store_true', help='Enable debug logging')
    parser.add_argument('--dry-run', action='store_true', help='Process appointments but don\'t send to Keragon')
    
    args = parser.parse_args()
    
    # Handle verbose logging
    if args.verbose:
        logger.setLevel(logging.DEBUG)
        logger.debug("Debug logging enabled")
    
    # Handle dump state
    if args.dump_state:
        if os.path.exists(config.state_file):
            with open(config.state_file, 'r') as f:
                print(json.dumps(json.load(f), indent=2))
        else:
            print("No state file exists.")
        return 0
    
    # Handle reset
    if args.reset:
        if os.path.exists(config.state_file):
            os.remove(config.state_file)
            logger.info("State file removed.")
        else:
            logger.info("No state file to remove.")
        return 0
    
    # Handle test mode
    if args.test:
        logger.info("Testing connectivity...")
        if not validate_configuration():
            return 1
        
        # Test OpenDental API
        try:
            headers = make_auth_header()
            response = requests.get(f"{config.api_base_url}/operatories", headers=headers, timeout=10)
            response.raise_for_status()
            logger.info("✓ OpenDental API connection successful")
        except Exception as e:
            logger.error(f"✗ OpenDental API connection failed: {e}")
            return 1
        
        # Test Keragon webhook
        try:
            test_payload = {"test": True, "timestamp": datetime.datetime.utcnow().isoformat()}
            response = requests.post(config.keragon_webhook_url, json=test_payload, timeout=10)
            response.raise_for_status()
            logger.info("✓ Keragon webhook connection successful")
        except Exception as e:
            logger.error(f"✗ Keragon webhook connection failed: {e}")
            return 1
        
        logger.info("All connectivity tests passed!")
        return 0
    
    # Handle dry run
    if args.dry_run:
        logger.info("DRY RUN MODE - appointments will be processed but not sent to Keragon")
        # TODO: Implement dry run logic
        return 0
    
    # Run sync
    try:
        success = run_sync()
        return 0 if success else 1
    except KeyboardInterrupt:
        logger.info("Sync interrupted by user")
        return 130
    except Exception as e:
        logger.critical(f"Sync failed with unexpected error: {e}", exc_info=True)
        return 1

if __name__ == "__main__":
    sys.exit(main())
