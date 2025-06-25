#!/usr/bin/env python3
import os
import sys
import json
import logging
import datetime
import requests
import tempfile
import shutil
from datetime import timezone, timedelta
from zoneinfo import ZoneInfo
from typing import List, Dict, Any, Optional, Set

# === CONFIGURATION ===
API_BASE_URL        = os.environ.get('OPEN_DENTAL_API_URL', 'https://api.opendental.com/api/v1')
DEVELOPER_KEY       = os.environ.get('OPEN_DENTAL_DEVELOPER_KEY')
CUSTOMER_KEY        = os.environ.get('OPEN_DENTAL_CUSTOMER_KEY')
KERAGON_WEBHOOK_URL = os.environ.get('KERAGON_WEBHOOK_URL')

STATE_FILE      = 'last_sync_state.json'
SENT_APPTS_FILE = 'sent_appointments.json'
LOG_LEVEL       = os.environ.get('LOG_LEVEL', 'INFO')
LOOKAHEAD_HOURS = int(os.environ.get('LOOKAHEAD_HOURS', '720'))
CLINIC_NUMS     = [int(x) for x in os.environ.get('CLINIC_NUMS', '').split(',') if x.strip().isdigit()]

# America/Chicago timezone for GHL (GMT-6 CST / GMT-5 CDT)
GHL_TIMEZONE = ZoneInfo('America/Chicago')

CLINIC_OPERATORY_FILTERS: Dict[int, List[int]] = {
    9034: [11579, 11580, 11588],
    9035: [11574, 11576, 11577],
}

# Operatory-specific appointment type filters (using appointment type names)
OPERATORY_APPOINTMENT_TYPE_FILTERS: Dict[int, List[str]] = {
    11588: ["COMP EX", "COMP EX CHILD"],  # Clinic 9034, Operatory 11588
    11574: ["Cash Consult", "Insurance Consult"]  # Clinic 9035, Operatory 11574
}

VALID_STATUSES = {'Scheduled', 'Complete', 'Broken'}

# Global cache for appointment types
_appointment_types_cache: Optional[Dict[int, str]] = None

# === LOGGING ===
logging.basicConfig(
    level=getattr(logging, LOG_LEVEL),
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[logging.StreamHandler(sys.stdout)]
)
logger = logging.getLogger('opendental_sync')

# === APPOINTMENT TYPES CACHE ===
def get_appointment_types() -> Dict[int, str]:
    """Fetch and cache appointment types mapping from AppointmentTypeNum to AppointmentTypeName"""
    global _appointment_types_cache
    
    if _appointment_types_cache is not None:
        return _appointment_types_cache
    
    try:
        logger.info("Fetching appointment types from OpenDental API...")
        r = requests.get(f"{API_BASE_URL}/appointmenttypes", headers=make_auth_header(), timeout=30)
        r.raise_for_status()
        data = r.json()
        
        # Handle both single object and list responses
        appt_types = data if isinstance(data, list) else [data]
        
        # Create mapping from AppointmentTypeNum to AppointmentTypeName
        _appointment_types_cache = {}
        for apt_type in appt_types:
            type_num = apt_type.get('AppointmentTypeNum')
            type_name = apt_type.get('AppointmentTypeName', '')
            if type_num is not None:
                _appointment_types_cache[type_num] = type_name
        
        logger.info(f"Loaded {len(_appointment_types_cache)} appointment types")
        logger.debug(f"Appointment types mapping: {_appointment_types_cache}")
        
        return _appointment_types_cache
        
    except Exception as e:
        logger.error(f"Failed to fetch appointment types: {e}")
        # Return empty dict as fallback
        _appointment_types_cache = {}
        return _appointment_types_cache

def get_appointment_type_name(appointment: Dict[str, Any]) -> str:
    """Get the appointment type name for an appointment"""
    # First try to get from cached appointment types using AppointmentTypeNum
    apt_type_num = appointment.get('AppointmentTypeNum')
    if apt_type_num is not None:
        appointment_types = get_appointment_types()
        apt_type_name = appointment_types.get(apt_type_num, '')
        if apt_type_name:
            logger.debug(f"Found appointment type via AppointmentTypeNum {apt_type_num}: '{apt_type_name}'")
            return apt_type_name
    
    # Fallback to direct fields (though these seem to be empty in your case)
    apt_type_direct = appointment.get('AptType', '') or appointment.get('AppointmentType', '')
    if apt_type_direct:
        logger.debug(f"Found appointment type via direct field: '{apt_type_direct}'")
        return apt_type_direct
    
    logger.debug(f"No appointment type found for appointment {appointment.get('AptNum')}")
    return ''

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

# === APPOINTMENT FETCHING ===
def get_filtered_ops(clinic: int) -> List[int]:
    return CLINIC_OPERATORY_FILTERS.get(clinic, [])

def fetch_appointments(
    clinic: int,
    since: Optional[datetime.datetime],
    ops: List[int]
) -> List[Dict[str, Any]]:
    now_utc = datetime.datetime.utcnow().replace(tzinfo=timezone.utc)
    params = {
        'ClinicNum': clinic,
        'dateStart': now_utc.strftime('%Y-%m-%d'),
        'dateEnd': (now_utc + datetime.timedelta(hours=LOOKAHEAD_HOURS)).strftime('%Y-%m-%d')
    }
    
    if since:
        # Ensure timezone info
        if since.tzinfo is None:
            since = since.replace(tzinfo=timezone.utc)
        eff_utc = (since + datetime.timedelta(seconds=1)).astimezone(timezone.utc)
        params['DateTStamp'] = eff_utc.strftime('%Y-%m-%d %H:%M:%S')
        logger.info(f"Clinic {clinic}: fetching appointments modified after UTC {params['DateTStamp']}")
    else:
        logger.info(f"Clinic {clinic}: fetching all appointments (first run)")
    
    logger.debug(f"→ GET /appointments with params: {params}")
    
    try:
        r = requests.get(f"{API_BASE_URL}/appointments",
                         headers=make_auth_header(), params=params, timeout=30)
        r.raise_for_status()
        data = r.json()
        appts = data if isinstance(data, list) else [data]
        logger.debug(f"← Raw API response: {len(appts)} appointments")
    except Exception as e:
        logger.error(f"Error fetching appointments for clinic {clinic}: {e}")
        return []

    # Filter appointments
    valid = []
    for a in appts:
        apt_num = str(a.get('AptNum', ''))
        status = a.get('AptStatus', '')
        op_num = a.get('Op') or a.get('OperatoryNum')
        
        # Check status
        if status not in VALID_STATUSES:
            logger.debug(f"Skipping apt {apt_num}: invalid status '{status}'")
            continue
            
        # Check operatory filter
        if ops and op_num not in ops:
            logger.debug(f"Skipping apt {apt_num}: operatory {op_num} not in filter {ops}")
            continue
        
        # IMPROVED: Check appointment type filter for specific operatories
        if op_num in OPERATORY_APPOINTMENT_TYPE_FILTERS:
            allowed_types = OPERATORY_APPOINTMENT_TYPE_FILTERS[op_num]
            apt_type_name = get_appointment_type_name(a)
            
            if apt_type_name not in allowed_types:
                logger.debug(f"Skipping apt {apt_num}: appointment type '{apt_type_name}' not in allowed types {allowed_types} for operatory {op_num}")
                continue
            else:
                logger.info(f"✓ Apt {apt_num}: appointment type '{apt_type_name}' matches filter for operatory {op_num}")
        
        valid.append(a)
    
    logger.info(f"Clinic {clinic}: {len(valid)} valid appointment(s) found")
    if valid:
        logger.debug(f"Valid appointment IDs: {[a.get('AptNum') for a in valid]}")
    
    return valid

# === PATIENT ===
def get_patient_details(pat_num: int) -> Dict[str, Any]:
    if not pat_num:
        return {}
    try:
        logger.debug(f"Fetching patient details for PatNum {pat_num}")
        r = requests.get(f"{API_BASE_URL}/patients/{pat_num}", headers=make_auth_header(), timeout=15)
        r.raise_for_status()
        return r.json()
    except Exception as e:
        logger.warning(f"Failed to fetch patient {pat_num}: {e}")
        return {}

# === FIXED SYNC FUNCTION - TIME MATCHING LOGIC ===
def send_to_keragon(appt: Dict[str, Any], clinic: int, dry_run: bool = False) -> bool:
    apt_num = str(appt.get('AptNum', ''))
    
    # Get patient details
    patient = get_patient_details(appt.get('PatNum'))
    first = patient.get('FName') or appt.get('FName', '')
    last = patient.get('LName') or appt.get('LName', '')
    name = f"{first} {last}".strip() or 'Unknown'
    provider_name = 'Dr. Gharbi' if clinic == 9034 else 'Dr. Ensley'

    # FIXED: Parse time from OpenDental (comes as UTC) and preserve the exact time values
    st_utc = parse_time(appt.get('AptDateTime'))
    if not st_utc:
        logger.error(f"Invalid start time for appointment {apt_num}")
        return False
    
    logger.debug(f"Appointment {apt_num} - Raw time from OpenDental: {appt.get('AptDateTime')}")
    logger.debug(f"Appointment {apt_num} - Parsed UTC time: {st_utc}")
    
    # KEY FIX: Extract the time components from UTC and recreate in GHL timezone
    # This ensures 8 AM UTC from OpenDental becomes 8 AM in GHL timezone
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
    logger.info(f"  Payload: {st_payload} to {en_payload}")

    payload = {
        'appointmentId': apt_num,
        'appointmentTime': st_payload,
        'appointmentEndTime': en_payload,
        'appointmentDurationMinutes': duration_minutes,
        'status': appt.get('AptStatus'),
        'notes': appt.get('Note', '') + ' [fromOD]',
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
        r = requests.post(KERAGON_WEBHOOK_URL, json=payload, timeout=30)
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

# === MAIN SYNC LOGIC ===
def run_sync(dry_run: bool = False):
    if not (DEVELOPER_KEY and CUSTOMER_KEY and KERAGON_WEBHOOK_URL and CLINIC_NUMS):
        logger.critical("Missing configuration – check your environment variables")
        logger.critical("Required: OPEN_DENTAL_DEVELOPER_KEY, OPEN_DENTAL_CUSTOMER_KEY, KERAGON_WEBHOOK_URL, CLINIC_NUMS")
        sys.exit(1)

    logger.info("=== Starting OpenDental → Keragon Sync ===")
    logger.info("Time values will match exactly: 8 AM OpenDental → 8 AM GHL")
    if dry_run:
        logger.info("DRY RUN MODE: No data will be sent to Keragon")

    # Load appointment types cache first
    logger.info("Loading appointment types...")
    get_appointment_types()

    # Load state
    last_syncs = load_last_sync_times()
    sent_appointments = load_sent_appointments()
    new_syncs = last_syncs.copy()  # Start with existing sync times
    
    total_sent = 0
    total_skipped = 0
    total_processed = 0
    
    # Track if we need to save state (only if something actually changed)
    state_changed = False

    for clinic in CLINIC_NUMS:
        logger.info(f"--- Processing Clinic {clinic} ---")
        
        # Fetch appointments
        appointments = fetch_appointments(clinic, last_syncs.get(clinic), get_filtered_ops(clinic))
        
        # Only update sync time if we actually process appointments
        if not appointments:
            logger.info(f"Clinic {clinic}: No appointments to process - sync time unchanged")
            continue
            
        clinic_sent = 0
        clinic_skipped = 0
        clinic_processed = 0
        
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
                if not dry_run:
                    sent_appointments.add(apt_num)
                    state_changed = True
            else:
                logger.warning(f"Failed to send appointment {apt_num}")
                # Continue processing other appointments instead of stopping

        # Only update sync time if we actually processed appointments
        if clinic_processed > 0:
            if latest_processed_timestamp and validate_sync_time_update(clinic, last_syncs.get(clinic), latest_processed_timestamp):
                new_syncs[clinic] = latest_processed_timestamp
                state_changed = True
                logger.info(f"Clinic {clinic}: Processed {clinic_processed} appointments, updated sync time to {latest_processed_timestamp}")
            else:
                logger.warning(f"Clinic {clinic}: Processed {clinic_processed} appointments but could not determine valid sync time - keeping previous sync time")
        
        logger.info(f"Clinic {clinic}: ✓ Sent {clinic_sent}, Skipped {clinic_skipped}, Total processed {clinic_processed}")

        total_sent += clinic_sent
        total_skipped += clinic_skipped  
        total_processed += clinic_processed

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
    
    logger.info("=== Sync Complete ===")
    logger.info(f"Total sent: {total_sent}")
    logger.info(f"Total skipped: {total_skipped}")
    logger.info(f"Total processed: {total_processed}")
    logger.info(f"Total tracked appointments: {len(sent_appointments)}")

def cleanup_old_sent_appointments(days_to_keep: int = 30):
    """Remove old appointment IDs from the sent appointments file"""
    logger.info(f"Note: Sent appointments file contains {len(load_sent_appointments())} appointment IDs")
    logger.info("Consider implementing cleanup logic if this file grows too large")

if __name__ == '__main__':
    import argparse
    parser = argparse.ArgumentParser(description='Sync OpenDental → Keragon')
    parser.add_argument('--reset', action='store_true', help='Clear saved sync timestamps and sent appointments')
    parser.add_argument('--reset-sent', action='store_true', help='Clear only sent appointments tracking')
    parser.add_argument('--verbose', action='store_true', help='Enable DEBUG logging')
    parser.add_argument('--dry-run', action='store_true', help='Show what would be sent without actually sending')
    parser.add_argument('--show-appt-types', action='store_true', help='Display all appointment types and exit')
    args = parser.parse_args()

    if args.verbose:
        logger.setLevel(logging.DEBUG)
        logger.debug("Debug logging enabled")

    if args.show_appt_types:
        if not (DEVELOPER_KEY and CUSTOMER_KEY):
            logger.critical("Missing API credentials - check OPEN_DENTAL_DEVELOPER_KEY and OPEN_DENTAL_CUSTOMER_KEY")
            sys.exit(1)
        appt_types = get_appointment_types()
        print("=== Available Appointment Types ===")
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
