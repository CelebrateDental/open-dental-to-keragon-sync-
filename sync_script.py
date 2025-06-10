#!/usr/bin/env python3
import os
import sys
import json
import logging
import datetime
import requests
from zoneinfo import ZoneInfo
from typing import List, Dict, Any, Optional
from requests.exceptions import HTTPError, RequestException

# === CONFIGURATION ===
API_BASE_URL        = os.environ.get('OPEN_DENTAL_API_URL', 'https://api.opendental.com/api/v1')
DEVELOPER_KEY       = os.environ.get('OPEN_DENTAL_DEVELOPER_KEY')
CUSTOMER_KEY        = os.environ.get('OPEN_DENTAL_CUSTOMER_KEY')
KERAGON_WEBHOOK_URL = os.environ.get('KERAGON_WEBHOOK_URL')

STATE_FILE = 'last_sync_state.json'
LOG_LEVEL           = os.environ.get('LOG_LEVEL', 'INFO')
OVERLAP_MINUTES     = int(os.environ.get('OVERLAP_MINUTES', '5'))
LOOKAHEAD_HOURS     = int(os.environ.get('LOOKAHEAD_HOURS', '720'))

CLINIC_NUMS = [
    int(x) for x in os.environ.get('CLINIC_NUMS', '').split(',')
    if x.strip().isdigit()
]

CLINIC_OPERATORY_FILTERS: Dict[int, List[int]] = {
    9034: [11579, 11580, 11588],
    9035: [11574, 11576, 11577],
}

# Valid appointment statuses to sync
VALID_STATUSES = {'Scheduled', 'Complete', 'Broken'}

logging.basicConfig(
    level=getattr(logging, LOG_LEVEL),
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[logging.StreamHandler(sys.stdout)]
)
logger = logging.getLogger('opendental_sync')


def load_last_sync_state() -> Dict[int, Dict[str, Any]]:
    if os.path.exists(STATE_FILE):
        try:
            with open(STATE_FILE, 'r') as f:
                data = json.load(f)
            result: Dict[int, Dict[str, Any]] = {}
            for k, v in data.items():
                cnum = int(k)
                last_sync_str = v.get('lastSync')
                did_full = v.get('didFullFetch', False)
                last_sync_dt = datetime.datetime.fromisoformat(last_sync_str) if last_sync_str else None
                result[cnum] = {'lastSync': last_sync_dt, 'didFullFetch': bool(did_full)}
            for c in CLINIC_NUMS:
                if c not in result:
                    result[c] = {'lastSync': None, 'didFullFetch': False}
            return result
        except Exception as e:
            logger.error(f"Error reading state file: {e}")
    return {c: {'lastSync': None, 'didFullFetch': False} for c in CLINIC_NUMS}


def save_last_sync_state(state: Dict[int, Dict[str, Any]]) -> None:
    try:
        serial: Dict[str, Any] = {}
        for k, v in state.items():
            last_str = v['lastSync'].isoformat() if v['lastSync'] else None
            serial[str(k)] = {'lastSync': last_str, 'didFullFetch': v['didFullFetch']}
        with open(STATE_FILE, 'w') as f:
            json.dump(serial, f, indent=2)
    except Exception as e:
        logger.error(f"Error saving state file: {e}")


def parse_time(ts: Optional[str]) -> Optional[datetime.datetime]:
    if not ts:
        return None
    for fmt in ("%Y-%m-%dT%H:%M:%S", "%Y-%m-%d %H:%M:%S", "%Y-%m-%d"):
        try:
            return datetime.datetime.strptime(ts, fmt)
        except ValueError:
            continue
    logger.warning(f"Unrecognized time format: {ts}")
    return None


def convert_to_timezone_aware(dt: datetime.datetime, timezone_str: str = "America/Chicago") -> datetime.datetime:
    """Convert naive datetime to timezone-aware datetime"""
    if dt is None:
        return None
    
    if dt.tzinfo is not None:
        # Already timezone aware, convert to target timezone
        return dt.astimezone(ZoneInfo(timezone_str))
    
    # Assume the naive datetime is in the target timezone
    target_tz = ZoneInfo(timezone_str)
    return dt.replace(tzinfo=target_tz)


def calculate_pattern_duration(pattern: str, pattern_secondary: str = None, minutes_per_slot: int = 10) -> int:
    """
    Calculate appointment duration from pattern strings.
    In OpenDental, each character in the pattern represents a time slot (usually 10 minutes).
    'X' means the appointment occupies that slot.
    Returns duration in minutes.
    """
    if not pattern:
        return 0
    
    # Count all X characters in primary pattern
    x_count = pattern.upper().count('X')
    
    # Add X characters from secondary pattern if it exists
    if pattern_secondary:
        x_count += pattern_secondary.upper().count('X')
    
    # If no X found, use total pattern length as fallback
    if x_count == 0:
        total_length = len(pattern)
        if pattern_secondary:
            total_length += len(pattern_secondary)
        return total_length * minutes_per_slot
    
    # Each X represents one time slot
    duration_minutes = x_count * minutes_per_slot
    
    logger.debug(f"Pattern '{pattern}' + '{pattern_secondary or ''}' has {x_count} X chars = {duration_minutes} minutes")
    return duration_minutes


def get_appointment_duration_from_api(appt_id: str) -> Optional[int]:
    """Fetch full appointment details to get accurate duration"""
    if not appt_id:
        return None
    
    endpoint = f"{API_BASE_URL}/appointments/{appt_id}"
    headers = make_auth_header()
    
    try:
        resp = requests.get(endpoint, headers=headers, timeout=15)
        resp.raise_for_status()
        appt_detail = resp.json()
        
        # Try to get duration from various fields (in order of preference)
        duration_sources = [
            'Length',           # Total length in minutes
            'Minutes',          # Duration in minutes  
            'PatternLength',    # Pattern-based length
            'TotalTime',        # Total time
            'LengthTime'        # Sometimes used for appointment length
        ]
        
        for field in duration_sources:
            if field in appt_detail and appt_detail[field]:
                try:
                    duration = int(appt_detail[field])
                    if duration > 0:
                        logger.debug(f"Got duration from API field '{field}': {duration} minutes for appointment {appt_id}")
                        return duration
                except (ValueError, TypeError):
                    continue
        
        # Try to calculate from start/end times if available
        start_time = appt_detail.get('AptDateTime')
        end_time = appt_detail.get('AptDateTimeEnd') or appt_detail.get('EndTime')
        
        if start_time and end_time:
            start_dt = parse_time(start_time)
            end_dt = parse_time(end_time)
            if start_dt and end_dt:
                duration = int((end_dt - start_dt).total_seconds() / 60)
                if duration > 0:
                    logger.debug(f"Calculated duration from start/end times: {duration} minutes for appointment {appt_id}")
                    return duration
        
        # Use pattern calculation as last resort
        pattern = appt_detail.get('Pattern', '')
        pattern_secondary = appt_detail.get('PatternSecondary', '')
        
        if pattern:
            duration = calculate_pattern_duration(pattern, pattern_secondary)
            logger.debug(f"Calculated duration from patterns for appointment {appt_id}: '{pattern}' + '{pattern_secondary}' = {duration} minutes")
            return duration
            
        logger.warning(f"No duration source found for appointment {appt_id}")
            
    except Exception as e:
        logger.warning(f"Could not fetch detailed appointment data for {appt_id}: {e}")
    
    return None


def calculate_end_time(start_dt: datetime.datetime, pattern: str, appt_data: Dict[str, Any] = None) -> Optional[datetime.datetime]:
    """Calculate end time properly handling timezone-aware datetime"""
    if not start_dt:
        return None
    
    duration_minutes = 0
    appt_id = appt_data.get('AptNum') if appt_data else None
    
    # Method 1: Try to get accurate duration from API
    if appt_id:
        api_duration = get_appointment_duration_from_api(str(appt_id))
        if api_duration and api_duration > 0:
            duration_minutes = api_duration
            logger.debug(f"Using API duration: {duration_minutes} minutes")
    
    # Method 2: Check local appointment data for duration fields
    if duration_minutes <= 0 and appt_data:
        duration_sources = ['Length', 'Minutes', 'PatternLength', 'TotalTime']
        for field in duration_sources:
            if field in appt_data and appt_data[field]:
                try:
                    duration_minutes = int(appt_data[field])
                    if duration_minutes > 0:
                        logger.debug(f"Using local field '{field}': {duration_minutes} minutes")
                        break
                except (ValueError, TypeError):
                    continue
    
    # Method 3: Try direct end time calculation
    if duration_minutes <= 0 and appt_data:
        raw_end = appt_data.get('AptDateTimeEnd') or appt_data.get('EndTime')
        if raw_end:
            end_dt = parse_time(raw_end)
            if end_dt:
                end_aware = convert_to_timezone_aware(end_dt, "America/Chicago")
                logger.debug(f"Using direct end time: {end_aware.isoformat()}")
                return end_aware
    
    # Method 4: Calculate from patterns
    if duration_minutes <= 0:
        pattern_secondary = appt_data.get('PatternSecondary', '') if appt_data else ''
        duration_minutes = calculate_pattern_duration(pattern, pattern_secondary)
        if duration_minutes > 0:
            logger.debug(f"Using pattern calculation: {duration_minutes} minutes")
    
    # Default fallback
    if duration_minutes <= 0:
        duration_minutes = 60  # Default to 1 hour
        logger.warning(f"No duration found, defaulting to {duration_minutes} minutes")
    
    # Add duration to the timezone-aware datetime
    end_dt = start_dt + datetime.timedelta(minutes=duration_minutes)
    logger.debug(f"Final calculated end time: {end_dt.isoformat()} (added {duration_minutes} min)")
    return end_dt


def make_auth_header() -> Dict[str, str]:
    return {'Authorization': f'ODFHIR {DEVELOPER_KEY}/{CUSTOMER_KEY}', 'Content-Type': 'application/json'}


def list_operatories_for_clinic(clinic: int) -> List[Dict[str, Any]]:
    endpoint = f"{API_BASE_URL}/operatories"
    headers = make_auth_header()
    try:
        resp = requests.get(endpoint, headers=headers, params={'ClinicNum': clinic}, timeout=15)
        resp.raise_for_status()
        ops = resp.json()
        logger.info(f"Operatories for clinic {clinic}:")
        for o in ops:
            logger.info(f"  OperatoryNum={o.get('OperatoryNum')} | OpName={o.get('OpName','')}")
        return ops
    except Exception as e:
        logger.error(f"Failed to list operatories for clinic {clinic}: {e}")
        return []


def get_filtered_operatories_for_clinic(clinic: int) -> List[int]:
    filt = CLINIC_OPERATORY_FILTERS.get(clinic, [])
    if filt:
        logger.info(f"Clinic {clinic}: filtering to ops {filt}")
    else:
        logger.info(f"Clinic {clinic}: no operatory filter, will fetch all")
    return filt


def fetch_appointments(clinic: int, since: Optional[datetime.datetime], filtered_ops: List[int] = None) -> List[Dict[str, Any]]:
    """
    Fetch appointments using corrected API parameters that match the documentation examples.
    Post-processes results to filter by status and operatory.
    """
    endpoint = f"{API_BASE_URL}/appointments"
    headers = make_auth_header()
    now = datetime.datetime.utcnow()
    date_start = now.strftime("%Y-%m-%d")
    date_end = (now + datetime.timedelta(hours=LOOKAHEAD_HOURS)).strftime("%Y-%m-%d")
    
    # Base parameters that match the documentation examples
    params = {
        'dateStart': date_start,
        'dateEnd': date_end,
        'ClinicNum': clinic
    }
    
    # Add DateTStamp for incremental sync if we have a last sync time
    if since:
        # Format as YYYY-MM-DD HH:MM:SS for the API (NOT just YYYY-MM-DD)
        params['DateTStamp'] = since.strftime("%Y-%m-%d %H:%M:%S")
        logger.info(f"Fetching appointments since {params['DateTStamp']}")
    
    try:
        logger.info(f"Fetching appointments for clinic {clinic} from {date_start} to {date_end}")
        resp = requests.get(endpoint, headers=headers, params=params, timeout=30)
        resp.raise_for_status()
        
        appointments = resp.json()
        if not isinstance(appointments, list):
            logger.warning(f"Unexpected response format: {type(appointments)}")
            return []
        
        logger.info(f"Raw fetched for clinic {clinic}: {len(appointments)} records")
        
        # Post-process filtering
        filtered_appointments = []
        
        for appt in appointments:
            # Filter by status
            status = appt.get('AptStatus', '')
            if status not in VALID_STATUSES:
                continue
            
            # Filter by operatory if specified
            if filtered_ops:
                op_num = appt.get('Op') or appt.get('OperatoryNum')
                if op_num not in filtered_ops:
                    continue
            
            filtered_appointments.append(appt)
        
        if filtered_ops:
            logger.info(f"After operatory filtering: {len(filtered_appointments)} records")
        
        logger.info(f"After status filtering: {len(filtered_appointments)} records with valid statuses")
        return filtered_appointments
        
    except HTTPError as e:
        if e.response.status_code == 400:
            logger.error(f"Bad request (400) - check API parameters: {e.response.text}")
        elif e.response.status_code == 401:
            logger.error(f"Authentication failed (401) - check API keys")
        elif e.response.status_code == 404:
            logger.error(f"Endpoint not found (404) - check API URL")
        else:
            logger.error(f"HTTP error {e.response.status_code}: {e.response.text}")
        return []
    except RequestException as e:
        logger.error(f"Request failed: {e}")
        return []
    except Exception as e:
        logger.error(f"Unexpected error fetching appointments: {e}")
        return []


def filter_new_appointments(appts: List[Dict[str, Any]], since: Optional[datetime.datetime]) -> List[Dict[str, Any]]:
    if since is None:
        return []
    new_list: List[Dict[str, Any]] = []
    for a in appts:
        mod = parse_time(a.get('DateTStamp'))
        if mod and mod > since:
            new_list.append(a)
    logger.info(f"Kept {len(new_list)} new/updated appointments since {since.isoformat()}")
    return new_list


def get_patient_details(pat_num: int) -> Dict[str, Any]:
    if not pat_num:
        return {}
    endpoint = f"{API_BASE_URL}/patients/{pat_num}"
    headers = make_auth_header()
    try:
        resp = requests.get(endpoint, headers=headers, timeout=15)
        resp.raise_for_status()
        return resp.json()
    except Exception:
        logger.warning(f"Primary fetch failed for PatNum {pat_num}")
    try:
        resp = requests.get(f"{API_BASE_URL}/patients", headers=headers, params={'PatNum': pat_num}, timeout=15)
        resp.raise_for_status()
        data = resp.json()
        if isinstance(data, list) and data:
            return data[0]
        if isinstance(data, dict):
            return data
    except Exception as e:
        logger.error(f"Fallback patient fetch failed: {e}")
    return {}


def send_to_keragon(appt: Dict[str, Any]) -> bool:
    apt_id = str(appt.get('AptNum', ''))
    original_note = appt.get('Note', '') or ""
    now = datetime.datetime.utcnow()

    # 1) Strip the GHL tag if present
    if "[fromGHL]" in original_note:
        cleaned = original_note.replace("[fromGHL]", "").strip()
        try:
            patch_url = f"{API_BASE_URL}/appointments/{apt_id}"
            resp = requests.patch(
                patch_url,
                headers=make_auth_header(),
                json={"Note": cleaned},
                timeout=30
            )
            resp.raise_for_status()
            logger.info(f"Stripped [fromGHL] from AptNum={apt_id}")
        except Exception as e:
            logger.error(f"Failed stripping [fromGHL] on AptNum={apt_id}: {e}")
        return True

    # 2) Build payload with corrected time handling
    raw_start = appt.get('AptDateTime')
    start_dt = parse_time(raw_start)
    
    # Convert to timezone-aware datetime FIRST
    if start_dt:
        aware_start = convert_to_timezone_aware(start_dt, "America/Chicago")
        iso_start = aware_start.isoformat()
        
        # Calculate end time using the timezone-aware start time
        pattern = appt.get('Pattern', '')
        aware_end = calculate_end_time(aware_start, pattern, appt)
        iso_end = aware_end.isoformat() if aware_end else None
        
        # Debug logging
        logger.debug(f"AptNum={apt_id} | Start: {iso_start} | End: {iso_end} | Pattern: '{pattern}'")
    else:
        iso_start = None
        iso_end = None
        logger.warning(f"AptNum={apt_id} | Could not parse start time: {raw_start}")

    tagged_note = f"{original_note} [fromOpenDental]"

    # Fetch patient details once
    patient = get_patient_details(appt.get('PatNum')) if appt.get('PatNum') else {}

    payload = {
        'firstName':         patient.get('FName', appt.get('FName', '')),
        'lastName':          patient.get('LName', appt.get('LName', '')),
        'email':             patient.get('Email', appt.get('Email', '')),
        'phone':             (patient.get('HmPhone')
                              or patient.get('WkPhone')
                              or patient.get('WirelessPhone')
                              or appt.get('HmPhone', '')),
        'appointmentTime':   iso_start or raw_start,
        'endTime':           iso_end,
        'locationId':        str(appt.get('ClinicNum', '')),
        'calendarId':        str(appt.get('Op', '') or appt.get('OperatoryNum', '')),
        'status':            appt.get('AptStatus', ''),
        'providerName':      appt.get('provAbbr', ''),
        'appointmentLength': appt.get('Pattern', ''),
        'notes':             tagged_note,
        'appointmentId':     apt_id,
        'patientId':         str(appt.get('PatNum', '')),
        # Additional patient fields:
        'birthdate':         patient.get('Birthdate', ''),
        'zipCode':           patient.get('Zip', ''),
        'state':             patient.get('State', ''),
        'city':              patient.get('City', ''),
        'gender':            patient.get('Gender', '')
    }

    try:
        r = requests.post(KERAGON_WEBHOOK_URL, json=payload, timeout=30)
        r.raise_for_status()
        logger.info(f"Sent AptNum={apt_id} to Keragon with [fromOpenDental]")
        return True
    except Exception as e:
        logger.error(f"Keragon send failed for AptNum={apt_id}: {e}")
        return False


def process_appointments(clinic: int, appts: List[Dict[str, Any]], lastSync: Optional[datetime.datetime], didFullFetch: bool) -> Dict[str, Any]:
    now = datetime.datetime.utcnow()
    max_mod = lastSync or now
    to_send = appts if not didFullFetch or lastSync is None else filter_new_appointments(appts, lastSync)
    
    if didFullFetch and not to_send:
        logger.info(f"Clinic {clinic}: no updates since {lastSync}")
        return {'newLastSync': lastSync, 'didFullFetch': True}
    
    sent_count = 0
    for a in to_send:
        # send_to_keragon handles strip vs append
        success = send_to_keragon(a)
        if success:
            sent_count += 1
            # advance mod time so we don't resend
            max_mod = now if now > max_mod else max_mod
    
    logger.info(f"Clinic {clinic}: processed {sent_count}/{len(to_send)} appointments")
    return {'newLastSync': max_mod, 'didFullFetch': True}


def run_sync():
    if not (DEVELOPER_KEY and CUSTOMER_KEY and KERAGON_WEBHOOK_URL):
        logger.critical("Missing API keys or webhook URL")
        sys.exit(1)
    if not CLINIC_NUMS:
        logger.critical("No CLINIC_NUMS defined")
        sys.exit(1)
    
    logger.info(f"Starting sync for clinics: {CLINIC_NUMS}")
    state = load_last_sync_state()
    new_state = {}
    
    for clinic in CLINIC_NUMS:
        logger.info(f"Processing clinic {clinic}")
        list_operatories_for_clinic(clinic)
        
        lastSync = state[clinic]['lastSync']
        didFullFetch = state[clinic]['didFullFetch']
        
        appts = fetch_appointments(clinic, lastSync, get_filtered_operatories_for_clinic(clinic))
        result = process_appointments(clinic, appts, lastSync, didFullFetch)
        
        new_state[clinic] = {
            'lastSync': result['newLastSync'], 
            'didFullFetch': result['didFullFetch']
        }
    
    save_last_sync_state(new_state)
    logger.info("Sync complete")


if __name__ == "__main__":
    import argparse
    parser = argparse.ArgumentParser(description='Sync Open Dental appointments to Keragon')
    parser.add_argument('--once', action='store_true', help='Run sync once and exit')
    parser.add_argument('--test', action='store_true', help='Test mode - validate config and exit')
    parser.add_argument('--dump-state', action='store_true', help='Display current state file')
    parser.add_argument('--reset', action='store_true', help='Reset state file')
    parser.add_argument('--verbose', action='store_true', help='Enable debug logging')
    args = parser.parse_args()

    if args.verbose:
        logger.setLevel(logging.DEBUG)
        
    if args.dump_state:
        if os.path.exists(STATE_FILE):
            print(json.dumps(json.load(open(STATE_FILE)), indent=2))
        else:
            print("No state file exists.")
        sys.exit(0)
        
    if args.reset and os.path.exists(STATE_FILE):
        os.remove(STATE_FILE)
        logger.info("State file removed.")
        sys.exit(0)
        
    if args.test:
        logger.info("Test mode - validating configuration")
        if not (DEVELOPER_KEY and CUSTOMER_KEY and KERAGON_WEBHOOK_URL):
            logger.error("Missing required environment variables")
            sys.exit(1)
        if not CLINIC_NUMS:
            logger.error("No CLINIC_NUMS configured")
            sys.exit(1)
        logger.info("Configuration appears valid")
        sys.exit(0)

    run_sync()
