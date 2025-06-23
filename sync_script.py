#!/usr/bin/env python3
import os
import sys
import json
import logging
import datetime
import requests
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
VALID_STATUSES = {'Scheduled', 'Complete', 'Broken'}

# === LOGGING ===
logging.basicConfig(
    level=getattr(logging, LOG_LEVEL),
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[logging.StreamHandler(sys.stdout)]
)
logger = logging.getLogger('opendental_sync')

# === STATE MANAGEMENT ===
def load_last_sync_times() -> Dict[int, Optional[datetime.datetime]]:
    state: Dict[int, Optional[datetime.datetime]] = {}
    if os.path.exists(STATE_FILE):
        try:
            with open(STATE_FILE) as f:
                data = json.load(f)
            for c in CLINIC_NUMS:
                ts = data.get(str(c))
                state[c] = datetime.datetime.fromisoformat(ts) if ts else None
        except Exception as e:
            logger.error(f"Error reading state file: {e}")
            state = {c: None for c in CLINIC_NUMS}
    else:
        state = {c: None for c in CLINIC_NUMS}
    return state

def save_last_sync_times(times: Dict[int, Optional[datetime.datetime]]) -> None:
    out = {str(c): dt.isoformat() if dt else None for c, dt in times.items()}
    try:
        with open(STATE_FILE, 'w') as f:
            json.dump(out, f, indent=2)
        logger.debug(f"Saved sync times: {out}")
    except Exception as e:
        logger.error(f"Error saving state file: {e}")

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

def save_sent_appointments(sent_ids: Set[str]) -> None:
    """Save set of sent appointment IDs"""
    try:
        with open(SENT_APPTS_FILE, 'w') as f:
            json.dump(list(sent_ids), f, indent=2)
        logger.debug(f"Saved {len(sent_ids)} sent appointment IDs")
    except Exception as e:
        logger.error(f"Error saving sent appointments: {e}")

# === UTILITIES ===
def parse_time(s: Optional[str]) -> Optional[datetime.datetime]:
    if not s:
        return None
    try:
        if s.endswith('Z'):
            s = s[:-1] + '+00:00'
        return datetime.datetime.fromisoformat(s)
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

# === SYNC ===
def send_to_keragon(appt: Dict[str, Any], clinic: int, dry_run: bool = False) -> bool:
    apt_num = str(appt.get('AptNum', ''))
    
    # Get patient details
    patient = get_patient_details(appt.get('PatNum'))
    first = patient.get('FName') or appt.get('FName', '')
    last = patient.get('LName') or appt.get('LName', '')
    name = f"{first} {last}".strip() or 'Unknown'
    provider_name = 'Dr. Gharbi' if clinic == 9034 else 'Dr. Ensley'

    # Parse raw time and assign America/Chicago timezone (assuming OpenDental uses local time)
    st = parse_time(appt.get('AptDateTime'))
    if not st:
        logger.error(f"Invalid start time for appointment {apt_num}")
        return False
    
    # If no timezone info, assume it's in Chicago time (OpenDental local time)
    if st.tzinfo is None:
        st = st.replace(tzinfo=ZoneInfo('America/Chicago'))

    # Calculate end time in the same timezone
    en = calculate_end_time(st, appt.get('Pattern', ''))
    if en.tzinfo is None:
        en = en.replace(tzinfo=ZoneInfo('America/Chicago'))

    # Convert to GHL timezone (America/Chicago)
    st_ghl = st.astimezone(GHL_TIMEZONE)
    en_ghl = en.astimezone(GHL_TIMEZONE)

    # Format for payload (ISO with timezone offset)
    st_payload = st_ghl.isoformat(timespec='seconds')
    en_payload = en_ghl.isoformat(timespec='seconds')

    logger.info(f"Appointment {apt_num} for {name} ({provider_name})")
    logger.info(f"  Time: {st_payload} to {en_payload} (Chicago time)")
    logger.debug(f"  Original time: {st} (Chicago) → {st_ghl} (Chicago)")

    payload = {
        'appointmentId': apt_num,
        'appointmentTime': st_payload,
        'appointmentEndTime': en_payload,
        'appointmentDurationMinutes': int((en - st).total_seconds() / 60),
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

# === MAIN ===
def run_sync(dry_run: bool = False):
    if not (DEVELOPER_KEY and CUSTOMER_KEY and KERAGON_WEBHOOK_URL and CLINIC_NUMS):
        logger.critical("Missing configuration – check your environment variables")
        logger.critical("Required: OPEN_DENTAL_DEVELOPER_KEY, OPEN_DENTAL_CUSTOMER_KEY, KERAGON_WEBHOOK_URL, CLINIC_NUMS")
        sys.exit(1)

    logger.info("=== Starting OpenDental → Keragon Sync ===")
    if dry_run:
        logger.info("DRY RUN MODE: No data will be sent to Keragon")

    # Load state
    last_syncs = load_last_sync_times()
    sent_appointments = load_sent_appointments()
    new_syncs = last_syncs.copy()  # Start with existing sync times
    
    total_sent = 0
    total_skipped = 0

    for clinic in CLINIC_NUMS:
        logger.info(f"--- Processing Clinic {clinic} ---")
        
        # Fetch appointments
        appointments = fetch_appointments(clinic, last_syncs.get(clinic), get_filtered_ops(clinic))
        
        if not appointments:
            logger.info(f"Clinic {clinic}: No appointments to process")
            continue

        clinic_sent = 0
        clinic_skipped = 0
        latest_timestamp = last_syncs.get(clinic)

        for appt in appointments:
            apt_num = str(appt.get('AptNum', ''))
            
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
                
                # Update latest timestamp based on DateTStamp or current time
           appt_timestamp = parse_time(appt.get('DateTStamp'))
           if appt_timestamp:
              if latest_timestamp and latest_timestamp.tzinfo is None:
                latest_timestamp = latest_timestamp.replace(tzinfo=timezone.utc)
              if not latest_timestamp or appt_timestamp > latest_timestamp:
                latest_timestamp = appt_timestamp
                logger.debug(f"Updated latest timestamp to {latest_timestamp}")

            else:
                logger.warning(f"Failed to send appointment {apt_num}")

        # Update sync time only if we successfully sent appointments
        if clinic_sent > 0:
            new_syncs[clinic] = latest_timestamp or datetime.datetime.now(timezone.utc)
            logger.info(f"Clinic {clinic}: ✓ Sent {clinic_sent} appointments, updated sync time")
        else:
            logger.info(f"Clinic {clinic}: No new appointments sent, keeping previous sync time")

        if clinic_skipped > 0:
            logger.info(f"Clinic {clinic}: Skipped {clinic_skipped} already-sent appointments")

        total_sent += clinic_sent
        total_skipped += clinic_skipped

    # Save state
    if not dry_run:
        save_last_sync_times(new_syncs)
        save_sent_appointments(sent_appointments)
    
    logger.info("=== Sync Complete ===")
    logger.info(f"Total sent: {total_sent}")
    logger.info(f"Total skipped: {total_skipped}")
    logger.info(f"Total tracked appointments: {len(sent_appointments)}")

def cleanup_old_sent_appointments(days_to_keep: int = 30):
    """Remove old appointment IDs from the sent appointments file"""
    # This is a simple cleanup - you might want to implement more sophisticated logic
    # based on actual appointment dates rather than just tracking IDs
    logger.info(f"Note: Sent appointments file contains {len(load_sent_appointments())} appointment IDs")
    logger.info("Consider implementing cleanup logic if this file grows too large")

if __name__ == '__main__':
    import argparse
    parser = argparse.ArgumentParser(description='Sync OpenDental → Keragon')
    parser.add_argument('--reset', action='store_true', help='Clear saved sync timestamps and sent appointments')
    parser.add_argument('--reset-sent', action='store_true', help='Clear only sent appointments tracking')
    parser.add_argument('--verbose', action='store_true', help='Enable DEBUG logging')
    parser.add_argument('--dry-run', action='store_true', help='Show what would be sent without actually sending')
    args = parser.parse_args()

    if args.verbose:
        logger.setLevel(logging.DEBUG)
        logger.debug("Debug logging enabled")

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
