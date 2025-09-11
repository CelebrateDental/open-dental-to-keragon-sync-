#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
OpenDental → GoHighLevel (LeadConnector) sync

What this does:
- Pull appointments from OpenDental for selected clinics & operatories.
- Ensure a GHL Contact with your required fields (phone-first lookup; if not found, create).
  Fields we send to GHL Contacts: firstName, lastName, dateOfBirth, email (validated/normalized),
  phone (normalized), address1, postalCode, gender, locationId, and a custom field "Clinic".
- Map clinic → calendarId and assignedUserId (9034/9035 strict routing).
- If AptNum already mapped to a GHL event → UPDATE that exact event (even if date/time changed).
- If AptNum not mapped:
    * If contact is NEW → CREATE new appointment immediately (no prior fetch).
    * If existing contact → fetch that contact’s appointments, filter to same clinic-local day
      **and** same calendar, pick the **latest by `dateUpdated`/`updatedAt`**, then UPDATE it;
      if none match, CREATE a new one.
- Appointment status mapping:
    Broken → cancelled
    Complete → showed
    Scheduled → confirmed
- Both CREATE and UPDATE set `ignoreFreeSlotValidation = true`.
- Persist mappings/state in MEGA:
    - `ghl_contacts_map.json`: PatNum → contactId (plus phone/email snapshot)
    - `ghl_appointments_map.json`: AptNum → { eventId, contactId, calendarId, clinic }
    - plus last_sync_state.json, sent_appointments.json, appointments_store.json

Requirements:
- Python 3.11+
- pip install: requests, mega.py
- Environment variables (see bottom).
"""

import os
import re
import sys
import json
import time
import copy
import shutil
import tempfile
import logging
import threading
import datetime
from typing import Any, Dict, List, Optional, Set, Tuple
from dataclasses import dataclass
from datetime import timezone, timedelta
from zoneinfo import ZoneInfo

import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

# =========================
# ====== CONFIG ===========
# =========================

# OpenDental API
API_BASE_URL = os.environ.get('OPEN_DENTAL_API_URL', 'https://api.opendental.com/api/v1').rstrip('/')
DEVELOPER_KEY = os.environ.get('OPEN_DENTAL_DEVELOPER_KEY', '').strip()
CUSTOMER_KEY = os.environ.get('OPEN_DENTAL_CUSTOMER_KEY', '').strip()

# GoHighLevel (LeadConnector) API
GHL_API_BASE = os.environ.get('GHL_API_BASE', 'https://services.leadconnectorhq.com').rstrip('/')
GHL_API_TOKEN = (
    os.environ.get('GHL_API_KEY')
    or os.environ.get('GHL_OAUTH_TOKEN')
    or os.environ.get('GHL_AUTH_TOKEN')
    or ''
).strip()
GHL_LOCATION_ID = os.environ.get('GHL_LOCATION_ID', '').strip()
GHL_API_VERSION = os.environ.get('GHL_API_VERSION', '2021-07-28')

# Clinic → calendarId (strict by clinic)
# Provide as JSON, e.g. {"clinic:9034":"cal_XXXX","clinic:9035":"cal_YYYY","default":"cal_DEFAULT"}
try:
    GHL_CALENDAR_MAP: Dict[str, str] = json.loads(os.environ.get('GHL_CALENDAR_MAP', '{}'))
except Exception:
    GHL_CALENDAR_MAP = {}

# Clinic → assignedUserId (strict by clinic)
# Provide as JSON, e.g. {"clinic:9034":"usr_XXXX","clinic:9035":"usr_YYYY"}
try:
    GHL_ASSIGNED_USER_MAP: Dict[str, str] = json.loads(os.environ.get('GHL_ASSIGNED_USER_MAP', '{}'))
except Exception:
    GHL_ASSIGNED_USER_MAP = {}

# Custom Field ID for "Clinic" on GHL Contacts (required to store clinic number)
# If missing, we will WARN and fall back to tagging (clinic:9034/9035).
GHL_CUSTOM_FIELD_CLINIC_ID = os.environ.get('GHL_CUSTOM_FIELD_CLINIC_ID', '').strip()

# MEGA cloud storage (maps & state)
MEGA_EMAIL = os.environ.get('MEGA_EMAIL', '').strip()
MEGA_PASSWORD = os.environ.get('MEGA_PASSWORD', '').strip()
MEGA_FOLDER = os.environ.get('MEGA_FOLDER', 'od_ghl_sync').strip()

# Local state files (also mirrored to MEGA)
STATE_FILE = 'last_sync_state.json'             # clinic → ISO timestamp
SENT_APPTS_FILE = 'sent_appointments.json'      # AptNum → { last_sent_tstamp }
APPT_SNAPSHOT_FILE = 'appointments_store.json'  # AptNum → last OD snapshot used
GHL_MAP_FILE = 'ghl_appointments_map.json'      # AptNum → { contactId, eventId, calendarId, clinic }
GHL_CONTACTS_MAP_FILE = 'ghl_contacts_map.json' # PatNum → { contactId, phone, email, updatedAt }

# Performance / retry / pagination
LOG_LEVEL = os.environ.get('LOG_LEVEL', 'INFO').upper()
REQUEST_TIMEOUT = int(os.environ.get('REQUEST_TIMEOUT', '120'))
RETRY_ATTEMPTS = int(os.environ.get('RETRY_ATTEMPTS', '5'))
BACKOFF_FACTOR = float(os.environ.get('BACKOFF_FACTOR', '3.0'))
ENABLE_PAGINATION = os.environ.get('ENABLE_PAGINATION', 'true').lower() == 'true'
PAGE_SIZE = int(os.environ.get('PAGE_SIZE', '100'))

# Sync windows
INCREMENTAL_SYNC_MINUTES = int(os.environ.get('INCREMENTAL_SYNC_MINUTES', '20160'))  # 14 days (lookahead)
DEEP_SYNC_HOURS = int(os.environ.get('DEEP_SYNC_HOURS', '720'))                      # 30 days (lookahead)
SAFETY_OVERLAP_HOURS = int(os.environ.get('SAFETY_OVERLAP_HOURS', '2'))

# Scheduling (used if you run in loop mode)
CLINIC_TIMEZONE = ZoneInfo('America/Chicago')
CLINIC_OPEN_HOUR = 8
CLINIC_CLOSE_HOUR = 20
DEEP_SYNC_HOUR = 2
INCREMENTAL_INTERVAL_MINUTES = 60
CLINIC_DELAY_SECONDS = float(os.environ.get('CLINIC_DELAY_SECONDS', '5.0'))

# Clinics & operatories (restrict which chairs to sync)
CLINIC_NUMS = [int(x) for x in os.environ.get('CLINIC_NUMS', '9034,9035').split(',') if x.strip().isdigit()]
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
    'ProvNum', 'ProvHyg', 'Asst', 'ClinicNum', 'Address', 'Zip', 'Email', 'WirelessPhone', 'Gender', 'Birthdate'
]

# =========================
# ===== LOGGING ===========
# =========================
logging.basicConfig(
    level=getattr(logging, LOG_LEVEL, logging.INFO),
    format='%(asctime)s  %(levelname)s  %(message)s',
    handlers=[logging.StreamHandler(sys.stdout)]
)
logger = logging.getLogger("od_to_ghl")

# =========================
# ===== UTILITIES =========
# =========================
_session = None
_session_lock = threading.Lock()

def get_session() -> requests.Session:
    global _session
    if _session is None:
        with _session_lock:
            if _session is None:
                s = requests.Session()
                adapter = HTTPAdapter(
                    max_retries=Retry(
                        total=RETRY_ATTEMPTS,
                        backoff_factor=BACKOFF_FACTOR,
                        status_forcelist=[408, 429, 500, 502, 503, 504, 520, 521, 522, 523, 524],
                        allowed_methods=["HEAD", "GET", "POST", "PUT", "PATCH"],
                        raise_on_status=False
                    ),
                    pool_connections=10,
                    pool_maxsize=20,
                    pool_block=True,
                )
                s.mount("https://", adapter)
                s.mount("http://", adapter)
                s.headers.update({
                    'User-Agent': 'OD-to-GHL/4.2',
                    'Accept': 'application/json',
                    'Accept-Encoding': 'gzip, deflate',
                    'Connection': 'keep-alive',
                })
                _session = s
    return _session

def od_headers() -> Dict[str, str]:
    return {'Authorization': f'ODFHIR {DEVELOPER_KEY}/{CUSTOMER_KEY}', 'Content-Type': 'application/json'}

def ghl_headers() -> Dict[str, str]:
    if not GHL_API_TOKEN:
        raise RuntimeError("Missing GHL API token. Set GHL_API_KEY or GHL_OAUTH_TOKEN or GHL_AUTH_TOKEN")
    return {
        'Authorization': f'Bearer {GHL_API_TOKEN}',
        'Version': GHL_API_VERSION,
        'Content-Type': 'application/json',
        'Accept': 'application/json'
    }

def parse_time(s: Optional[str]) -> Optional[datetime.datetime]:
    if not s:
        return None
    try:
        dt = datetime.datetime.fromisoformat(s.replace('Z', '+00:00'))
        return dt if dt.tzinfo else dt.replace(tzinfo=timezone.utc)
    except Exception:
        # Try fallback formats (e.g., YYYY-MM-DD)
        try:
            dt = datetime.datetime.strptime(s, "%Y-%m-%d")
            return dt.replace(tzinfo=timezone.utc)
        except Exception:
            return None

def to_iso(dt: datetime.datetime) -> str:
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=timezone.utc)
    return dt.isoformat()

def clinic_local_date(dt: datetime.datetime) -> datetime.date:
    return dt.astimezone(CLINIC_TIMEZONE).date()

EMAIL_RE = re.compile(r"^[A-Za-z0-9._%+\-']+@[A-Za-z0-9.\-]+\.[A-Za-z]{2,}$")
def valid_email_or_unknown(email: str) -> str:
    email = (email or '').strip()
    return email if email and EMAIL_RE.match(email) else "unknown@gmail.com"

def normalize_phone_for_search(phone: str) -> str:
    phone = (phone or '').strip()
    if not phone:
        return ""
    digits = re.sub(r"[^\d+]", "", phone)
    if digits and digits[0] != '+':
        pure = re.sub(r"\D", "", digits)
        if len(pure) == 10:
            return "+1" + pure  # default to +1; adjust per region if needed
    return digits

# =========================
# ===== MEGA STORAGE ======
# =========================
class MegaStore:
    def __init__(self, email: str, password: str, folder: str):
        self.email = email; self.password = password; self.folder = folder
        self._mega = None; self._folder_node = None
        self._ready = None  # cache readiness

    def _ensure(self):
        if self._ready is not None:
            return self._ready
        if not self.email or not self.password:
            self._ready = False
            return False
        try:
            from mega import Mega  # pip install mega.py
        except Exception:
            logger.warning("MEGA module missing (pip install mega.py) - using local only.")
            self._ready = False
            return False
        try:
            m = Mega()
            self._mega = m.login(self.email, self.password)
            # ensure folder
            node = None
            files = self._mega.get_files()
            for handle, meta in files.items():
                if meta.get('a', {}).get('n') == self.folder and meta.get('t') == 1:
                    node = {'h': handle}
                    break
            if not node:
                node = self._mega.create_folder(self.folder)
            self._folder_node = node
            self._ready = True
        except Exception as e:
            logger.warning(f"MEGA folder ensure failed: {e}")
            self._ready = False
        return self._ready

    def _find_file(self, name: str):
        try:
            files = self._mega.get_files()
            for handle, meta in files.items():
                if meta.get('a', {}).get('n') == name:
                    return {'h': handle}
        except Exception:
            pass
        return None

    def pull(self, filename: str):
        if not self._ensure(): return
        node = self._find_file(os.path.basename(filename))
        if not node: return
        td = tempfile.mkdtemp()
        try:
            self._mega.download(node, dest_path=td)
            src = os.path.join(td, os.path.basename(filename))
            if os.path.exists(src):
                shutil.move(src, filename)
                logger.info(f"Pulled {filename} from MEGA/{self.folder}")
        finally:
            shutil.rmtree(td, ignore_errors=True)

    def push(self, filename: str):
        if not self._ensure(): return
        try:
            # delete old if exists (simplify)
            old = self._find_file(os.path.basename(filename))
            if old:
                try: self._mega.delete(old)
                except Exception: pass
            self._mega.upload(filename, self._folder_node)
            logger.info(f"Pushed {filename} to MEGA/{self.folder}")
        except Exception as e:
            logger.warning(f"MEGA upload failed for {filename}: {e}")

MEGA_STORE = MegaStore(MEGA_EMAIL, MEGA_PASSWORD, MEGA_FOLDER)

def atomic_save_json(path: str, obj: Any):
    os.makedirs(os.path.dirname(path) or ".", exist_ok=True)
    tmp = None
    try:
        with tempfile.NamedTemporaryFile('w', delete=False, dir=os.path.dirname(path) or ".", suffix='.tmp') as f:
            json.dump(obj, f, indent=2)
            tmp = f.name
        shutil.move(tmp, path)
    finally:
        if tmp and os.path.exists(tmp):
            try: os.unlink(tmp)
            except Exception: pass

def load_json_or(path: str, default: Any):
    if os.path.exists(path):
        try:
            with open(path) as f:
                return json.load(f)
        except Exception as e:
            logger.warning(f"Failed to read {path}: {e}")
    return copy.deepcopy(default)

def pull_all_from_mega():
    for f in (STATE_FILE, SENT_APPTS_FILE, APPT_SNAPSHOT_FILE, GHL_MAP_FILE, GHL_CONTACTS_MAP_FILE):
        MEGA_STORE.pull(f)

def save_state(sent_map: Dict[str, Dict[str, str]], last_sync: Dict[int, Optional[str]],
               snapshot: Dict[str, Any], ghl_map: Dict[str, Any], contact_map: Dict[int, Any]):
    atomic_save_json(SENT_APPTS_FILE, sent_map); MEGA_STORE.push(SENT_APPTS_FILE)
    atomic_save_json(STATE_FILE, last_sync); MEGA_STORE.push(STATE_FILE)
    atomic_save_json(APPT_SNAPSHOT_FILE, snapshot); MEGA_STORE.push(APPT_SNAPSHOT_FILE)
    atomic_save_json(GHL_MAP_FILE, ghl_map); MEGA_STORE.push(GHL_MAP_FILE)
    atomic_save_json(GHL_CONTACTS_MAP_FILE, {str(k): v for k, v in contact_map.items()}); MEGA_STORE.push(GHL_CONTACTS_MAP_FILE)

# =========================
# ===== OD FETCH ==========
# =========================
def od_get(endpoint: str, params: Dict[str, Any]) -> Optional[List[Any]]:
    url = f"{API_BASE_URL}/{endpoint}"
    try:
        r = get_session().get(url, headers=od_headers(), params=params, timeout=REQUEST_TIMEOUT)
        r.raise_for_status()
        data = r.json()
        return data if isinstance(data, list) else [data]
    except requests.RequestException as e:
        logger.error(f"OD GET failed {endpoint}: {e}")
        return None

def od_get_paginated(endpoint: str, params: Dict[str, Any]) -> Optional[List[Any]]:
    if not ENABLE_PAGINATION:
        return od_get(endpoint, params)
    out: List[Any] = []
    offset = 0
    while True:
        p = dict(params); p['limit'] = PAGE_SIZE; p['offset'] = offset
        chunk = od_get(endpoint, p)
        if chunk is None:
            return None
        out.extend(chunk)
        if len(chunk) < PAGE_SIZE:
            break
        offset += PAGE_SIZE
    return out

def calculate_pattern_duration(pattern: str, minutes_per_slot: int = 5) -> int:
    if not pattern:
        return 60
    slots = sum(1 for ch in pattern.upper() if ch in ('X', '/'))
    return (slots or 12) * minutes_per_slot  # default 60 if no slots

def calculate_end_time(start_local: datetime.datetime, pattern: str) -> datetime.datetime:
    return start_local + timedelta(minutes=calculate_pattern_duration(pattern))

def get_operatories(clinic: int) -> List[Dict[str, Any]]:
    return od_get_paginated('operatories', {'ClinicNum': clinic}) or []

def fetch_appointments_for_window(clinic: int, start: datetime.datetime, end: datetime.datetime) -> List[Dict[str, Any]]:
    params_base = {
        'ClinicNum': str(clinic),
        'dateStart': start.astimezone(timezone.utc).date().strftime('%Y-%m-%d'),
        'dateEnd': end.astimezone(timezone.utc).date().strftime('%Y-%m-%d'),
        'fields': ','.join(REQUIRED_APPOINTMENT_FIELDS),
    }
    all_appts: List[Dict[str, Any]] = []
    valid_ops = set(CLINIC_OPERATORY_FILTERS.get(clinic, []))

    # touch operatories (logs)
    _ = get_operatories(clinic)

    for status in VALID_STATUSES:
        for op in CLINIC_OPERATORY_FILTERS.get(clinic, []):
            p = dict(params_base); p['AptStatus'] = status; p['Op'] = str(op)
            chunk = od_get_paginated('appointments', p) or []
            for a in chunk:
                opnum = a.get('Op') or a.get('OperatoryNum')
                if opnum in valid_ops:
                    all_appts.append(a)
            time.sleep(0.12)

    # de-dupe by (AptNum, PatNum, AptDateTime)
    uniq: Dict[Tuple[str, str, str], Dict[str, Any]] = {}
    for a in all_appts:
        key = (str(a.get('AptNum', '')), str(a.get('PatNum', '')), a.get('AptDateTime', ''))
        uniq[key] = a
    return list(uniq.values())

# =========================
# ===== GHL CLIENT =========
# =========================
def ghl_search_contact_by_phone(phone: str) -> Optional[Dict[str, Any]]:
    """
    Search contacts by text query (phone). POST /contacts/search
    """
    if not phone:
        return None
    url = f"{GHL_API_BASE}/contacts/search"
    body = {"query": phone, "limit": 10, "page": 1, "locationId": GHL_LOCATION_ID}
    try:
        r = get_session().post(url, headers=ghl_headers(), json=body, timeout=REQUEST_TIMEOUT)
        if r.status_code == 401:
            logger.error("GHL auth failed (401) – check token and Version header.")
            return None
        r.raise_for_status()
        data = r.json()
        contacts = data.get('contacts') if isinstance(data, dict) else (data if isinstance(data, list) else [])
        return contacts[0] if contacts else None
    except requests.RequestException as e:
        logger.error(f"GHL contact search failed: {e}")
        return None

def ghl_build_contact_payload(patient: Dict[str, Any], clinic_num: int) -> Dict[str, Any]:
    # Required fields you requested
    first = (patient.get("FName") or "").strip()
    last  = (patient.get("LName") or "").strip()
    email = valid_email_or_unknown(patient.get("Email"))
    phone = normalize_phone_for_search(patient.get("WirelessPhone") or patient.get("WirelessPh") or patient.get("HmPhone") or "")
    dob   = (patient.get("Birthdate") or "").strip() or None  # prefer YYYY-MM-DD from OD
    address1 = (patient.get("Address") or "").strip()
    postal = (patient.get("Zip") or patient.get("PostalCode") or "").strip()
    gender = (patient.get("Gender") or "").strip()

    payload: Dict[str, Any] = {
        "locationId": GHL_LOCATION_ID,
        "firstName": first,
        "lastName": last,
        "email": email,
        "phone": phone or None,
        "dateOfBirth": dob,          # YYYY-MM-DD
        "address1": address1 or None,
        "postalCode": postal or None,
        "gender": gender or None,
    }

    # Write clinic number to the custom field (preferred)
    if GHL_CUSTOM_FIELD_CLINIC_ID:
        payload["customFields"] = [{"id": GHL_CUSTOM_FIELD_CLINIC_ID, "value": str(clinic_num)}]
    else:
        # Fallback: tag if custom field id isn't provided
        payload["tags"] = [f"clinic:{clinic_num}"]

    # Clean out null/empty list fields to keep payload tidy
    return {k: v for k, v in payload.items() if v not in (None, [], "")}

def ghl_upsert_contact(patient: Dict[str, Any], clinic_num: int) -> Optional[str]:
    """
    1) Try search by phone (wireless/home).
    2) If found → return contactId.
    3) Else /contacts/upsert (create or update) with full payload → return its id.
    """
    phone = normalize_phone_for_search(patient.get("WirelessPhone") or patient.get("WirelessPh") or patient.get("HmPhone") or "")
    if phone:
        found = ghl_search_contact_by_phone(phone)
        if found and found.get('id'):
            return found['id']

    url = f"{GHL_API_BASE}/contacts/upsert"
    body = ghl_build_contact_payload(patient, clinic_num)
    try:
        r = get_session().post(url, headers=ghl_headers(), json=body, timeout=REQUEST_TIMEOUT)
        r.raise_for_status()
        j = r.json()
        return (j.get("contact") or {}).get("id") or j.get("id")
    except requests.RequestException as e:
        logger.error(f"GHL upsert contact failed: {e} body={e.response.text if getattr(e,'response',None) else ''}")
        return None

def map_appt_status_to_ghl(status: str) -> str:
    s = (status or "").strip().lower()
    if s == "broken":
        return "cancelled"
    if s == "complete":
        return "showed"
    if s == "scheduled":
        return "confirmed"
    return "confirmed"

def pick_calendar_id(clinic: int) -> Optional[str]:
    return GHL_CALENDAR_MAP.get(f"clinic:{clinic}") or GHL_CALENDAR_MAP.get("default")

def pick_assigned_user_id(clinic: int) -> Optional[str]:
    return GHL_ASSIGNED_USER_MAP.get(f"clinic:{clinic}")

def _event_id(a: Dict[str, Any]) -> Optional[str]:
    return a.get('id') or a.get('eventId') or a.get('appointmentId')

def _start_dt(a: Dict[str, Any]) -> Optional[datetime.datetime]:
    for k in ("startTime", "startAt", "start", "startDate"):
        dt = parse_time(a.get(k))
        if dt: return dt
    return None

def _updated_dt(a: Dict[str, Any]) -> datetime.datetime:
    for k in ("dateUpdated", "updatedAt", "updatedAtUtc", "lastUpdated", "modifiedAt"):
        dt = parse_time(a.get(k))
        if dt:
            return dt
    return _start_dt(a) or datetime.datetime.min.replace(tzinfo=timezone.utc)

def _calendar_id_in_event(a: Dict[str, Any]) -> Optional[str]:
    return a.get("calendarId") or (a.get("calendar") or {}).get("id")

def ghl_get_contact_appointments(contact_id: str) -> List[Dict[str, Any]]:
    if not contact_id:
        return []
    url = f"{GHL_API_BASE}/contacts/{contact_id}/appointments"
    try:
        r = get_session().get(url, headers=ghl_headers(), timeout=REQUEST_TIMEOUT)
        r.raise_for_status()
        data = r.json()
        if isinstance(data, list): return data
        return data.get('appointments', []) if isinstance(data, dict) else []
    except requests.RequestException as e:
        logger.error(f"GHL get contact appointments failed: {e}")
        return []

def pick_latest_same_day_event(contact_events: List[Dict[str, Any]],
                               od_start_local: datetime.datetime,
                               calendar_id: str) -> Optional[str]:
    """
    Filter to events on the SAME clinic-local day AND same calendar,
    then choose the one with the greatest 'updated' timestamp.
    """
    od_day = clinic_local_date(od_start_local)
    same_day_events: List[Dict[str, Any]] = []
    for ev in contact_events or []:
        sdt = _start_dt(ev)
        if not sdt:
            continue
        if clinic_local_date(sdt) == od_day and (_calendar_id_in_event(ev) == calendar_id):
            same_day_events.append(ev)

    if not same_day_events:
        return None

    # Sort desc by last-updated; tiebreaker = later start time
    same_day_events.sort(
        key=lambda e: (_updated_dt(e), _start_dt(e) or datetime.datetime.min.replace(tzinfo=timezone.utc)),
        reverse=True
    )
    return _event_id(same_day_events[0])

def ghl_create_appointment(calendar_id: str, contact_id: str, assigned_user_id: Optional[str],
                           title: str, start_dt: datetime.datetime, end_dt: datetime.datetime,
                           status: str) -> Optional[str]:
    url = f"{GHL_API_BASE}/calendars/events/appointments"
    body = {
        "calendarId": calendar_id,
        "contactId": contact_id,
        "assignedUserId": assigned_user_id,
        "title": title or "Dental Appointment",
        "appointmentStatus": status,  # confirmed / cancelled / showed
        "startTime": to_iso(start_dt.astimezone(CLINIC_TIMEZONE)),
        "endTime": to_iso(end_dt.astimezone(CLINIC_TIMEZONE)),
        "ignoreFreeSlotValidation": True,
        "locationId": GHL_LOCATION_ID
    }
    try:
        r = get_session().post(url, headers=ghl_headers(), json=body, timeout=REQUEST_TIMEOUT)
        r.raise_for_status()
        j = r.json()
        return j.get('id') or j.get('eventId') or j.get('appointmentId')
    except requests.RequestException as e:
        logger.error(f"GHL create appointment failed: {e} body={e.response.text if getattr(e,'response',None) else ''}")
        return None

def ghl_update_appointment(event_id: str, calendar_id: str, assigned_user_id: Optional[str],
                           title: str, start_dt: datetime.datetime, end_dt: datetime.datetime,
                           status: str) -> bool:
    if not event_id:
        return False
    url = f"{GHL_API_BASE}/calendars/events/appointments/{event_id}"
    body = {
        "calendarId": calendar_id,
        "assignedUserId": assigned_user_id,
        "title": title or "Dental Appointment",
        "appointmentStatus": status,
        "startTime": to_iso(start_dt.astimezone(CLINIC_TIMEZONE)),
        "endTime": to_iso(end_dt.astimezone(CLINIC_TIMEZONE)),
        "ignoreFreeSlotValidation": True,
        "locationId": GHL_LOCATION_ID
    }
    try:
        r = get_session().put(url, headers=ghl_headers(), json=body, timeout=REQUEST_TIMEOUT)
        r.raise_for_status()
        return True
    except requests.RequestException as e:
        logger.error(f"GHL update appointment failed: {e} body={e.response.text if getattr(e,'response',None) else ''}")
        return False

# =========================
# ===== STATE & CACHE =====
# =========================
def load_last_sync() -> Dict[int, Optional[str]]:
    raw = load_json_or(STATE_FILE, {})
    out: Dict[int, Optional[str]] = {}
    for k, v in raw.items():
        try:
            out[int(k)] = v
        except:
            pass
    return out

def load_patient_cache() -> Dict[int, Dict[str, Any]]:
    data = load_json_or('patient_cache.json', {"patients": {}})
    patients = data.get("patients", {})
    out: Dict[int, Dict[str, Any]] = {}
    for k, v in patients.items():
        try:
            out[int(k)] = v
        except:
            pass
    return out

def save_patient_cache(cache: Dict[int, Dict[str, Any]]):
    atomic_save_json('patient_cache.json', {"patients": {str(k): v for k, v in cache.items()}})

def load_contact_map() -> Dict[int, Any]:
    raw = load_json_or(GHL_CONTACTS_MAP_FILE, {})
    out: Dict[int, Any] = {}
    for k, v in raw.items():
        try:
            out[int(k)] = v
        except:
            pass
    return out

# =========================
# ===== CORE: PROCESS =====
# =========================
def validate_configuration() -> bool:
    errs = []
    if not DEVELOPER_KEY or not CUSTOMER_KEY:
        errs.append("Missing OpenDental API credentials")
    if not GHL_API_TOKEN:
        errs.append("Missing GHL API token (GHL_API_KEY / GHL_OAUTH_TOKEN / GHL_AUTH_TOKEN)")
    if not GHL_LOCATION_ID:
        errs.append("Missing GHL_LOCATION_ID")
    for c in CLINIC_NUMS:
        if f"clinic:{c}" not in GHL_CALENDAR_MAP and "default" not in GHL_CALENDAR_MAP:
            errs.append(f"Missing calendarId for clinic {c} (GHL_CALENDAR_MAP)")
        if f"clinic:{c}" not in GHL_ASSIGNED_USER_MAP:
            errs.append(f"Missing assignedUserId for clinic {c} (GHL_ASSIGNED_USER_MAP)")
        if c not in CLINIC_OPERATORY_FILTERS:
            errs.append(f"Missing operatory filter for clinic {c}")
        if c not in CLINIC_BROKEN_APPOINTMENT_TYPE_FILTERS:
            errs.append(f"Missing broken-appointment type filter for clinic {c}")
    if not GHL_CUSTOM_FIELD_CLINIC_ID:
        logger.warning("GHL_CUSTOM_FIELD_CLINIC_ID not set: will fall back to tagging clinic:<num> on contact.")
    if errs:
        logger.error("Config validation failed:")
        for e in errs: logger.error(" - " + e)
        return False
    return True

def ensure_contact_id(first: str, last: str, email: str, phone: str,
                      pat_num: int,
                      clinic_num: int,
                      contact_map: Dict[int, Any]) -> Tuple[Optional[str], bool]:
    """
    Returns (contact_id, is_new_contact).
    Path:
      1) If PatNum is already mapped -> return that id (is_new=False)
      2) Else search by normalized phone
      3) Else upsert (create) and return id (is_new=True)
    """
    # 1) PatNum fast path
    mapped = contact_map.get(pat_num)
    if mapped and mapped.get('contactId'):
        return mapped['contactId'], False

    # 2) phone search
    phone_norm = normalize_phone_for_search(phone)
    found = ghl_search_contact_by_phone(phone_norm) if phone_norm else None
    if found and found.get('id'):
        cid = found['id']
        contact_map[pat_num] = {
            "contactId": cid,
            "phone": phone_norm,
            "email": valid_email_or_unknown(email),
            "updatedAt": datetime.datetime.utcnow().replace(tzinfo=timezone.utc).isoformat()
        }
        return cid, False

    # 3) create with full payload
    patient_like = {
        "FName": first,
        "LName": last,
        "Email": email,
        "WirelessPhone": phone,
    }
    cid = ghl_upsert_contact(patient_like, clinic_num)
    if cid:
        contact_map[pat_num] = {
            "contactId": cid,
            "phone": phone_norm,
            "email": valid_email_or_unknown(email),
            "updatedAt": datetime.datetime.utcnow().replace(tzinfo=timezone.utc).isoformat()
        }
        return cid, True

    return None, False

def process_one_appt(appt: Dict[str, Any],
                     patients: Dict[int, Dict[str, Any]],
                     clinic: int,
                     ghl_map: Dict[str, Any],
                     contact_map: Dict[int, Any]) -> Optional[str]:
    """
    Rules:
      - If AptNum is already mapped → UPDATE that specific GHL eventId (even if day/time changed).
      - If AptNum not mapped:
          * ensure contact by phone (create if needed) and record contactId in contact_map.
          * If it's a NEW contact → CREATE appointment immediately (no need to fetch appointments).
          * If not new:
              - fetch the contact's appointments; pick the LATEST same-day event (by dateUpdated/updatedAt) on the SAME calendar → UPDATE.
              - if none → CREATE new event.
          * store AptNum -> eventId mapping for future updates.
    """
    apt_num = str(appt.get('AptNum', ''))
    pat_num = int(appt.get('PatNum', 0) or 0)
    if not apt_num or not pat_num:
        logger.warning("Skipping appt without AptNum/PatNum")
        return None

    # times
    start = parse_time(appt.get('AptDateTime', ''))
    if not start:
        logger.warning(f"Apt {apt_num}: invalid AptDateTime")
        return None
    start_local = start.astimezone(CLINIC_TIMEZONE)
    end_local = calculate_end_time(start_local, appt.get('Pattern', ''))

    # patient fields (for contact payload/title)
    p = patients.get(pat_num, {}) or {}
    first = (p.get('FName') or appt.get('FName') or '').strip()
    last  = (p.get('LName') or appt.get('LName') or '').strip()
    email = valid_email_or_unknown(p.get('Email', ''))
    phone = p.get('WirelessPhone') or p.get('HmPhone') or ''

    # clinic routing
    calendar_id = pick_calendar_id(clinic)
    assigned_user_id = pick_assigned_user_id(clinic)
    if not calendar_id:
        logger.error(f"Clinic {clinic}: missing calendar mapping")
        return None

    # Ensure / create contact, capture whether it's new
    contact_id, is_new_contact = ensure_contact_id(first, last, email, phone, pat_num, clinic, contact_map)
    if not contact_id:
        logger.error(f"Apt {apt_num}: failed to ensure contact")
        return None

    title = f"{first} {last}".strip() or "Dental Appointment"
    status = map_appt_status_to_ghl(appt.get('AptStatus', ''))

    # ===== If AptNum already mapped → update exact event =====
    mapped = ghl_map.get(apt_num)
    if mapped and mapped.get('eventId'):
        event_id = mapped['eventId']
        ok = ghl_update_appointment(event_id, calendar_id, assigned_user_id, title, start_local, end_local, status)
        if ok:
            mapped.update({"contactId": contact_id, "calendarId": calendar_id, "clinic": clinic})
            ghl_map[apt_num] = mapped
            logger.info(f"✓ Updated mapped event {event_id} for AptNum {apt_num}")
            return event_id
        else:
            logger.warning(f"Apt {apt_num}: mapped update failed — creating fresh and remapping")
            new_id = ghl_create_appointment(calendar_id, contact_id, assigned_user_id, title, start_local, end_local, status)
            if new_id:
                ghl_map[apt_num] = {"contactId": contact_id, "eventId": new_id, "calendarId": calendar_id, "clinic": clinic}
                return new_id
            return None

    # ===== Not mapped =====
    if is_new_contact:
        # brand-new contact → no need to fetch appointments; just create
        new_event_id = ghl_create_appointment(calendar_id, contact_id, assigned_user_id, title, start_local, end_local, status)
        if new_event_id:
            ghl_map[apt_num] = {"contactId": contact_id, "eventId": new_event_id, "calendarId": calendar_id, "clinic": clinic}
            logger.info(f"＋ Created event {new_event_id} for NEW contact, AptNum {apt_num}")
            return new_event_id
        logger.error(f"Apt {apt_num}: failed to create event for new contact")
        return None

    # Existing contact (but AptNum not mapped) → pick latest same-day event by updatedAt, same calendar only
    contact_events = ghl_get_contact_appointments(contact_id)
    candidate_event_id = pick_latest_same_day_event(contact_events, start_local, calendar_id)
    if candidate_event_id:
        ok = ghl_update_appointment(candidate_event_id, calendar_id, assigned_user_id, title, start_local, end_local, status)
        if ok:
            ghl_map[apt_num] = {"contactId": contact_id, "eventId": candidate_event_id, "calendarId": calendar_id, "clinic": clinic}
            logger.info(f"✓ Reconciled via latest same-day event {candidate_event_id} for AptNum {apt_num}")
            return candidate_event_id
        else:
            logger.warning(f"Apt {apt_num}: same-day update failed — creating fresh")

    # No same-day match → create new
    new_event_id = ghl_create_appointment(calendar_id, contact_id, assigned_user_id, title, start_local, end_local, status)
    if new_event_id:
        ghl_map[apt_num] = {"contactId": contact_id, "eventId": new_event_id, "calendarId": calendar_id, "clinic": clinic}
        logger.info(f"＋ Created event {new_event_id} for AptNum {apt_num}")
        return new_event_id

    logger.error(f"Apt {apt_num}: failed to create appointment")
    return None

# =========================
# ===== SYNC WINDOWS ======
# =========================
def generate_window(last_sync_iso: Optional[str], force_deep: bool) -> Tuple[datetime.datetime, datetime.datetime, bool]:
    now_utc = datetime.datetime.utcnow().replace(tzinfo=timezone.utc)
    if force_deep or not last_sync_iso:
        return (now_utc - timedelta(hours=SAFETY_OVERLAP_HOURS), now_utc + timedelta(hours=DEEP_SYNC_HOURS), False)
    last_dt = parse_time(last_sync_iso)
    if last_dt and (now_utc - last_dt) < timedelta(hours=24):
        return (last_dt - timedelta(hours=SAFETY_OVERLAP_HOURS), now_utc + timedelta(minutes=INCREMENTAL_SYNC_MINUTES), True)
    return (now_utc - timedelta(hours=SAFETY_OVERLAP_HOURS), now_utc + timedelta(hours=DEEP_SYNC_HOURS), False)

def get_next_run_time(now: datetime.datetime, force_deep: bool) -> datetime.datetime:
    now_c = now.astimezone(CLINIC_TIMEZONE)
    if force_deep or (now_c.hour == DEEP_SYNC_HOUR and now_c.minute == 0):
        return now_c
    if CLINIC_OPEN_HOUR <= now_c.hour < CLINIC_CLOSE_HOUR:
        minutes = (now_c.minute // INCREMENTAL_INTERVAL_MINUTES + 1) * INCREMENTAL_INTERVAL_MINUTES
        nxt = now_c.replace(second=0, microsecond=0, minute=0, hour=now_c.hour) + timedelta(minutes=minutes)
        if nxt.hour >= CLINIC_CLOSE_HOUR:
            nxt = nxt.replace(hour=DEEP_SYNC_HOUR, minute=0) + timedelta(days=1)
        return nxt
    return now_c.replace(hour=DEEP_SYNC_HOUR, minute=0, second=0, microsecond=0) + timedelta(days=1)

# =========================
# ===== MAIN SYNC =========
# =========================
def main_once(dry_run: bool = False, force_deep_sync: bool = False):
    if not validate_configuration():
        sys.exit(1)

    # Pull latest state from MEGA (best-effort)
    pull_all_from_mega()

    last_sync = load_last_sync()                  # clinic → iso str
    sent_map = load_json_or(SENT_APPTS_FILE, {})  # AptNum → {last_sent_tstamp}
    snapshot = load_json_or(APPT_SNAPSHOT_FILE, {})
    ghl_map = load_json_or(GHL_MAP_FILE, {})
    contact_map = load_contact_map()

    patient_cache = load_patient_cache()
    total = 0

    for idx, clinic in enumerate(CLINIC_NUMS):
        if idx > 0:
            time.sleep(CLINIC_DELAY_SECONDS)

        start, end, is_incr = generate_window(last_sync.get(clinic), force_deep_sync)
        logger.info(f"Clinic {clinic}: window {start} → {end} ({'incremental' if is_incr else 'deep'})")

        appts = fetch_appointments_for_window(clinic, start, end)
        if not appts:
            logger.info(f"Clinic {clinic}: no appointments in window.")
            continue

        # Filter to new/updated by DateTStamp vs SENT_APPTS_FILE
        to_process: List[Dict[str, Any]] = []
        pat_nums: Set[int] = set()
        for a in appts:
            aptnum = str(a.get('AptNum', ''))
            tstamp = parse_time(a.get('DateTStamp', ''))
            if not aptnum or not tstamp:
                continue
            prev = sent_map.get(aptnum, {}).get('last_sent_tstamp')
            prev_dt = parse_time(prev) if prev else None
            if prev_dt and tstamp <= prev_dt:
                continue
            to_process.append(a)
            if a.get('PatNum'):
                try: pat_nums.add(int(a['PatNum']))
                except: pass

        if not to_process:
            logger.info(f"Clinic {clinic}: nothing new/updated to send.")
            continue

        # Pull minimal patient info (prefer cache; if missing, fetch)
        patients: Dict[int, Dict[str, Any]] = {}
        for pn in sorted(pat_nums):
            if pn in patient_cache:
                patients[pn] = patient_cache[pn]
                continue
            data = od_get(f"patients/{pn}", {}) or []
            pat = (data[0] if isinstance(data, list) and data else (data if isinstance(data, dict) else {}))
            if pat:
                patients[pn] = pat
                patient_cache[pn] = pat
            time.sleep(0.12)

        # Process each
        for a in to_process:
            aptnum = str(a.get('AptNum', ''))
            if dry_run:
                logger.info(f"[DRY-RUN] Would process AptNum {aptnum}")
                continue

            event_id = process_one_appt(a, patients, clinic, ghl_map, contact_map)
            if event_id:
                total += 1
                sent_map.setdefault(aptnum, {})['last_sent_tstamp'] = a.get('DateTStamp', '')
                snapshot[aptnum] = a

        # advance last_sync only if we processed items
        if to_process:
            last_sync[clinic] = datetime.datetime.utcnow().replace(tzinfo=timezone.utc).isoformat()

    # Persist state + MEGA push
    save_patient_cache(patient_cache)
    save_state(sent_map, last_sync, snapshot, ghl_map, contact_map)
    logger.info(f"Done. Created/updated {total} appointments.")

def main_loop(dry_run: bool = False, force_deep_sync: bool = False):
    if not validate_configuration():
        sys.exit(1)
    while True:
        main_once(dry_run=dry_run, force_deep_sync=force_deep_sync)
        now = datetime.datetime.now(tz=timezone.utc)
        nxt = get_next_run_time(now, force_deep_sync)
        sleep_s = max(0, (nxt.astimezone(timezone.utc) - now).total_seconds())
        logger.info(f"Next run at {nxt.astimezone(CLINIC_TIMEZONE)} (in {sleep_s/60:.1f} min)")
        time.sleep(sleep_s)

# =========================
# ======== CLI ============
# =========================
if __name__ == "__main__":
    import argparse
    p = argparse.ArgumentParser(description="OpenDental → GHL sync (AptNum-first, latest same-day reconciliation, Clinic custom field)")
    p.add_argument("--dry-run", action="store_true", help="Log actions without calling GHL")
    p.add_argument("--force-deep-sync", action="store_true", help="Use deep sync window this cycle")
    p.add_argument("--once", action="store_true", help="Run once and exit")
    p.add_argument("--verbose", action="store_true", help="Enable DEBUG logs")
    args = p.parse_args()
    if args.verbose:
        logger.setLevel(logging.DEBUG)
    if args.once:
        main_once(dry_run=args.dry_run, force_deep_sync=args.force_deep_sync)
    else:
        main_loop(dry_run=args.dry_run, force_deep_sync=args.force_deep_sync)
