#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
OpenDental → GoHighLevel (LeadConnector) sync

What this does:
- Pull appointments from OpenDental for selected clinics & operatories.
- Ensure a GHL Contact with your required fields (phone-first lookup; if not found, create).
  Fields sent to GHL Contacts: firstName, lastName, dateOfBirth, email (validated/normalized),
  phone (normalized), address1, postalCode, gender, locationId, and a custom field "Clinic".
- Map clinic → calendarId and assignedUserId (9034/9035 strict routing).
- If AptNum already mapped to a GHL event → UPDATE that exact event (even if date/time changed).
- If AptNum not mapped:
    * If contact is NEW → CREATE new appointment immediately (no prior fetch).
    * If existing contact → **NEW BEHAVIOR**: look up AptNum in the Drive-mirrored
      reconciliation file `ghl_od_appointments_map.json` (eventId→info). If found, use
      that eventId for UPDATE; also refresh `updatedAt` in that file and update the main
      map. If not found, CREATE a new event.
      (We no longer fetch all GHL appointments to pick "latest same-day".)
- On **update failure**: **do not create** a new event. Log the error and record to a
  local dead-letter queue (`failed_updates.json`) for investigation.
- Appointment status mapping:
    Broken → cancelled
    Complete → showed
    Scheduled → confirmed
- Both CREATE and UPDATE set `ignoreFreeSlotValidation = true`.
- On **every appointment CREATE or UPDATE**, tag the contact with **fromopendental**.
- Persist mappings/state in **Google Drive (Service Account)**:
    - `ghl_contacts_map.json`: PatNum → contactId (plus phone/email snapshot)
    - `ghl_appointments_map.json`: AptNum → { eventId, contactId, calendarId, clinic }
    - plus `last_sync_state.json`, `sent_appointments.json`, `appointments_store.json`
- Drive is **local-first**: if a local state file exists, it is kept; Drive is used to
  seed missing files and to push updates after each run.

Requirements:
- Python 3.11+
- pip install: requests, google-api-python-client, google-auth, google-auth-httplib2, google-auth-oauthlib
- Environment variables (see bottom).
"""

import os
import re
import sys
import io
import json
import time
import copy
import shutil
import tempfile
import logging
import threading
import datetime
import base64
from typing import Any, Dict, Iterable, List, Optional, Set, Tuple
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

CONFIRMATION_STATUS_APPT_VALUE = 2936
SATURDAY_PREPAYMENT_AMOUNT = 50

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
# If missing, we will WARN and fall back to tagging (clinic:<num>).
GHL_CUSTOM_FIELD_CLINIC_ID = os.environ.get('GHL_CUSTOM_FIELD_CLINIC_ID', '').strip()
# Custom field that tracks family grouping (defaults to existing deployments)
GHL_CUSTOM_FIELD_FAMILY_NUMBER_ID = os.environ.get('GHL_CUSTOM_FIELD_FAMILY_NUMBER_ID', 'nbPK7xXHaN20YAL0gAQY').strip()
GHL_CUSTOM_FIELD_FAMILY_NUMBER_KEY = os.environ.get('GHL_CUSTOM_FIELD_FAMILY_NUMBER_KEY', 'contact.family_number').strip()
GHL_FAMILY_ASSOCIATION_ID = os.environ.get('GHL_FAMILY_ASSOCIATION_ID', '691b5120d98e4f55e855628f').strip()

# Google Drive (Service Account) for state
GDRIVE_SA_JSON        = os.environ.get('GDRIVE_SERVICE_ACCOUNT_JSON', '').strip()
GDRIVE_SA_JSON_B64    = os.environ.get('GDRIVE_SERVICE_ACCOUNT_JSON_B64', '').strip()
GDRIVE_SUBJECT        = os.environ.get('GDRIVE_SUBJECT', '').strip()         # optional (domain-wide delegation)
GDRIVE_FOLDER_ID      = os.environ.get('GDRIVE_FOLDER_ID', '').strip()
GDRIVE_FOLDER_NAME    = os.environ.get('GDRIVE_FOLDER_NAME', 'od_ghl_sync').strip()  # used if no folder id
GOOGLE_DRIVE_FILE_ID_OPPORTUNITIES = os.environ.get('GOOGLE_DRIVE_FILE_ID_OPPORTUNITIES', '').strip()

# Local state files (also mirrored to Drive)
STATE_FILE = 'last_sync_state.json'             # clinic → ISO timestamp
SENT_APPTS_FILE = 'sent_appointments.json'      # AptNum → { last_sent_tstamp }
APPT_SNAPSHOT_FILE = 'appointments_store.json'  # AptNum → last OD snapshot used
GHL_MAP_FILE = 'ghl_appointments_map.json'      # AptNum → { contactId, eventId, calendarId, clinic }
GHL_CONTACTS_MAP_FILE = 'ghl_contacts_map.json' # PatNum → { contactId, phone, email, updatedAt }
FAILED_UPDATES_FILE = 'failed_updates.json'     # AptNum → failure info (DLQ)
# NEW: reconciliation file (Drive-mirrored; eventId → info containing aptNum)
GHL2OD_APPTS_FILE = 'ghl_od_appointments_map.json'

# Track created family relations per run to avoid duplicate API calls
_FAMILY_RELATIONS_CREATED: Set[Tuple[str, str]] = set()
_FAMILY_RELATION_CONFIG_WARNED = False

# Performance / retry / pagination
LOG_LEVEL = os.environ.get('LOG_LEVEL', 'INFO').upper()
REQUEST_TIMEOUT = int(os.environ.get('REQUEST_TIMEOUT', '120'))
RETRY_ATTEMPTS = int(os.environ.get('RETRY_ATTEMPTS', '5'))
BACKOFF_FACTOR = float(os.environ.get('BACKOFF_FACTOR', '3.0'))
ENABLE_PAGINATION = os.environ.get('ENABLE_PAGINATION', 'true').lower() == 'true'
PAGE_SIZE = int(os.environ.get('PAGE_SIZE', '100'))

# ➕ Gentler on OpenDental (tune via env; try 0.75–1.0 for “very gentle”)
OD_RATE_LIMIT_SECONDS = float(os.environ.get('OD_RATE_LIMIT_SECONDS', '0.35'))

# Optional deep trace (writes sync_debug.log and debug/*.json)
TRACE = os.environ.get('OD_GHL_TRACE', '0') == '1'

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
VALID_STATUSES = {'Scheduled', 'Complete', 'Broken', 'UnschedList'}

CONGRESS_ORTHO_CLINIC = 9035
CONGRESS_ORTHO_BLOCKED_OPERATORIES = {0, 11576, 11577, 11582, 11574}

REQUIRED_APPOINTMENT_FIELDS = [
    'AptNum', 'AptDateTime', 'AptStatus', 'PatNum', 'Op', 'OperatoryNum',
    'Pattern', 'AppointmentTypeNum', 'Note', 'DateTStamp', 'FName', 'LName',
    'ProvNum', 'ProvHyg', 'Asst', 'ClinicNum', 'Address', 'Zip', 'Email',
    'WirelessPhone', 'Gender', 'Birthdate'
]

# >>> ALWAYS read cache file from the repo folder next to this script
APPT_TYPES_CACHE_PATH = os.path.join(os.path.dirname(__file__), 'appointment_types_cache.json')

# =========================
# ===== LOGGING ===========
# =========================
handlers = [logging.StreamHandler(sys.stdout)]
if TRACE:
    try:
        fh = logging.FileHandler('sync_debug.log', encoding='utf-8')
        handlers.append(fh)
    except Exception:
        pass

logging.basicConfig(
    level=getattr(logging, LOG_LEVEL, logging.INFO),
    format='%(asctime)s  %(levelname)s  %(message)s',
    handlers=handlers
)
logger = logging.getLogger("od_to_ghl")

def _debug_write(name: str, obj: Any):
    if not TRACE:
        return
    try:
        os.makedirs('debug', exist_ok=True)
        ts = datetime.datetime.utcnow().strftime('%Y%m%dT%H%M%SZ')
        path = os.path.join('debug', f'{ts}_{name}')
        with open(path, 'w', encoding='utf-8') as f:
            json.dump(obj, f, indent=2)
    except Exception as e:
        logger.debug(f"debug write failed for {name}: {e}")

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
                    'User-Agent': 'OD-to-GHL/4.3',
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

# ---- TIME HELPERS (timezone fix) ----
def parse_time(s: Optional[str]) -> Optional[datetime.datetime]:
    """
    Parse OpenDental datetime strings.
    - If the string is naive (no tz), treat it as clinic-local (America/Chicago).
    - Otherwise respect the given timezone.
    """
    if not s:
        return None
    try:
        dt = datetime.datetime.fromisoformat(s.replace('Z', '+00:00'))
        if dt.tzinfo is None:
            return dt.replace(tzinfo=CLINIC_TIMEZONE)
        return dt
    except Exception:
        try:
            dt = datetime.datetime.strptime(s, "%Y-%m-%d")
            return dt.replace(tzinfo=CLINIC_TIMEZONE)
        except Exception:
            return None

def to_iso(dt: datetime.datetime) -> str:
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=timezone.utc)
    return dt.isoformat()

def to_utc_z(dt: datetime.datetime) -> str:
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=CLINIC_TIMEZONE)
    return dt.astimezone(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")

def clinic_tz_name() -> str:
    return getattr(CLINIC_TIMEZONE, "key", str(CLINIC_TIMEZONE))

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
        return pure
    return digits


def detect_family_patients(patients: Dict[int, Dict[str, Any]]) -> Tuple[Set[int], Dict[str, Set[int]]]:
    """Identify patients who belong to the same family based on shared wireless phone numbers."""
    groups: Dict[str, List[Tuple[int, Dict[str, Any]]]] = {}
    for pat_num, info in patients.items():
        phone_raw = (info.get('WirelessPhone') or '').strip()
        phone_key = normalize_phone_for_search(phone_raw)
        if not phone_key:
            continue
        groups.setdefault(phone_key, []).append((pat_num, info))

    family_pat_nums: Set[int] = set()
    family_groups: Dict[str, Set[int]] = {}

    def _first_key(name: Optional[str]) -> str:
        return re.sub(r"\s+", "", (name or '').strip().lower())

    def _last_parts(name: Optional[str]) -> Set[str]:
        return {part for part in re.split(r"\s+", (name or '').strip().lower()) if part}

    for phone_key, members in groups.items():
        if len(members) < 2:
            continue

        prepared: List[Tuple[int, str, Set[str]]] = []
        for pat_num, info in members:
            prepared.append((pat_num, _first_key(info.get('FName')), _last_parts(info.get('LName'))))

        matched_for_key: Set[int] = set()
        for idx, (pat_num, first_key, last_parts) in enumerate(prepared):
            if not first_key or not last_parts:
                continue
            for jdx, (other_pat_num, other_first_key, other_last_parts) in enumerate(prepared):
                if idx == jdx:
                    continue
                if not other_first_key or not other_last_parts:
                    continue
                if first_key == other_first_key:
                    continue
                if last_parts.intersection(other_last_parts):
                    matched_for_key.add(pat_num)
                    matched_for_key.add(other_pat_num)
                    break

        if matched_for_key:
            family_groups[phone_key] = matched_for_key
            family_pat_nums.update(matched_for_key)

    return family_pat_nums, family_groups

# =========================
# === APPT TYPE CACHE =====
# =========================
_APPT_TYPE_BY_NUM: Dict[int, str] = {}
_APPT_TYPES_LOADED = False

def _try_int(x) -> Optional[int]:
    try:
        return int(x)
    except Exception:
        return None

def load_appointment_types_cache(path: str = APPT_TYPES_CACHE_PATH) -> Dict[int, str]:
    """
    Load local cache without hitting OD.
    Accepts flexible shapes:
      - {"by_num": {"123":"COMP EX", ...}}
      - {"types": [{"AppointmentTypeNum":123,"AppointmentTypeName":"COMP EX"}, ...]}
      - {"appointment_types": {"shared": {"123":"COMP EX", ...}, ...}}
      - Or a plain {"123":"COMP EX", ...}
    """
    data = load_json_or(path, {})
    by_num: Dict[int, str] = {}

    def _add_pair(k: Any, v: Any):
        ik = _try_int(k)
        if ik is not None and v:
            by_num[ik] = str(v)

    # Case 1: explicit "by_num"
    if isinstance(data, dict) and data.get("by_num"):
        for k, v in (data["by_num"] or {}).items():
            _add_pair(k, v)
        logger.info(f"Loaded {len(by_num)} appointment types from {os.path.abspath(path)}")
        return by_num

    # Case 2: list of dicts
    if isinstance(data, dict) and isinstance(data.get("types"), list):
        for t in data["types"]:
            n = _try_int(t.get("AppointmentTypeNum"))
            name = t.get("AppointmentTypeName") or t.get("Description") or t.get("Name")
            if n is not None and name:
                by_num[n] = str(name)
        logger.info(f"Loaded {len(by_num)} appointment types from {os.path.abspath(path)}")
        return by_num

    # Case 3: nested groups under "appointment_types" (e.g., {"appointment_types": {"shared": {...}}})
    if isinstance(data, dict) and isinstance(data.get("appointment_types"), dict):
        groups = data["appointment_types"]
        for grp in groups.values():
            if isinstance(grp, dict):
                for k, v in grp.items():
                    _add_pair(k, v)
            elif isinstance(grp, list):
                for t in grp:
                    n = _try_int(t.get("AppointmentTypeNum"))
                    name = t.get("AppointmentTypeName") or t.get("Description") or t.get("Name")
                    if n is not None and name:
                        by_num[n] = str(name)
        logger.info(f"Loaded {len(by_num)} appointment types from {os.path.abspath(path)}")
        return by_num

    # Case 4: plain num->name map
    if isinstance(data, dict):
        for k, v in data.items():
            _add_pair(k, v)
        logger.info(f"Loaded {len(by_num)} appointment types from {os.path.abspath(path)}")
        return by_num

    logger.info(f"Loaded 0 appointment types from {os.path.abspath(path)}")
    return {}

def appt_type_name_from_cache(appointment_type_num: Any) -> Optional[str]:
    n = _try_int(appointment_type_num)
    if n is None:
        return None
    return _APPT_TYPE_BY_NUM.get(n)

# =========================
# === GOOGLE DRIVE STATE ==
# =========================
_FAILED_UPDATES: Dict[str, Any] = {}
opportunities_to_match: Dict[str, Any] = {}

class DriveAdapter:
    """
    Google Drive storage for state files (Service Account).
    Env:
      GDRIVE_SERVICE_ACCOUNT_JSON (raw JSON) OR GDRIVE_SERVICE_ACCOUNT_JSON_B64 (base64)
      Optional: GDRIVE_SUBJECT for domain-wide delegation
      One of: GDRIVE_FOLDER_ID or GDRIVE_FOLDER_NAME (folder auto-created if missing)
    """
    def __init__(self):
        self.service = None
        self.folder_id = GDRIVE_FOLDER_ID

    def _creds(self):
        try:
            from google.oauth2 import service_account
        except Exception as e:
            logger.error("Google auth libs not installed. pip install google-auth google-api-python-client. %s", e)
            return None

        sa_info = None
        if GDRIVE_SA_JSON:
            try:
                sa_info = json.loads(GDRIVE_SA_JSON)
            except Exception as e:
                logger.error("Invalid GDRIVE_SERVICE_ACCOUNT_JSON: %s", e)
                return None
        elif GDRIVE_SA_JSON_B64:
            try:
                sa_info = json.loads(base64.b64decode(GDRIVE_SA_JSON_B64).decode('utf-8'))
            except Exception as e:
                logger.error("Invalid GDRIVE_SERVICE_ACCOUNT_JSON_B64: %s", e)
                return None
        else:
            logger.warning("No service account JSON provided; Drive will be unavailable.")
            return None

        scopes = ['https://www.googleapis.com/auth/drive']
        if GDRIVE_SUBJECT:
            return service_account.Credentials.from_service_account_info(sa_info, scopes=scopes).with_subject(GDRIVE_SUBJECT)
        return service_account.Credentials.from_service_account_info(sa_info, scopes=scopes)

    def connect(self):
        creds = self._creds()
        if not creds:
            logger.warning("Drive unavailable; running in local-only mode.")
            return
        try:
            from googleapiclient.discovery import build
        except Exception as e:
            logger.error("google-api-python-client not installed. pip install google-api-python-client. %s", e)
            return

        self.service = build('drive', 'v3', credentials=creds, cache_discovery=False)

        if not self.folder_id:
            # find folder by name or create it
            q = (
                f"name = '{GDRIVE_FOLDER_NAME}' and "
                f"mimeType = 'application/vnd.google-apps.folder' and trashed = false"
            )
            res = self.service.files().list(q=q, fields="files(id,name)").execute()
            files = res.get('files', [])
            if files:
                self.folder_id = files[0]['id']
            else:
                body = {'name': GDRIVE_FOLDER_NAME, 'mimeType': 'application/vnd.google-apps.folder'}
                f = self.service.files().create(body=body, fields='id').execute()
                self.folder_id = f['id']

        logger.info("Drive connected. Folder id: %s", self.folder_id)

    def _find_file(self, name: str) -> Optional[str]:
        if not self.service or not self.folder_id:
            return None
        q = f"name = '{os.path.basename(name)}' and '{self.folder_id}' in parents and trashed = false"
        res = self.service.files().list(
            q=q,
            fields="files(id,name,modifiedTime)",
            orderBy="modifiedTime desc"
        ).execute()
        files = res.get('files', [])
        return files[0]['id'] if files else None

    def pull(self, filename: str):
        """
        Download the current contents of an existing file into the local path,
        but only if the local file does not already exist.
        """
        if os.path.exists(filename):
            return
        if not self.service or not self.folder_id:
            logger.warning("Drive unavailable; running in local-only mode.")
            return

        file_id = self._find_file(filename)
        if not file_id:
            return

        try:
            from googleapiclient.http import MediaIoBaseDownload

            req = self.service.files().get_media(fileId=file_id)
            fh = io.BytesIO()
            downloader = MediaIoBaseDownload(fh, req)
            done = False
            while not done:
                _, done = downloader.next_chunk()
            with open(filename, "wb") as f:
                f.write(fh.getvalue())
            logger.info("Drive pull → %s", os.path.basename(filename))
        except Exception as e:
            logger.warning("Drive pull error for %s: %s", os.path.basename(filename), e)

    def push(self, filename: str):
        """
        Upload a new version of an EXISTING user-owned file.
        - No delete
        - No create
        This avoids both 'insufficientFilePermissions' and 'storageQuotaExceeded'
        when the SA has editor access but no quota.
        """
        if not os.path.exists(filename):
            logger.warning("Drive push skipped (local file missing): %s", filename)
            return
        if not self.service or not self.folder_id:
            logger.warning("Drive push skipped (Drive unavailable): %s", filename)
            return

        basename = os.path.basename(filename)
        file_id = self._find_file(basename)
        if not file_id:
            logger.error(
                "Drive push aborted: '%s' not found in folder. "
                "Create the empty file in Drive (owned by you), share with the SA as Editor, then rerun.",
                basename,
            )
            return

        try:
            from googleapiclient.http import MediaFileUpload
            media = MediaFileUpload(filename, mimetype="application/json", resumable=False)
            updated = self.service.files().update(fileId=file_id, media_body=media).execute()
            logger.info("Drive push (version update) → %s (id %s)", basename, updated.get('id'))
        except Exception as e:
            logger.warning("Drive push error for %s: %s", basename, e)
            if "storageQuotaExceeded" in str(e):
                logger.error(
                    "Drive refused due to SA quota. Ensure you are UPDATING an existing file you own. "
                    "Do not delete/create files in My Drive with the service account."
                )

    def fetch_file_by_id(self, file_id: str) -> Optional[bytes]:
        """
        Download a file by its Drive id and return raw bytes.
        """
        if not file_id:
            return None
        if not self.service:
            logger.warning("Drive unavailable; cannot fetch file id %s", file_id)
            return None
        try:
            from googleapiclient.http import MediaIoBaseDownload

            req = self.service.files().get_media(fileId=file_id)
            fh = io.BytesIO()
            downloader = MediaIoBaseDownload(fh, req)
            done = False
            while not done:
                _, done = downloader.next_chunk()
            return fh.getvalue()
        except Exception as e:
            logger.warning("Drive fetch error for id %s: %s", file_id, e)
            return None

    def update_file_by_id(self, file_id: str, content: bytes, mime: str = "application/json", max_retries: int = 3) -> bool:
        if not file_id:
            logger.warning("No file id provided for Drive update")
            return False
        if not self.service:
            logger.warning("Drive unavailable; cannot update file id %s", file_id)
            return False
        
        from googleapiclient.http import MediaIoBaseUpload
        
        last_error: Optional[Exception] = None
        for attempt in range(max_retries):
            try:
                media = MediaIoBaseUpload(io.BytesIO(content), mimetype=mime, resumable=False)
                self.service.files().update(fileId=file_id, media_body=media).execute()
                return True
            except Exception as e:
                last_error = e
                err_str = str(e).lower()
                # Retry on transient SSL/network errors
                if any(x in err_str for x in ('eof occurred', 'ssl', 'connection', 'timeout', 'broken pipe')):
                    wait_time = (2 ** attempt) + 1  # 2, 3, 5 seconds
                    logger.warning("Drive update transient error (attempt %d/%d) for id %s: %s. Retrying in %ds...", 
                                   attempt + 1, max_retries, file_id, e, wait_time)
                    time.sleep(wait_time)
                    continue
                # Non-retryable error
                break
        
        logger.warning("Drive update error for id %s: %s", file_id, last_error)
        return False


DRIVE = DriveAdapter()


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

def pull_all_from_drive():
    """
    Local-first: only pull a file from Drive if it does NOT exist locally.
    """
    for f in (STATE_FILE, SENT_APPTS_FILE, APPT_SNAPSHOT_FILE, GHL_MAP_FILE, GHL_CONTACTS_MAP_FILE, FAILED_UPDATES_FILE, GHL2OD_APPTS_FILE):
        if not os.path.exists(f):
            try:
                DRIVE.pull(f)
            except Exception as e:
                logger.warning(f"Drive pull failed for {f}: {e}")

def load_opportunities_to_match_from_drive() -> Dict[str, Any]:
    file_id = GOOGLE_DRIVE_FILE_ID_OPPORTUNITIES
    if not file_id:
        logger.warning("GOOGLE_DRIVE_FILE_ID_OPPORTUNITIES not set; opportunities_to_match remains empty.")
        return {}
    blob = DRIVE.fetch_file_by_id(file_id)
    if blob is None:
        return {}
    try:
        data = json.loads(blob.decode('utf-8'))
        if isinstance(data, dict):
            logger.info("Loaded opportunities_to_match from Drive id %s (%d keys)", file_id, len(data))
            return data
        logger.warning("Opportunities file is not a JSON object (got %s)", type(data).__name__)
    except Exception as e:
        logger.warning("Failed to parse opportunities file: %s", e)
    return {}


def save_state(sent_map: Dict[str, Dict[str, str]], last_sync: Dict[int, Optional[str]],
               snapshot: Dict[str, Any], ghl_map: Dict[str, Any], contact_map: Dict[int, Any]):
    atomic_save_json(SENT_APPTS_FILE, sent_map); DRIVE.push(SENT_APPTS_FILE)
    atomic_save_json(STATE_FILE, last_sync); DRIVE.push(STATE_FILE)
    atomic_save_json(APPT_SNAPSHOT_FILE, snapshot); DRIVE.push(APPT_SNAPSHOT_FILE)
    atomic_save_json(GHL_MAP_FILE, ghl_map); DRIVE.push(GHL_MAP_FILE)
    atomic_save_json(GHL_CONTACTS_MAP_FILE, {str(k): v for k, v in contact_map.items()}); DRIVE.push(GHL_CONTACTS_MAP_FILE)

def load_failed_updates() -> Dict[str, Any]:
    return load_json_or(FAILED_UPDATES_FILE, {})

def save_failed_updates(failed: Dict[str, Any]):
    atomic_save_json(FAILED_UPDATES_FILE, failed)
    # Optional Drive push if you've pre-created failed_updates.json in Drive:
    DRIVE.push(FAILED_UPDATES_FILE)

def dlq_record(apt_num: str, clinic: int, event_id: Optional[str], contact_id: Optional[str],
               err_code: Optional[int], err_msg: str):
    now = datetime.datetime.utcnow().replace(tzinfo=timezone.utc).isoformat()
    item = _FAILED_UPDATES.get(apt_num, {})
    item.update({
        "aptnum": apt_num,
        "clinic": clinic,
        "eventId": event_id,
        "contactId": contact_id,
        "lastErrorCode": err_code,
        "lastErrorMessage": (err_msg or "")[:1000],
        "retryCount": item.get("retryCount", 0) + 1,
        "firstSeen": item.get("firstSeen") or now,
        "lastTried": now,
    })
    _FAILED_UPDATES[apt_num] = item

# =========================
# ===== OD FETCH ==========
# =========================
def od_get(endpoint: str, params: Dict[str, Any]) -> Optional[List[Any]]:
    url = f"{API_BASE_URL}/{endpoint}"
    try:
        r = get_session().get(url, headers=od_headers(), params=params, timeout=REQUEST_TIMEOUT)
        _debug_write(f"od_{endpoint}_req.json", {"url": url, "params": params})
        r.raise_for_status()
        data = r.json()
        _debug_write(f"od_{endpoint}_resp.json", data)
        return data if isinstance(data, list) else [data]
    except requests.RequestException as e:
        resp_obj = getattr(e, 'response', None)
        body = getattr(resp_obj, 'text', '') if resp_obj is not None else ''
        logger.error(f"OD GET failed {endpoint}: {e} {body}")
        return None

def od_get_paginated(endpoint: str, params: Dict[str, Any]) -> Optional[List[Any]]:
    if not ENABLE_PAGINATION:
        return od_get(endpoint, params)
    out: List[Any] = []
    offset = 0
    while True:
        p = dict(params); p['limit'] = PAGE_SIZE; p['offset'] = offset
        chunk = od_get(endpoint, p) or []
        if chunk is None:
            return None
        out.extend(chunk)
        # be gentle between OD page fetches
        time.sleep(OD_RATE_LIMIT_SECONDS)
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
    ops = od_get_paginated('operatories', {'ClinicNum': clinic}) or []
    logger.debug(f"Clinic {clinic}: operatories found: {len(ops)}")
    if ops:
        try:
            sample = [str(o.get('OperatoryNum') or o.get('Op')) for o in ops][:10]
            logger.debug(f"Clinic {clinic}: operatory ids (sample): {', '.join(sample)}")
        except Exception:
            pass
    return ops

def fetch_appointment_history(aptnum: str) -> List[Dict[str, Any]]:
    if not aptnum:
        return []
    history = od_get('histappointments', {'AptNum': str(aptnum)}) or []
    return history if isinstance(history, list) else []

def _history_entry_timestamp(entry: Dict[str, Any]) -> datetime.datetime:
    for key in ('DateTStamp', 'SecDateTEntry', 'SecDateTEdit', 'ChangedDate'):
        ts = parse_time(entry.get(key)) if entry else None
        if ts:
            return ts
    return datetime.datetime.min.replace(tzinfo=timezone.utc)


def fetch_appointments_for_window(clinic: int, start: datetime.datetime, end: datetime.datetime) -> Tuple[List[Dict[str, Any]], List[Dict[str, Any]]]:
    params_base = {
        'ClinicNum': str(clinic),
        'dateStart': start.astimezone(timezone.utc).date().strftime('%Y-%m-%d'),
        'dateEnd': end.astimezone(timezone.utc).date().strftime('%Y-%m-%d'),
        'fields': ','.join(REQUIRED_APPOINTMENT_FIELDS),
    }
    all_appts: List[Dict[str, Any]] = []
    special_saturday_appts: List[Dict[str, Any]] = []
    valid_ops = set(CLINIC_OPERATORY_FILTERS.get(clinic, []))
    # prepare Broken-type allowlist (normalized)
    broken_allow = set(n.strip().upper() for n in CLINIC_BROKEN_APPOINTMENT_TYPE_FILTERS.get(clinic, []))

    _ = get_operatories(clinic)

    # show the window, ops & statuses we’re about to pull
    logger.debug(
        f"Clinic {clinic}: fetching appointments {params_base['dateStart']}..{params_base['dateEnd']} "
        f"ops={sorted(valid_ops)} statuses={sorted(VALID_STATUSES)}"
    )

    for status in VALID_STATUSES:
        for op in CLINIC_OPERATORY_FILTERS.get(clinic, []):
            p = dict(params_base); p['AptStatus'] = status; p['Op'] = str(op)

            if status == "UnschedList":
                p['Op'] = "0"

            chunk = od_get_paginated('appointments', p) or []

            # row-count per status/op query
            logger.debug(f"Clinic {clinic}: AptStatus={status} Op={p['Op']} -> {len(chunk)} returned")

            for a in chunk:
                if not status == "UnschedList":
                    opnum = a.get('Op') or a.get('OperatoryNum')
                    if opnum not in valid_ops:
                        continue

                # >>> NEW RULE: exclude any appointment whose Note contains "[fromGHL]"
                note_val = a.get('Note') or ''
                if isinstance(note_val, str) and '[fromGHL]' in note_val:
                    logger.debug(f"Skip AptNum {a.get('AptNum')} – Note contains [fromGHL]")

                    # if the appointment is not set for saturday, then skip it
                    # we use another mechanism to handle saturday appts created from GHL
                    apt_date_str = a.get('AptDateTime') or a.get('AptDate')
                    apt_date = parse_time(apt_date_str)
                    if apt_date and apt_date.weekday() == 5:
                        a['is_family'] = False
                        special_saturday_appts.append(a)
                    continue

                # NEW: filter Broken appts by appointment type name using local cache
                if status == 'Broken':
                    type_name = appt_type_name_from_cache(a.get('AppointmentTypeNum')) \
                                or (a.get('AppointmentTypeName') or a.get('AppointmentType') or '')
                    name_norm = (type_name or '').strip().upper()
                    if not name_norm:
                        logger.debug(f"Skip Broken AptNum {a.get('AptNum')} – unknown AppointmentType (not in cache)")
                        continue
                    if name_norm not in broken_allow:
                        # not in clinic allow-list → skip
                        continue

                a['is_family'] = False
                all_appts.append(a)
            # be gentle on OD between queries, too
            time.sleep(OD_RATE_LIMIT_SECONDS)

            # There is no need to iterate over all the OP numbers for the clinic
            # if we are only searching for the unscheduled list (always op=0)
            if status == "UnschedList":
                break

    # de-dupe regular appointments
    uniq: Dict[Tuple[str, str, str], Dict[str, Any]] = {}
    for a in all_appts:
        key = (str(a.get('AptNum', '')), str(a.get('PatNum', '')), a.get('AptDateTime', ''))
        uniq[key] = a
    result = list(uniq.values())

    # de-dupe special saturday appointments
    uniq_saturday: Dict[Tuple[str, str, str], Dict[str, Any]] = {}
    for a in special_saturday_appts:
        key = (str(a.get('AptNum', '')), str(a.get('PatNum', '')), a.get('AptDateTime', ''))
        uniq_saturday[key] = a
    special_saturday_result = list(uniq_saturday.values())

    for appointment in result:
        if appointment.get('AptStatus') == "UnschedList":
            note_marker = None
            history = fetch_appointment_history(str(appointment.get('AptNum', '')))
            if history:
                history_sorted = sorted(history, key=_history_entry_timestamp)
                if len(history_sorted) >= 2:
                    last_status = str((history_sorted[-1] or {}).get('AptStatus', '')).strip().lower()
                    prev_hist_app_action = str((history_sorted[-2] or {}).get('HistApptAction', '')).strip().lower()
                    if last_status in ['unschedlist', 'broken'] and prev_hist_app_action == 'cancelled':
                        note_marker = "CANCELLED"
                    if last_status in ['unschedlist', 'broken'] and prev_hist_app_action == 'missed':
                        note_marker = "MISSED"

            appointment['_ghl_note'] = note_marker
            time.sleep(OD_RATE_LIMIT_SECONDS)

    # final count summary for the clinic
    logger.info(f"Clinic {clinic}: appointments raw={len(all_appts)} after-de-dupe={len(result)} special-saturday={len(special_saturday_result)}")
    _debug_write(f"od_appts_clinic_{clinic}.json", {"start": start.isoformat(), "end": end.isoformat(), "appointments": result, "special_saturday_appts": special_saturday_result})

    return result, special_saturday_result


# =========================
# ===== GHL CLIENT ========
# =========================
def ghl_search_contact_by_phone(phone: str) -> Optional[Dict[str, Any]]:
    """
    Search contacts by text query (phone). POST /contacts/search
    - Skips search if phone is too short (to avoid 422s and useless queries)
    - Uses page/pageLimit (no 'limit') to satisfy API schema
    """
    if not phone:
        return None

    # Require at least 7 digits for a "usable" search
    pure_digits = re.sub(r"\D", "", phone.replace("+", ""))
    if len(pure_digits) < 7:
        logger.debug(f"Skipping GHL contact search: phone too short ('{phone}')")
        return None

    url = f"{GHL_API_BASE}/contacts/search"

    body = {"query": phone, "page": 1, "pageLimit": 1, "locationId": GHL_LOCATION_ID}
    try:
        r = get_session().post(url, headers=ghl_headers(), json=body, timeout=REQUEST_TIMEOUT)
        _debug_write("ghl_contacts_search_req.json", {"url": url, "body": body})
        if r.status_code == 401:
            logger.error("GHL auth failed (401) – check token and Version header.")
            return None
        if 400 <= r.status_code < 600:
            reason = ""
            try:
                reason = r.text
            except Exception:
                pass
            logger.error(f"GHL contact search failed: {r.status_code}. Reason: {reason}")
            return None
        data = r.json()

        _debug_write("ghl_contacts_search_resp.json", data)
        contacts = data.get('contacts') if isinstance(data, dict) else (data if isinstance(data, list) else [])
        return contacts[0] if contacts else None
    except requests.RequestException as e:
        logger.error(f"GHL contact search failed: {e}")
        return None
    
def convert_language_to_ghl_format(language_od_format: str) -> str:
    match language_od_format:
        case "eng":
            return "English"
        case "spa":
            return "Spanish"
        case _:
            return ""

def ghl_build_contact_payload(patient: Dict[str, Any], clinic_num: int, is_family_member: bool) -> Dict[str, Any]:
    # Required fields
    first = (patient.get("FName") or "").strip()
    last  = (patient.get("LName") or "").strip()
    email = valid_email_or_unknown(str(patient.get("Email") or ""))
    phone = normalize_phone_for_search(patient.get("WirelessPhone") or patient.get("WirelessPh") or patient.get("HmPhone") or "")
    dob   = (patient.get("Birthdate") or "").strip() or None  # prefer YYYY-MM-DD from OD
    address1 = (patient.get("Address") or "").strip()
    postal = (patient.get("Zip") or patient.get("PostalCode") or "").strip()
    gender = (patient.get("Gender") or "").strip()
    language = (patient.get("Language") or "").strip()

    payload: Dict[str, Any] = {
        "locationId": GHL_LOCATION_ID,
        "firstName": first,
        "lastName": last,
        "email": email,
        "phone": phone if not is_family_member else None,
        "dateOfBirth": dob,          # YYYY-MM-DD
        "address1": address1 or None,
        "postalCode": postal or None,
        "gender": gender or None,
    }

    payload['customFields'] = []

    if language:
        payload['customFields'].append(
            {
                "id": "hm8CJt0R3HVbZw2PWEhW",
                "key": "contact.language",
                "field_value": [convert_language_to_ghl_format(language)]
            },
        )

    if is_family_member and GHL_CUSTOM_FIELD_FAMILY_NUMBER_ID:
        family_field = {
            "id": GHL_CUSTOM_FIELD_FAMILY_NUMBER_ID,
            "field_value": phone
        }
        if GHL_CUSTOM_FIELD_FAMILY_NUMBER_KEY:
            family_field["key"] = GHL_CUSTOM_FIELD_FAMILY_NUMBER_KEY
        payload['customFields'].append(family_field)


    # Preferred: custom field for clinic
    if GHL_CUSTOM_FIELD_CLINIC_ID:
        payload["customFields"].append({"id": GHL_CUSTOM_FIELD_CLINIC_ID, "value": str(clinic_num)})
    else:
        # Fallback: tag the contact with clinic:<num>
        payload["tags"] = [f"clinic:{clinic_num}"]

    # Clean out null/empty list fields
    return {k: v for k, v in payload.items() if v not in (None, [], "")}

def ghl_upsert_contact(patient: Dict[str, Any], clinic_num: int, is_family_member: bool) -> Optional[str]:
    """
    POST /contacts/upsert
    """
    url = f"{GHL_API_BASE}/contacts/upsert"
    body = ghl_build_contact_payload(patient, clinic_num, is_family_member)

    try:
        r = get_session().post(url, headers=ghl_headers(), json=body, timeout=REQUEST_TIMEOUT)
        _debug_write("ghl_contact_upsert_req.json", {"url": url, "body": body})
        r.raise_for_status()
        j = r.json()
        _debug_write("ghl_contact_upsert_resp.json", j)
        return (j.get("contact") or {}).get("id") or j.get("id")
    except requests.RequestException as e:
        resp = getattr(e, 'response', None)
        resp_text = ''
        try:
            if resp is not None:
                resp_text = getattr(resp, 'text', '') or ''
        except Exception:
            resp_text = ''
        logger.error(f"GHL upsert contact failed: {e} body={resp_text}")
        return None

def ghl_tag_contact(contact_id: str, tag: str = "fromopendental") -> bool:
    """
    Tag the contact. Primary: POST /contacts/{id}/tags {"tags": ["fromopendental"]}
    Fallback: PATCH /contacts/{id} {"tags":["fromopendental"]}
    """
    if not contact_id:
        return False
    # Primary attempt
    url = f"{GHL_API_BASE}/contacts/{contact_id}/tags"
    body = {"tags": [tag]}
    try:
        r = get_session().post(url, headers=ghl_headers(), json=body, timeout=REQUEST_TIMEOUT)
        _debug_write("ghl_contact_tag_req.json", {"url": url, "body": body, "contactId": contact_id})
        if r.status_code in (200, 201, 204):
            return True
        # Try fallback if API variation
        if r.status_code in (404, 405, 400):
            raise requests.RequestException(f"Unsupported tag endpoint ({r.status_code})")
        r.raise_for_status()
        return True
    except Exception:
        # Fallback: PATCH contact with tags
        try:
            url2 = f"{GHL_API_BASE}/contacts/{contact_id}"
            body2 = {"tags": [tag]}
            r2 = get_session().patch(url2, headers=ghl_headers(), json=body2, timeout=REQUEST_TIMEOUT)
            _debug_write("ghl_contact_tag_fallback_req.json", {"url": url2, "body": body2, "contactId": contact_id})
            if r2.status_code in (200, 204):
                return True
            r2.raise_for_status()
            return True
        except requests.RequestException as e2:
            logger.warning(f"GHL tag contact failed for {contact_id}: {e2}")
            return False

def associate_contact_with_family(primary_contact_id: str, related_contact_ids: Iterable[str]) -> None:
    """Create GHL relations between the primary contact and related family members."""
    for related_id in related_contact_ids:
        ghl_create_contact_relation(primary_contact_id, related_id)

def ensure_family_relations_for_contact(primary_contact_id: str,
                                        family_patnums: Iterable[int],
                                        contact_map: Dict[int, Any]) -> None:
    if not primary_contact_id:
        return
    related_ids: Set[str] = set()
    fam_pat_list = list(family_patnums or [])
    for fam_pat in family_patnums or []:
        try:
            fam_pat_int = int(fam_pat)  # type: ignore[arg-type]
        except (TypeError, ValueError):
            continue
        entry = contact_map.get(fam_pat_int)
        if isinstance(entry, dict):
            fam_cid = entry.get("contactId")
            if fam_cid:
                related_ids.add(str(fam_cid))

    related_ids.discard(str(primary_contact_id))
    if not related_ids:
        logger.debug(
            "Family relation skip: no existing contacts yet for %s (family pats=%s)",
            primary_contact_id,
            fam_pat_list,
        )
        return

    logger.info(
        "Family relation candidates %s ↔ %s (family pats=%s)",
        primary_contact_id,
        sorted(related_ids),
        fam_pat_list,
    )
    associate_contact_with_family(str(primary_contact_id), related_ids)

def ghl_create_contact_relation(contact_a: str, contact_b: str) -> bool:
    """Link two contacts together using the configured GHL association."""
    global _FAMILY_RELATION_CONFIG_WARNED

    if not contact_a or not contact_b or contact_a == contact_b:
        return False

    if not GHL_FAMILY_ASSOCIATION_ID:
        if not _FAMILY_RELATION_CONFIG_WARNED:
            logger.error("GHL_FAMILY_ASSOCIATION_ID not set; cannot create family relations.")
            _FAMILY_RELATION_CONFIG_WARNED = True
        return False

    ordered = sorted((contact_a, contact_b))
    pair: Tuple[str, str] = (ordered[0], ordered[1])
    if pair in _FAMILY_RELATIONS_CREATED:
        return True

    payload = {
        "firstRecordId": pair[0],
        "secondRecordId": pair[1],
        "associationId": GHL_FAMILY_ASSOCIATION_ID,
        "locationId": GHL_LOCATION_ID,
    }

    url = f"{GHL_API_BASE}/associations/relations"
    logger.info(
        "Creating GHL relation %s ↔ %s using association %s",
        pair[0],
        pair[1],
        GHL_FAMILY_ASSOCIATION_ID,
    )
    try:
        r = get_session().post(url, headers=ghl_headers(), json=payload, timeout=REQUEST_TIMEOUT)
        _debug_write("ghl_create_relation_req.json", {"url": url, "payload": payload})
        resp_dump: Dict[str, Any]
        try:
            resp_json = r.json()
            resp_dump = {"status": r.status_code, "body": resp_json}
        except ValueError:
            resp_dump = {"status": r.status_code, "body": (r.text or "")[:1000]}
        _debug_write("ghl_create_relation_resp.json", resp_dump)
        if r.status_code in (200, 201):
            _FAMILY_RELATIONS_CREATED.add(pair)
            logger.info(f"＋ Linked family contacts {pair[0]} ↔ {pair[1]}")
            return True

        if r.status_code in (409, 422):
            try:
                body = r.json()
            except ValueError:
                body = r.text or ""
            message = str(body)
            if "exist" in message.lower():
                _FAMILY_RELATIONS_CREATED.add(pair)
                logger.debug(f"Family relation already exists for {pair[0]} ↔ {pair[1]}")
                return True

        logger.warning(
            "GHL relation response %s body=%s",
            r.status_code,
            (r.text or "")[:500],
        )
        r.raise_for_status()
        _FAMILY_RELATIONS_CREATED.add(pair)
        logger.info(f"＋ Linked family contacts {pair[0]} ↔ {pair[1]}")
        return True
    except requests.RequestException as exc:
        response = getattr(exc, "response", None)
        body_text = ""
        if response is not None:
            try:
                body_text = (response.text or "")[:500]
            except Exception:
                body_text = ""
        logger.warning(f"Failed to create family relation for {pair[0]} ↔ {pair[1]}: {exc} body={body_text}")
        return False

def map_appt_status_to_ghl(status: str) -> str:
    s = (status or "").strip().lower()
    if s == "broken" or s == "unschedlist":
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
    # NOTE: kept for completeness; not used in the unmapped-appointment path anymore.
    if not contact_id:
        return []
    url = f"{GHL_API_BASE}/contacts/{contact_id}/appointments"
    try:
        r = get_session().get(url, headers=ghl_headers(), timeout=REQUEST_TIMEOUT)
        _debug_write("ghl_contact_appts_req.json", {"url": url, "contactId": contact_id})
        r.raise_for_status()
        data = r.json()
        _debug_write("ghl_contact_appts_resp.json", data)
        if isinstance(data, list): return data
        return data.get('appointments', []) if isinstance(data, dict) else []
    except requests.RequestException as e:
        logger.error(f"GHL get contact appointments failed: {e}")
        return []

def pick_latest_same_day_event(contact_events: List[Dict[str, Any]],
                               od_start_local: datetime.datetime,
                               calendar_id: str) -> Optional[str]:
    """
    (Legacy) Not used in the new flow. Keeping for reference.
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

    same_day_events.sort(
        key=lambda e: (_updated_dt(e), _start_dt(e) or datetime.datetime.min.replace(tzinfo=timezone.utc)),
        reverse=True
    )
    chosen = same_day_events[0]
    _debug_write("ghl_same_day_pick.json", {"picked": chosen, "candidates": same_day_events[:25]})
    return _event_id(chosen)

def ghl_create_appointment(calendar_id: str, contact_id: str, assigned_user_id: Optional[str],
                           title: str, start_dt: datetime.datetime, end_dt: datetime.datetime,
                           status: str, note: Optional[str] = None) -> Optional[str]:
    
    if end_dt.weekday() == 5: # appointment on saturday is unconfirmed automatically
        status = "new"

    url = f"{GHL_API_BASE}/calendars/events/appointments"
    body = {
        "calendarId": calendar_id,
        "contactId": contact_id,
        "assignedUserId": assigned_user_id,
        "title": title or "Dental Appointment",
        "appointmentStatus": status,  # confirmed / cancelled / showed
        "startTime": to_utc_z(start_dt),   # send UTC Z
        "endTime":   to_utc_z(end_dt),     # send UTC Z
        "timeZone":  clinic_tz_name(),     # ensure proper rendering
        "ignoreFreeSlotValidation": True,
        "locationId": GHL_LOCATION_ID
    }

    if note:
        add_tag_patient(contact_id, note)

    try:
        r = get_session().post(url, headers=ghl_headers(), json=body, timeout=REQUEST_TIMEOUT)
        _debug_write("ghl_create_event_req.json", {"url": url, "body": body})
        r.raise_for_status()
        j = r.json()
        _debug_write("ghl_create_event_resp.json", j)
        return j.get('id') or j.get('eventId') or j.get('appointmentId')
    except requests.RequestException as e:
        resp_text = getattr(getattr(e, 'response', None), 'text', '') or ''
        logger.error(f"GHL create appointment failed: {e} body={resp_text}")
        return None
    
def add_tag_patient(contact_id: str, note: str):
    url = f"{GHL_API_BASE}/contacts/"+str(contact_id)+"/tags"

    body = {
        "tags": [note]
    }

    try:
        r = get_session().post(url, headers=ghl_headers(), json=body, timeout=REQUEST_TIMEOUT)
        _debug_write("ghl_create_event_req.json", {"url": url, "body": body})
        r.raise_for_status()
        j = r.json()
        _debug_write("ghl_create_event_resp.json", j)
        return j.get('tags')
    except requests.RequestException as e:
        resp_text = getattr(getattr(e, 'response', None), 'text', '') or ''
        logger.error(f"GHL add tag failed: {e} body={resp_text}")
        return None

def ghl_update_appointment(event_id: str, calendar_id: str, assigned_user_id: Optional[str],
                           title: str, start_dt: datetime.datetime, end_dt: datetime.datetime,
                           status: str, note: Optional[str] = None, 
                           contact_id: Optional[str] = None) -> Tuple[bool, Optional[int], str]:
    if not event_id:
        return False, None, "missing event_id"
    url = f"{GHL_API_BASE}/calendars/events/appointments/{event_id}"
    body = {
        "calendarId": calendar_id,
        "assignedUserId": assigned_user_id,
        "title": title or "Dental Appointment",
        "appointmentStatus": status,
        "startTime": to_utc_z(start_dt),
        "endTime":   to_utc_z(end_dt),
        "timeZone":  clinic_tz_name(),
        "ignoreFreeSlotValidation": True,
        "locationId": GHL_LOCATION_ID
    }

    if note and contact_id:
        add_tag_patient(contact_id, note)

    try:
        r = get_session().put(url, headers=ghl_headers(), json=body, timeout=REQUEST_TIMEOUT)
        _debug_write("ghl_update_event_req.json", {"url": url, "body": body, "eventId": event_id})
        if 200 <= r.status_code < 300:
            return True, r.status_code, ""
        err_txt = ""
        try:
            err_txt = r.text
        except Exception:
            pass
        logger.error(f"GHL update appointment failed: HTTP {r.status_code} body={err_txt}")
        return False, r.status_code, err_txt
    except requests.RequestException as e:
        code = getattr(getattr(e, "response", None), "status_code", None)
        body = getattr(getattr(e, "response", None), "text", None) or str(e)
        logger.error(f"GHL update appointment failed: {e} body={body}")
        return False, code, body
    
def update_od_appt_to_confirmed(appt_id: str) -> bool:
    url = f"{API_BASE_URL}/appointments/{appt_id}/Confirm"
    body = {
        "defNum": CONFIRMATION_STATUS_APPT_VALUE
    }
    try:
        r = get_session().put(url, headers=od_headers(), json=body, timeout=REQUEST_TIMEOUT)
        _debug_write(f"od_appt_confirm_req.json", {"url": url, "body": body})
        if 200 <= r.status_code < 300:
            return True
        err_txt = ""
        try:
            err_txt = r.text
        except Exception:
            pass
        logger.error(f"OD update appointment confirmation status failed: HTTP {r.status_code} body={err_txt}")
        return False
    except requests.RequestException as e:
        body = getattr(getattr(e, "response", None), "text", None) or str(e)
        logger.error(f"OD update appointment confirmation status failed: {e} body={body}")
        return False

def ghl_update_appt_status(event_id: str, status: str, note: Optional[str] = None) -> Tuple[bool, Optional[int], str]:
    if not event_id:
        return False, None, "missing event_id"
    url = f"{GHL_API_BASE}/calendars/events/appointments/{event_id}"
    body = {
        "appointmentStatus": status,
        "ignoreFreeSlotValidation": True,
        "locationId": GHL_LOCATION_ID,
        "toNotify": False
    }

    try:
        r = get_session().put(url, headers=ghl_headers(), json=body, timeout=REQUEST_TIMEOUT)
        _debug_write("ghl_update_event_req.json", {"url": url, "body": body, "eventId": event_id})
        if 200 <= r.status_code < 300:
            return True, r.status_code, ""
        err_txt = ""
        try:
            err_txt = r.text
        except Exception:
            pass
        logger.error(f"GHL update appointment status failed: HTTP {r.status_code} body={err_txt}")
        return False, r.status_code, err_txt
    except requests.RequestException as e:
        code = getattr(getattr(e, "response", None), "status_code", None)
        body = getattr(getattr(e, "response", None), "text", None) or str(e)
        logger.error(f"GHL update appointment status failed: {e} body={body}")
        return False, code, body

# =========================
# === OPPORTUNITY MATCHING =
# =========================
def normalize_name_for_match(name: str) -> str:
    """Normalize a name for fuzzy matching: lowercase, strip whitespace, remove special chars."""
    if not name:
        return ""
    # Lowercase, strip, and remove extra whitespace
    normalized = re.sub(r'\s+', ' ', (name or '').strip().lower())
    # Remove special characters but keep spaces
    normalized = re.sub(r'[^a-z0-9\s]', '', normalized)
    return normalized.strip()


def names_match(name1: str, name2: str) -> bool:
    """Check if two names match (case-insensitive, ignoring extra whitespace/special chars)."""
    n1 = normalize_name_for_match(name1)
    n2 = normalize_name_for_match(name2)
    if not n1 or not n2:
        return False
    return n1 == n2


def fetch_all_appointments_for_clinic_opportunity_match(clinic: int, start: datetime.datetime, end: datetime.datetime) -> List[Dict[str, Any]]:
    """
    Fetch ALL appointments for Congress Ortho Clinic (9035) regardless of operatory,
    for opportunity matching purposes.
    """
    params_base = {
        'ClinicNum': str(clinic),
        'dateStart': start.astimezone(timezone.utc).date().strftime('%Y-%m-%d'),
        'dateEnd': end.astimezone(timezone.utc).date().strftime('%Y-%m-%d'),
        'fields': ','.join(REQUIRED_APPOINTMENT_FIELDS),
    }
    all_appts: List[Dict[str, Any]] = []

    logger.debug(
        f"Clinic {clinic}: fetching ALL appointments for opportunity matching "
        f"{params_base['dateStart']}..{params_base['dateEnd']}"
    )

    for status in VALID_STATUSES:
        p = dict(params_base)
        p['AptStatus'] = status
        chunk = od_get_paginated('appointments', p) or []
        logger.debug(f"Clinic {clinic} opportunity fetch: AptStatus={status} -> {len(chunk)} returned")
        all_appts.extend(chunk)
        time.sleep(OD_RATE_LIMIT_SECONDS)

    # De-dupe
    uniq: Dict[Tuple[str, str, str], Dict[str, Any]] = {}
    for a in all_appts:
        key = (str(a.get('AptNum', '')), str(a.get('PatNum', '')), a.get('AptDateTime', ''))
        uniq[key] = a
    result = list(uniq.values())

    logger.info(f"Clinic {clinic}: ALL appointments for opportunity matching: raw={len(all_appts)} after-de-dupe={len(result)}")
    return result


def match_appointments_to_opportunities(
    appts: List[Dict[str, Any]],
    opps: Dict[str, Any],
    patients: Dict[int, Dict[str, Any]]
) -> Tuple[List[Tuple[Dict[str, Any], str]], Dict[str, Any]]:
    """
    Match appointments to opportunities based on patient name vs contact name.
    Only match if the operatory is NOT in CONGRESS_ORTHO_BLOCKED_OPERATORIES.
    
    Args:
        appts: List of appointment dictionaries
        opps: Opportunities dictionary (opp_id -> opp_data)
        contact_map: PatNum -> contact info mapping
        patients: PatNum -> patient info dictionary (for name lookup)
    
    Returns:
        - List of (appointment, opportunity_id) tuples that matched
        - Remaining opportunities dict (unmatched)
    """
    matched: List[Tuple[Dict[str, Any], str]] = []
    remaining_opps = dict(opps)  # Copy to avoid mutating original during iteration

    for appt in appts:
        # Check operatory - must NOT be in blocked list
        op_num = appt.get('Op') or appt.get('OperatoryNum') or 0
        try:
            op_num_int = int(op_num)
        except (TypeError, ValueError):
            op_num_int = 0

        if op_num_int in CONGRESS_ORTHO_BLOCKED_OPERATORIES:
            continue

        # Get patient info from patients dict using PatNum
        pat_num = appt.get('PatNum')
        pat_num_int: Optional[int] = None
        if pat_num is not None:
            try:
                pat_num_int = int(pat_num)
            except (TypeError, ValueError):
                pass

        # Build patient full name from patients dict (preferred) or fallback to appt fields
        patient_info = patients.get(pat_num_int, {}) if pat_num_int else {}
        first = (patient_info.get('FName') or appt.get('FName') or '').strip()
        last = (patient_info.get('LName') or appt.get('LName') or '').strip()
        patient_name = f"{first} {last}".strip()

        if not patient_name:
            logger.debug(f"Skipping appt {appt.get('AptNum')} - no patient name found (PatNum={pat_num})")
            continue

        # Try to match against each opportunity
        for opp_id, opp_data in list(remaining_opps.items()):
            if not isinstance(opp_data, dict):
                continue

            contact_name = opp_data.get('contact_name', '')
            if not contact_name:
                continue

            if names_match(patient_name, contact_name):
                matched.append((appt, opp_id))
                # Remove from remaining
                del remaining_opps[opp_id]
                logger.info(
                    f"Opportunity MATCH: AptNum {appt.get('AptNum')} patient '{patient_name}' "
                    f"matched opportunity '{opp_id}' contact '{contact_name}'"
                )
                break  # One appointment can only match one opportunity

    return matched, remaining_opps


def tag_contact_won(contact_id: str) -> bool:
    """Tag the contact with 'Won' tag."""
    return ghl_tag_contact(contact_id, "Won")


def process_opportunity_matches(
    matched: List[Tuple[Dict[str, Any], str]],
    opps: Dict[str, Any],
    contact_map: Dict[int, Any]
) -> Dict[str, Any]:
    """
    Process matched appointments:
    - Tag the contact with 'Won'
    - Remove the opportunity from the dictionary
    
    Returns the updated opportunities dictionary.
    """
    updated_opps = dict(opps)

    for appt, opp_id in matched:
        # Get contact_id from opportunity data or from contact_map
        opp_data = opps.get(opp_id, {})
        contact_id = opp_data.get('contact_id')

        # If no contact_id in opportunity, try to find via PatNum in contact_map
        if not contact_id:
            pat_num = appt.get('PatNum')
            if pat_num:
                try:
                    pat_num_int = int(pat_num)
                    contact_entry = contact_map.get(pat_num_int)
                    if isinstance(contact_entry, dict):
                        contact_id = contact_entry.get('contactId')
                except (TypeError, ValueError):
                    pass

        # Tag contact with "Won"
        if contact_id:
            success = tag_contact_won(contact_id)
            if success:
                logger.info(f"Tagged contact {contact_id} with 'Won' for opportunity {opp_id}")
            else:
                logger.warning(f"Failed to tag contact {contact_id} with 'Won' for opportunity {opp_id}")
        else:
            logger.warning(f"No contact_id found for opportunity {opp_id}, cannot tag 'Won'")

        # Remove opportunity from dict
        if opp_id in updated_opps:
            del updated_opps[opp_id]
            logger.info(f"Removed matched opportunity {opp_id} from opportunities_to_match")

    return updated_opps


def cleanup_old_opportunities(opps: Dict[str, Any], max_age_days: int = 30) -> Dict[str, Any]:
    """
    Remove opportunities older than max_age_days from the dictionary.
    Uses the 'opportunity_date' field from each opportunity.
    """
    if not opps:
        return {}

    now = datetime.datetime.now(tz=timezone.utc)
    cutoff = now - timedelta(days=max_age_days)
    cleaned_opps: Dict[str, Any] = {}
    removed_count = 0

    for opp_id, opp_data in opps.items():
        if not isinstance(opp_data, dict):
            cleaned_opps[opp_id] = opp_data
            continue

        opp_date_raw = opp_data.get('opportunity_date')
        if not opp_date_raw:
            # No date, keep it
            cleaned_opps[opp_id] = opp_data
            continue

        # Parse opportunity date
        opp_date: Optional[datetime.datetime] = None
        try:
            if isinstance(opp_date_raw, str):
                # Try mm/dd/YYYY format first (expected format from GHL)
                try:
                    opp_date = datetime.datetime.strptime(opp_date_raw, "%m/%d/%Y").replace(tzinfo=timezone.utc)
                except ValueError:
                    # Fall back to ISO format
                    opp_date = datetime.datetime.fromisoformat(opp_date_raw.replace('Z', '+00:00'))
            elif isinstance(opp_date_raw, datetime.datetime):
                opp_date = opp_date_raw
            elif isinstance(opp_date_raw, datetime.date):
                opp_date = datetime.datetime.combine(opp_date_raw, datetime.time.min, tzinfo=timezone.utc)
        except Exception as e:
            logger.debug(f"Could not parse opportunity_date '{opp_date_raw}' for {opp_id}: {e}")
            cleaned_opps[opp_id] = opp_data
            continue

        if opp_date is None:
            cleaned_opps[opp_id] = opp_data
            continue

        # Ensure timezone-aware
        if opp_date.tzinfo is None:
            opp_date = opp_date.replace(tzinfo=timezone.utc)

        # Check if older than cutoff
        if opp_date < cutoff:
            removed_count += 1
            logger.info(f"Removing old opportunity {opp_id} (date: {opp_date.date()}, older than {max_age_days} days)")
        else:
            cleaned_opps[opp_id] = opp_data

    if removed_count > 0:
        logger.info(f"Removed {removed_count} opportunities older than {max_age_days} days")

    return cleaned_opps


def save_opportunities_to_drive(opps: Dict[str, Any]) -> bool:
    """
    Save the opportunities_to_match dictionary back to Google Drive.
    Uses the GOOGLE_DRIVE_FILE_ID_OPPORTUNITIES file id.
    """
    file_id = GOOGLE_DRIVE_FILE_ID_OPPORTUNITIES
    if not file_id:
        logger.warning("GOOGLE_DRIVE_FILE_ID_OPPORTUNITIES not set; cannot save opportunities to Drive")
        return False

    try:
        content = json.dumps(opps, indent=2, default=str).encode('utf-8')
        success = DRIVE.update_file_by_id(file_id, content, mime="application/json")
        if success:
            logger.info(f"Saved {len(opps)} opportunities to Drive file {file_id}")
        else:
            logger.warning(f"Failed to save opportunities to Drive file {file_id}")
        return success
    except Exception as e:
        logger.error(f"Error saving opportunities to Drive: {e}")
        return False


def process_congress_ortho_opportunity_matching(
    start: datetime.datetime,
    end: datetime.datetime,
    contact_map: Dict[int, Any]
) -> None:
    """
    Main function to process opportunity matching for Congress Ortho Clinic (9035).
    - Fetches all appointments from clinic 9035 regardless of operatory
    - Matches against opportunities_to_match
    - Tags matched contacts with 'Won'
    - Updates the global opportunities_to_match
    """
    global opportunities_to_match

    if not opportunities_to_match:
        logger.info("No opportunities to match against")
        return

    logger.info(f"Starting opportunity matching for Congress Ortho (clinic {CONGRESS_ORTHO_CLINIC})")
    logger.info(f"Opportunities to match: {len(opportunities_to_match)}")

    # Fetch all appointments for clinic 9035
    all_appts = fetch_all_appointments_for_clinic_opportunity_match(CONGRESS_ORTHO_CLINIC, start, end)

    if not all_appts:
        logger.info(f"No appointments found for clinic {CONGRESS_ORTHO_CLINIC} for opportunity matching")
        return

    # Collect unique PatNums and fetch patient info for name lookup
    pat_nums_to_fetch: Set[int] = set()
    for appt in all_appts:
        pat_num = appt.get('PatNum')
        if pat_num:
            try:
                pat_nums_to_fetch.add(int(pat_num))
            except (TypeError, ValueError):
                pass

    # Load patient cache and fetch missing patients or patients without names
    patient_cache = load_patient_cache()
    patients: Dict[int, Dict[str, Any]] = {}
    
    for pn in sorted(pat_nums_to_fetch):
        cached = patient_cache.get(pn)
        # Check if cache has the patient AND has FName/LName - if not, fetch from OD
        has_name = (
            cached is not None 
            and isinstance(cached, dict) 
            and (cached.get('FName') or cached.get('LName'))
        )
        
        if has_name:
            patients[pn] = dict(cached)
        else:
            # Fetch from OpenDental (either not in cache, or cache missing name fields)
            data = od_get(f"patients/{pn}", {}) or []
            pat = (data[0] if isinstance(data, list) and data else (data if isinstance(data, dict) else {}))
            patients[pn] = dict(pat) if isinstance(pat, dict) else {}
            # Update cache with fetched data (merge if existing)
            if cached is not None and isinstance(cached, dict):
                # Merge: keep existing fields, update with new data
                merged = dict(cached)
                merged.update(patients[pn])
                patient_cache[pn] = merged
                patients[pn] = merged
            else:
                patient_cache[pn] = dict(patients[pn])
            time.sleep(OD_RATE_LIMIT_SECONDS)

    # Save updated patient cache
    save_patient_cache(patient_cache)
    logger.info(f"Loaded patient info for {len(patients)} patients for opportunity matching")

    # Match appointments to opportunities
    matched, remaining = match_appointments_to_opportunities(all_appts, opportunities_to_match, patients)

    if matched:
        logger.info(f"Found {len(matched)} opportunity matches")
        # Process matches (tag contacts, remove from dict)
        opportunities_to_match = process_opportunity_matches(matched, opportunities_to_match, contact_map)
    else:
        logger.info("No opportunity matches found")


def finalize_opportunities() -> None:
    """
    Called at shutdown to:
    - Clean up opportunities older than 30 days
    - Save the final opportunities_to_match to Google Drive
    """
    global opportunities_to_match

    logger.info("Finalizing opportunities processing...")

    # Clean up old opportunities (older than 30 days)
    opportunities_to_match = cleanup_old_opportunities(opportunities_to_match, max_age_days=30)

    # Save to Google Drive
    if opportunities_to_match is not None:
        save_opportunities_to_drive(opportunities_to_match)
        logger.info(f"Final opportunities count after cleanup: {len(opportunities_to_match)}")
    else:
        logger.info("No opportunities to save")


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
    # Local only (you already stopped caching in GitHub Actions)
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

def load_ghl2od_map() -> Dict[str, str]:
    """
    Load mapping from ghl_od_appointments_map.json:
      {
        "<eventId>": { "aptNum": "3821874", "patNum": "...", ... },
        ...
      }
    Returns AptNum -> EventId for fast lookup.
    """
    data = load_json_or(GHL2OD_APPTS_FILE, {})
    apt_to_event: Dict[str, str] = {}
    if isinstance(data, dict):
        for event_id, info in data.items():
            if not isinstance(info, dict):
                continue
            apt = str(info.get("aptNum", "")).strip()
            if apt:
                apt_to_event[apt] = event_id
    return apt_to_event

def save_ghl2od_entry_updated_at(event_id: str):
    """
    Update only the 'updatedAt' field for the given event in ghl_od_appointments_map.json,
    preserving the file's existing structure/fields exactly, then push to Drive.
    """
    data = load_json_or(GHL2OD_APPTS_FILE, {})
    if isinstance(data, dict) and event_id in data and isinstance(data[event_id], dict):
        data[event_id]["updatedAt"] = datetime.datetime.utcnow().replace(tzinfo=timezone.utc).isoformat()
        atomic_save_json(GHL2OD_APPTS_FILE, data)
        DRIVE.push(GHL2OD_APPTS_FILE)

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
        logger.warning("GHL_CUSTOM_FIELD_CLINIC_ID not set: falling back to tagging clinic:<num> on contact.")
    if not GHL_FAMILY_ASSOCIATION_ID:
        errs.append("Missing GHL_FAMILY_ASSOCIATION_ID (required for family contact relations)")
    if errs:
        logger.error("Config validation failed:")
        for e in errs: logger.error(" - " + e)
        return False
    return True

def ghl_contact_payload_from_patient_like(first: str, last: str, email: str, phone: str, language) -> Dict[str, Any]:
    return {"FName": first, "LName": last, "Email": email, "WirelessPhone": phone, "Language": language}

def ensure_contact_id(first: str, last: str, email: str, phone: str,
                      pat_num: int,
                      clinic_num: int,
                      contact_map: Dict[int, Any],
                      language: str,
                      is_family: bool = False,
                      family_patnums: Optional[Iterable[int]] = None) -> Tuple[Optional[str], bool]:
    """
    Returns (contact_id, is_new_contact).
    Path:
      1) If PatNum is already mapped -> return that id (is_new=False)
      2) Else search by normalized phone (skip if unusable)
      3) Else upsert (create) and return id (is_new=True)
    """
    # 1) PatNum fast path
    mapped = contact_map.get(pat_num)
    if mapped and mapped.get('contactId'):
        return mapped['contactId'], False

    # 2) phone search
    phone_norm = normalize_phone_for_search(phone)
    found = ghl_search_contact_by_phone(phone_norm) if phone_norm else None
    is_family_member = is_family
    related_contact_candidates: Set[str] = set()

    if found and found.get('id'):
        found_id = str(found.get('id'))
        last_name_raw = (found.get('lastNameLowerCase') or '').strip()
        ghl_contact_lastname = {word.replace(" ", "") for word in last_name_raw.split() if word}

        od_last_raw = (last or '').strip().lower()
        od_contact_lastname = {word.replace(" ", "") for word in od_last_raw.split() if word}

        first_name_raw = (found.get('firstNameLowerCase') or '').replace(" ", "")
        od_contact_firstname = (first or '').strip().lower().replace(" ", "")

        if first_name_raw == od_contact_firstname and od_contact_lastname == ghl_contact_lastname:
            cid = found_id
            contact_map[pat_num] = {
                "contactId": cid,
                "phone": phone_norm,
                "email": valid_email_or_unknown(email),
                "updatedAt": datetime.datetime.utcnow().replace(tzinfo=timezone.utc).isoformat()
            }
            return cid, False
        
        if first_name_raw != od_contact_firstname and ghl_contact_lastname.intersection(od_contact_lastname):
            is_family_member = True
            related_contact_candidates.add(found_id)

    # 3) create with full payload
    patient_like = ghl_contact_payload_from_patient_like(first, last, email, phone, language)
    cid = ghl_upsert_contact(patient_like, clinic_num, is_family_member)
    if cid:
        cid_str = str(cid)
        contact_map[pat_num] = {
            "contactId": cid_str,
            "phone": phone_norm,
            "email": valid_email_or_unknown(email),
            "updatedAt": datetime.datetime.utcnow().replace(tzinfo=timezone.utc).isoformat()
        }

        if is_family_member:
            related_ids: Set[str] = set(related_contact_candidates)
            if family_patnums:
                for fam_pat in family_patnums:
                    try:
                        fam_pat_int = int(fam_pat)  # type: ignore[arg-type]
                    except (TypeError, ValueError):
                        continue
                    if fam_pat_int == pat_num:
                        continue
                    existing_entry = contact_map.get(fam_pat_int)
                    if isinstance(existing_entry, dict):
                        fam_cid = existing_entry.get("contactId")
                        if fam_cid and fam_cid != cid_str:
                            related_ids.add(str(fam_cid))
            related_ids.discard(cid_str)
            if related_ids:
                associate_contact_with_family(cid_str, related_ids)

        return cid_str, True

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
              - **NEW**: look up AptNum in ghl_od_appointments_map.json (Drive-mirrored).
                If found, UPDATE that eventId and refresh updatedAt in that file; also update main map.
                If none → CREATE a new event.
          * store AptNum -> eventId mapping for future updates.
      - After **every successful CREATE or UPDATE**, tag the contact with **fromopendental**.
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
    language = (p.get('Language') or '').strip()

    # clinic routing
    calendar_id = pick_calendar_id(clinic)
    assigned_user_id = pick_assigned_user_id(clinic)
    if not calendar_id:
        logger.error(f"Clinic {clinic}: missing calendar mapping")
        return None

    # Flag family members so the contact payload can omit direct phone and rely on the shared custom field
    is_family = bool(p.get('is_family') or appt.get('is_family'))

    family_members_patnums: List[int] = []
    family_members_source = p.get('family_members') or appt.get('family_members')
    if isinstance(family_members_source, (list, tuple, set)):
        for member in family_members_source:
            try:
                family_members_patnums.append(int(member))
            except (TypeError, ValueError):
                continue

    # Ensure / create contact, capture whether it's new
    contact_id, is_new_contact = ensure_contact_id(
        first,
        last,
        email,
        phone,
        pat_num,
        clinic,
        contact_map,
        language,
        is_family,
        family_members_patnums,
    )
    if not contact_id:
        logger.error(f"Apt {apt_num}: failed to ensure contact")
        return None

    if is_family and family_members_patnums:
        ensure_family_relations_for_contact(contact_id, family_members_patnums, contact_map)

    title = f"{first} {last}".strip() or "Dental Appointment"
    status = map_appt_status_to_ghl(appt.get('AptStatus', ''))
    ghl_note = appt.get('_ghl_note')

    # ===== If AptNum already mapped → update exact event =====
    mapped = ghl_map.get(apt_num)
    if mapped and mapped.get('eventId'):
        event_id = mapped['eventId']
        ok, code, msg = ghl_update_appointment(event_id, calendar_id, assigned_user_id, title, start_local, end_local, status, ghl_note, contact_id)
        if ok:
            mapped.update({"contactId": contact_id, "calendarId": calendar_id, "clinic": clinic})
            ghl_map[apt_num] = mapped
            logger.info(f"✓ Updated mapped event {event_id} for AptNum {apt_num}")
            # Tag the contact on update
            ghl_tag_contact(contact_id, "fromopendental")
            return event_id
        else:
            dlq_record(apt_num, clinic, event_id, contact_id, code, msg)
            logger.error(f"Apt {apt_num}: mapped update failed (HTTP {code}). NOT creating a new event. Logged to DLQ.")
            return None

    # ===== Not mapped =====
    if is_new_contact:
        # brand-new contact → no need to fetch appointments; just create
        new_event_id = ghl_create_appointment(calendar_id, contact_id, assigned_user_id, title, start_local, end_local, status, ghl_note)
        if new_event_id:
            ghl_map[apt_num] = {"contactId": contact_id, "eventId": new_event_id, "calendarId": calendar_id, "clinic": clinic}
            logger.info(f"＋ Created event {new_event_id} for NEW contact, AptNum {apt_num}")
            # Tag the contact on create
            ghl_tag_contact(contact_id, "fromopendental")
            return new_event_id
        logger.error(f"Apt {apt_num}: failed to create event for new contact")
        return None

    # Existing contact (but AptNum not mapped) → check the Drive-mirrored reconciliation file
    ghl2od_map = load_ghl2od_map()
    candidate_event_id = ghl2od_map.get(apt_num)

    if candidate_event_id:
        ok, code, msg = ghl_update_appointment(candidate_event_id, calendar_id, assigned_user_id, title, start_local, end_local, status, ghl_note, contact_id)
        if ok:
            # Update main map (memory; persisted via save_state())
            ghl_map[apt_num] = {"contactId": contact_id, "eventId": candidate_event_id, "calendarId": calendar_id, "clinic": clinic}
            logger.info(f"✓ Reconciled via {GHL2OD_APPTS_FILE} event {candidate_event_id} for AptNum {apt_num}")
            # Tag the contact on update
            ghl_tag_contact(contact_id, "fromopendental")
            # Refresh updatedAt in reconciliation file and push
            save_ghl2od_entry_updated_at(candidate_event_id)
            return candidate_event_id
        else:
            dlq_record(apt_num, clinic, candidate_event_id, contact_id, code, msg)
            logger.error(f"Apt {apt_num}: update via {GHL2OD_APPTS_FILE} failed (HTTP {code}). NOT creating new. Logged to DLQ.")
            return None

    # Not found in reconciliation file → create new
    new_event_id = ghl_create_appointment(calendar_id, contact_id, assigned_user_id, title, start_local, end_local, status, ghl_note)
    if new_event_id:
        ghl_map[apt_num] = {"contactId": contact_id, "eventId": new_event_id, "calendarId": calendar_id, "clinic": clinic}
        logger.info(f"＋ Created event {new_event_id} for AptNum {apt_num}")
        # Tag the contact on create
        ghl_tag_contact(contact_id, "fromopendental")
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

    # Connect Drive and seed state locally if needed
    DRIVE.connect()
    pull_all_from_drive()

    global opportunities_to_match
    opportunities_to_match = load_opportunities_to_match_from_drive()

    # Load appt types cache once (from repo file next to this script)
    global _APPT_TYPE_BY_NUM, _APPT_TYPES_LOADED
    if not _APPT_TYPES_LOADED:
        _APPT_TYPE_BY_NUM = load_appointment_types_cache(APPT_TYPES_CACHE_PATH)
        _APPT_TYPES_LOADED = True
        if not _APPT_TYPE_BY_NUM:
            logger.warning("appointment_types_cache.json not found or empty; Broken-type filtering may skip unknowns.")

    last_sync = load_last_sync()                  # clinic → iso str
    sent_map = load_json_or(SENT_APPTS_FILE, {})  # AptNum → {last_sent_tstamp}
    snapshot = load_json_or(APPT_SNAPSHOT_FILE, {})
    ghl_map = load_json_or(GHL_MAP_FILE, {})
    contact_map = load_contact_map()
    global _FAILED_UPDATES
    _FAILED_UPDATES = load_failed_updates()

    patient_cache = load_patient_cache()
    total = 0

    for idx, clinic in enumerate(CLINIC_NUMS):
        if idx > 0:
            time.sleep(CLINIC_DELAY_SECONDS)

        start, end, is_incr = generate_window(last_sync.get(clinic), force_deep_sync)
        logger.info(f"Clinic {clinic}: window {start} → {end} ({'incremental' if is_incr else 'deep'})")

        appts, special_saturday_appts = fetch_appointments_for_window(clinic, start, end)
        if not appts and not special_saturday_appts:
            logger.info(f"Clinic {clinic}: no appointments in window.")
            continue
        
        unique_patients: Set[int] = set()
        saturday_payments: Dict[str, Dict[str, Any]] = {}
        patient_saturday_appts: Dict[int, List[str]] = {}

        for a in appts + special_saturday_appts:
            apt_num_raw = a.get('AptNum')
            pat_raw = a.get('PatNum')
            apt_dt_raw = a.get('AptDateTime')
            if not apt_num_raw or not pat_raw or not apt_dt_raw:
                continue
            try:
                pat_num_int = int(pat_raw)
            except (TypeError, ValueError):
                continue

            appt_dt_iso = str(apt_dt_raw).replace('Z', '+00:00')
            try:
                appt_dt = datetime.datetime.fromisoformat(appt_dt_iso)
            except ValueError:
                continue

            if appt_dt.weekday() != 5:
                continue  # Not a Saturday appointment

            apt_num = str(apt_num_raw)
            unique_patients.add(pat_num_int)
            saturday_payments[apt_num] = {
                'PatNum': pat_num_int,
                'ApptDateTime': apt_dt_raw,
                'Paid': False
            }
            patient_saturday_appts.setdefault(pat_num_int, []).append(apt_num)

        patients_to_fetch: List[int] = []
        for pat in sorted(unique_patients):
            patients_to_fetch.append(pat)

        # payment match algorithm
        for p in patients_to_fetch:
            payments_patient = od_get(f"accountmodules/{p}/ServiceDateView", {}) or []
            if payments_patient is None:
                payments_patient = []
            if not isinstance(payments_patient, list):
                payments_patient = [payments_patient]

            patient_appts = patient_saturday_appts.get(p, [])
            if not patient_appts:
                continue

            for apt_id in patient_appts:
                appt_info = saturday_payments.get(apt_id)
                if not appt_info:
                    continue
                appt_dt_raw = appt_info.get('ApptDateTime')
                if not appt_dt_raw:
                    continue
                try:
                    appt_dt = datetime.datetime.fromisoformat(str(appt_dt_raw).replace('Z', '+00:00'))
                except ValueError:
                    continue

                best_candidate: Optional[Tuple[int, datetime.datetime]] = None
                for idx, payment in enumerate(list(payments_patient)):
                    if not isinstance(payment, dict):
                        continue
                    credit_val = payment.get('Credit', 0)
                    try:
                        credit_int = int(float(credit_val))
                    except (TypeError, ValueError):
                        continue
                    if credit_int != SATURDAY_PREPAYMENT_AMOUNT:
                        continue
                    reference = str(payment.get('Reference', '') or '').lower()
                    if 'prepay' not in reference:
                        continue
                    trans_raw = payment.get('TransDate')
                    if not trans_raw:
                        continue
                    try:
                        trans_dt = datetime.datetime.fromisoformat(str(trans_raw).replace('Z', '+00:00'))
                    except ValueError:
                        continue
                    if trans_dt <= appt_dt:
                        if not best_candidate or trans_dt > best_candidate[1]:
                            best_candidate = (idx, trans_dt)

                if best_candidate:
                    idx_to_pop, _ = best_candidate
                    saturday_payments[apt_id]['Paid'] = True
                    payments_patient.pop(idx_to_pop)

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

        if not to_process and not special_saturday_appts:
            logger.info(f"Clinic {clinic}: nothing new/updated to send.")
            continue

        # Pull minimal patient info (prefer cache; if missing, fetch)
        patients: Dict[int, Dict[str, Any]] = {}
        for pn in sorted(pat_nums):
            cached = patient_cache.get(pn)
            if cached is not None:
                patients[pn] = dict(cached)
            else:
                data = od_get(f"patients/{pn}", {}) or []
                pat = (data[0] if isinstance(data, list) and data else (data if isinstance(data, dict) else {}))
                patients[pn] = dict(pat) if isinstance(pat, dict) else {}
                patient_cache[pn] = dict(patients[pn])
                # be gentle between OD patient fetches
                time.sleep(OD_RATE_LIMIT_SECONDS)

        family_pat_nums, family_groups = detect_family_patients(patients)

        for pn, info in patients.items():
            is_family_flag = pn in family_pat_nums
            info['is_family'] = is_family_flag

            if pn in patient_cache:
                patient_cache[pn]['is_family'] = is_family_flag

            info.pop('family_members', None)
            info.pop('family_key', None)
            if pn in patient_cache:
                patient_cache[pn].pop('family_members', None)
                patient_cache[pn].pop('family_key', None)

            if not is_family_flag:
                continue

            family_key = None
            family_group: Optional[Set[int]] = None
            for key, group in family_groups.items():
                if pn in group:
                    family_key = key
                    family_group = group
                    break

            if family_group:
                others = sorted(member for member in family_group if member != pn)
                if others:
                    info['family_members'] = others
                    if pn in patient_cache:
                        patient_cache[pn]['family_members'] = others
                if family_key:
                    info['family_key'] = family_key
                    if pn in patient_cache:
                        patient_cache[pn]['family_key'] = family_key

        for appt in appts:
            patnum_raw = appt.get('PatNum')
            patnum_int: Optional[int] = None
            if patnum_raw is not None:
                try:
                    patnum_int = int(patnum_raw)
                except (TypeError, ValueError):
                    patnum_int = None
            appt['is_family'] = bool(patnum_int and patnum_int in family_pat_nums)
            if patnum_int and patnum_int in patients:
                fam_members = patients[patnum_int].get('family_members')
                if fam_members:
                    appt['family_members'] = list(fam_members)

        # Process each
        successes = 0
        max_success_ts: Optional[datetime.datetime] = None
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
                successes += 1
                ts = parse_time(a.get('DateTStamp', ''))
                if ts and (max_success_ts is None or ts > max_success_ts):
                    max_success_ts = ts

        # advance last_sync only if we had at least one success; use max successful DateTStamp
        if successes > 0 and max_success_ts:
            last_sync[clinic] = max_success_ts.astimezone(timezone.utc).isoformat()

        for appt_id, saturday_appt in saturday_payments.items():
            map_entry = ghl_map.get(appt_id)
            event_id: Optional[str] = None
            if isinstance(map_entry, dict):
                event_id = map_entry.get('eventId')
            elif isinstance(map_entry, str):
                event_id = map_entry

            # If no payment found for this Saturday appointment
            if not saturday_appt.get('Paid'):
                # If the map entry has paid=True, reset it to False since payment is no longer found
                if isinstance(map_entry, dict) and map_entry.get('paid') is True:
                    map_entry['paid'] = False
                    ghl_map[appt_id] = map_entry
                    logger.info(f"Apt {appt_id}: Reset paid to False (payment no longer found)")
                continue

            # Payment found - check if we need to call the API
            if not event_id:
                continue

            # Check if already marked as paid in ghl_map - skip API call if so
            already_paid = isinstance(map_entry, dict) and map_entry.get('paid') is True
            if already_paid:
                logger.debug(f"Apt {appt_id}: Already marked as paid in map, skipping status update")
                continue

            ok, code, msg = ghl_update_appt_status(event_id, "confirmed", "Saturday appointment prepaid")
            logger.info(f"Apt {appt_id}: Saturday prepaid update status to confirmed {'succeeded' if ok else 'failed'}")

            # If successful, mark as paid in ghl_map
            if ok:
                update_od_appt_to_confirmed(appt_id)
                if isinstance(map_entry, dict):
                    map_entry['paid'] = True
                    ghl_map[appt_id] = map_entry
                else:
                    # map_entry is a string (just event_id), convert to dict
                    ghl_map[appt_id] = {'eventId': event_id, 'paid': True}

    # Process opportunity matching for Congress Ortho Clinic (9035)
    # Use the widest sync window from the processed clinics for opportunity matching
    if CONGRESS_ORTHO_CLINIC in CLINIC_NUMS:
        opp_start, opp_end, _ = generate_window(last_sync.get(CONGRESS_ORTHO_CLINIC), force_deep_sync)
        process_congress_ortho_opportunity_matching(opp_start, opp_end, contact_map)

    # Finalize opportunities: cleanup old ones (>30 days) and save to Google Drive
    finalize_opportunities()

    # Persist state + Drive push
    save_patient_cache(patient_cache)  # local only; you're not caching this in Actions
    save_state(sent_map, last_sync, snapshot, ghl_map, contact_map)
    save_failed_updates(_FAILED_UPDATES)
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
    p = argparse.ArgumentParser(description="OpenDental → GHL sync (AptNum-first, reconciliation via Drive map, clinic routing, contact tagging)")
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
