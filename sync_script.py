#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
OpenDental → GoHighLevel (LeadConnector) sync

Key behaviors:
- First-ever run per clinic = DEEP: send ALL appts in 30-day window (ignore DateTStamp).
- Subsequent runs (deep or incremental): only send if not sent before OR DateTStamp increased; skip if previously sent and DateTStamp missing.
- America/Chicago clinic timezone; send start/end in UTC "Z" and include "timeZone".
- assignedUserId is REQUIRED per clinic (skip event if missing).
- WirelessPhone-only: validate; if unusable → skip search, omit phone in upsert, log warning.
- Guard GHL /contacts/search to avoid 422 on short/invalid queries.
- Robust CLINIC_NUMS parser (CSV or JSON) + config report.
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
from typing import Any, Dict, List, Optional, Set, Tuple
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

# Timezone (IANA zone). Your case: America/Chicago.
CLINIC_TZ = os.environ.get('CLINIC_TZ', 'America/Chicago')
CLINIC_TIMEZONE = ZoneInfo(CLINIC_TZ)

# Clinic → calendarId (strict by clinic)
# Provide as JSON, e.g. {"clinic:9034":"cal_XXXX","clinic:9035":"cal_YYYY","default":"cal_DEFAULT"}
try:
    GHL_CALENDAR_MAP: Dict[str, str] = json.loads(os.environ.get('GHL_CALENDAR_MAP', '{}'))
except Exception:
    GHL_CALENDAR_MAP = {}

# Clinic → assignedUserId (strict by clinic) — REQUIRED per clinic
# Provide as JSON, e.g. {"clinic:9034":"usr_XXXX","clinic:9035":"usr_YYYY"}
try:
    GHL_ASSIGNED_USER_MAP: Dict[str, str] = json.loads(os.environ.get('GHL_ASSIGNED_USER_MAP', '{}'))
except Exception:
    GHL_ASSIGNED_USER_MAP = {}

# Custom Field ID for "Clinic" on GHL Contacts (optional; else we tag clinic:<num>)
GHL_CUSTOM_FIELD_CLINIC_ID = os.environ.get('GHL_CUSTOM_FIELD_CLINIC_ID', '').strip()

# Google Drive (Service Account) for state
GDRIVE_SA_JSON        = os.environ.get('GDRIVE_SERVICE_ACCOUNT_JSON', '').strip()
GDRIVE_SA_JSON_B64    = os.environ.get('GDRIVE_SERVICE_ACCOUNT_JSON_B64', '').strip()
GDRIVE_SUBJECT        = os.environ.get('GDRIVE_SUBJECT', '').strip()         # optional (domain-wide delegation)
GDRIVE_FOLDER_ID      = os.environ.get('GDRIVE_FOLDER_ID', '').strip()
GDRIVE_FOLDER_NAME    = os.environ.get('GDRIVE_FOLDER_NAME', 'od_ghl_sync').strip()  # used if no folder id

# Local state files (also mirrored to Drive)
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

# Optional deep trace (writes sync_debug.log and debug/*.json)
TRACE = os.environ.get('OD_GHL_TRACE', '0') == '1'

# Sync windows
INCREMENTAL_SYNC_MINUTES = int(os.environ.get('INCREMENTAL_SYNC_MINUTES', '20160'))  # 14 days lookahead by default
DEEP_SYNC_HOURS = int(os.environ.get('DEEP_SYNC_HOURS', '720'))                      # 30 days lookahead
SAFETY_OVERLAP_HOURS = int(os.environ.get('SAFETY_OVERLAP_HOURS', '2'))

# Scheduling (used if you run in loop mode)
CLINIC_OPEN_HOUR = 8
CLINIC_CLOSE_HOUR = 20
DEEP_SYNC_HOUR = 2
INCREMENTAL_INTERVAL_MINUTES = 60
CLINIC_DELAY_SECONDS = float(os.environ.get('CLINIC_DELAY_SECONDS', '5.0'))

# Clinics & operatories (restrict which chairs to sync)
def parse_clinic_nums(val: str) -> List[int]:
    if not val:
        return []
    try:
        s = val.strip()
        if s.startswith('['):
            arr = json.loads(s)
            return [int(x) for x in arr if str(x).strip().isdigit()]
    except Exception:
        pass
    parts = re.split(r'[,\s|;]+', val.strip())
    out = []
    for p in parts:
        if p.strip().isdigit():
            out.append(int(p.strip()))
    return out

CLINIC_NUMS = parse_clinic_nums(os.environ.get('CLINIC_NUMS', '9034,9035'))

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
    'ProvNum', 'ProvHyg', 'Asst', 'ClinicNum', 'Address', 'Zip', 'Email',
    'WirelessPhone', 'Gender', 'Birthdate'
]

# Wireless phone normalization/search rules
PHONE_MIN_DIGITS = int(os.environ.get("PHONE_MIN_DIGITS", "7"))
DEFAULT_COUNTRY_CODE = os.environ.get("DEFAULT_COUNTRY_CODE", "+1")  # applied only if number lacks '+'

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

def log_config_report():
    def _yes(v): return "yes" if v else "no"
    logger.info(
        "CONFIG → clinics=%s | tz=%s | GHL location set=%s | token set=%s | calendars=%s | assignedUsers=%s",
        CLINIC_NUMS,
        CLINIC_TZ,
        _yes(bool(GHL_LOCATION_ID)),
        _yes(bool(GHL_API_TOKEN)),
        sorted(list(GHL_CALENDAR_MAP.keys())),
        sorted(list(GHL_ASSIGNED_USER_MAP.keys())),
    )
    if not CLINIC_NUMS:
        logger.error("CLINIC_NUMS resolved EMPTY → nothing to sync. Set CLINIC_NUMS like '9034,9035' or '[9034,9035]'.")
        sys.exit(1)

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
                    'User-Agent': 'OD-to-GHL/4.5',
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
        try:
            dt = datetime.datetime.strptime(s, "%Y-%m-%d")
            return dt.replace(tzinfo=timezone.utc)
        except Exception:
            return None

def to_utc_z(dt: datetime.datetime) -> str:
    return dt.astimezone(timezone.utc).isoformat().replace("+00:00", "Z")

def clinic_tz_name() -> str:
    return CLINIC_TZ

def clinic_local_date(dt: datetime.datetime) -> datetime.date:
    return dt.astimezone(CLINIC_TIMEZONE).date()

EMAIL_RE = re.compile(r"^[A-Za-z0-9._%+\-']+@[A-Za-z0-9.\-]+\.[A-Za-z]{2,}$")
def valid_email_or_unknown(email: str) -> str:
    email = (email or '').strip()
    return email if email and EMAIL_RE.match(email) else "unknown@gmail.com"

def normalize_wireless_for_search(raw: str) -> tuple[str, bool]:
    """
    Normalize WirelessPhone to a search candidate. Returns (normalized_value, ok_for_search).
    - Keep leading '+' if present, strip other non-digits.
    - If no '+', prepend DEFAULT_COUNTRY_CODE.
    - ok_for_search: at least PHONE_MIN_DIGITS digits.
    """
    raw = (raw or "").strip()
    if not raw:
        return "", False
    if raw.startswith('+'):
        digits = re.sub(r"\D", "", raw)
        return ("+" + digits, len(digits) >= PHONE_MIN_DIGITS)
    digits = re.sub(r"\D", "", raw)
    if not digits:
        return "", False
    e164 = (DEFAULT_COUNTRY_CODE + digits) if DEFAULT_COUNTRY_CODE else digits
    return (e164, len(digits) >= PHONE_MIN_DIGITS)

def ok_for_ghl_search(norm: str) -> bool:
    if not norm:
        return False
    digits = re.sub(r"\D", "", norm)
    return len(digits) >= PHONE_MIN_DIGITS

# =========================
# === GOOGLE DRIVE STATE ==
# =========================
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
    for f in (STATE_FILE, SENT_APPTS_FILE, APPT_SNAPSHOT_FILE, GHL_MAP_FILE, GHL_CONTACTS_MAP_FILE):
        if not os.path.exists(f):
            DRIVE.pull(f)

def save_state(sent_map: Dict[str, Dict[str, str]], last_sync: Dict[int, Optional[str]],
               snapshot: Dict[str, Any], ghl_map: Dict[str, Any], contact_map: Dict[int, Any]):
    atomic_save_json(SENT_APPTS_FILE, sent_map); DRIVE.push(SENT_APPTS_FILE)
    atomic_save_json(STATE_FILE, last_sync); DRIVE.push(STATE_FILE)
    atomic_save_json(APPT_SNAPSHOT_FILE, snapshot); DRIVE.push(APPT_SNAPSHOT_FILE)
    atomic_save_json(GHL_MAP_FILE, ghl_map); DRIVE.push(GHL_MAP_FILE)
    atomic_save_json(GHL_CONTACTS_MAP_FILE, {str(k): v for k, v in contact_map.items()}); DRIVE.push(GHL_CONTACTS_MAP_FILE)

# =========================
# ===== OD FETCH ==========
# =========================
def od_get(endpoint: str, params: Dict[str, Any]) -> Optional[List[Any]]:
    url = f"{API_BASE_URL}/{endpoint}"
    try:
        r = get_session().get(url, headers=od_headers(), params=params, timeout=REQUEST_TIMEOUT)
        _debug_write(f"od_{endpoint}_req.json", {"url": url, "params": params})
        if not r.ok:
            logger.error("OD GET failed %s %s: %s", endpoint, r.status_code, r.text)
            return None
        data = r.json()
        _debug_write(f"od_{endpoint}_resp.json", data)
        return data if isinstance(data, list) else [data]
    except requests.RequestException as e:
        body = e.response.text if getattr(e, 'response', None) else ''
        logger.error(f"OD GET exception {endpoint}: {e} {body}")
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
    ops = od_get_paginated('operatories', {'ClinicNum': clinic}) or []
    logger.debug(f"Clinic {clinic}: operatories found: {len(ops)}")
    if ops:
        try:
            sample = [str(o.get('OperatoryNum') or o.get('Op')) for o in ops][:10]
            logger.debug(f"Clinic {clinic}: operatory ids (sample): {', '.join(sample)}")
        except Exception:
            pass
    return ops

def fetch_appointments_for_window(clinic: int, start: datetime.datetime, end: datetime.datetime) -> List[Dict[str, Any]]:
    params_base = {
        'ClinicNum': str(clinic),
        'dateStart': start.astimezone(timezone.utc).date().strftime('%Y-%m-%d'),
        'dateEnd': end.astimezone(timezone.utc).date().strftime('%Y-%m-%d'),
        'fields': ','.join(REQUIRED_APPOINTMENT_FIELDS),
    }
    all_appts: List[Dict[str, Any]] = []
    valid_ops = set(CLINIC_OPERATORY_FILTERS.get(clinic, []))

    _ = get_operatories(clinic)

    logger.debug(
        f"Clinic {clinic}: fetching appointments {params_base['dateStart']}..{params_base['dateEnd']} "
        f"ops={sorted(valid_ops)} statuses={sorted(VALID_STATUSES)}"
    )

    for status in VALID_STATUSES:
        for op in CLINIC_OPERATORY_FILTERS.get(clinic, []):
            p = dict(params_base); p['AptStatus'] = status; p['Op'] = str(op)
            chunk = od_get_paginated('appointments', p) or []
            logger.debug(f"Clinic {clinic}: AptStatus={status} Op={op} -> {len(chunk)} returned")
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
    result = list(uniq.values())

    logger.info(f"Clinic {clinic}: appointments raw={len(all_appts)} after-de-dupe={len(result)}")
    _debug_write(f"od_appts_clinic_{clinic}.json", {"start": start.isoformat(), "end": end.isoformat(), "appointments": result})
    return result

# =========================
# ===== GHL CLIENT ========
# =========================
def ghl_search_contact_by_phone(phone: str) -> Optional[Dict[str, Any]]:
    """
    Search contacts by text query (phone). POST /contacts/search
    Caller must guard with ok_for_ghl_search() to avoid 422s.
    """
    if not ok_for_ghl_search(phone):
        return None
    url = f"{GHL_API_BASE}/contacts/search"
    body = {"query": phone, "limit": 10, "page": 1, "locationId": GHL_LOCATION_ID}
    try:
        r = get_session().post(url, headers=ghl_headers(), json=body, timeout=REQUEST_TIMEOUT)
        _debug_write("ghl_contacts_search_req.json", {"url": url, "body": body})
        if not r.ok:
            logger.error("GHL contact search failed %s: %s", r.status_code, r.text)
            return None
        data = r.json()
        _debug_write("ghl_contacts_search_resp.json", data)
        contacts = data.get('contacts') if isinstance(data, dict) else (data if isinstance(data, list) else [])
        return contacts[0] if contacts else None
    except requests.RequestException as e:
        logger.error(f"GHL contact search exception: {e}")
        return None

def ghl_build_contact_payload(patient: Dict[str, Any], clinic_num: int) -> Dict[str, Any]:
    # Required/known fields
    first = (patient.get("FName") or "").strip()
    last  = (patient.get("LName") or "").strip()
    email = valid_email_or_unknown(patient.get("Email"))
    # Wireless-only; upstream passes "" if invalid
    phone = (patient.get("WirelessPhone") or "").strip()
    dob   = (patient.get("Birthdate") or "").strip() or None
    address1 = (patient.get("Address") or "").strip()
    postal = (patient.get("Zip") or patient.get("PostalCode") or "").strip()
    gender = (patient.get("Gender") or "").strip()

    payload: Dict[str, Any] = {
        "locationId": GHL_LOCATION_ID,
        "firstName": first,
        "lastName": last,
        "email": email,
        **({"phone": phone} if phone else {}),  # only include if non-empty
        "dateOfBirth": dob or None,
        "address1": address1 or None,
        "postalCode": postal or None,
        "gender": gender or None,
    }

    # Clinic custom field or fallback tag
    if GHL_CUSTOM_FIELD_CLINIC_ID:
        payload["customFields"] = [{"id": GHL_CUSTOM_FIELD_CLINIC_ID, "value": str(clinic_num)}]
    else:
        payload["tags"] = [f"clinic:{clinic_num}"]

    return {k: v for k, v in payload.items() if v not in (None, [], "")}

def ghl_upsert_contact(patient: Dict[str, Any], clinic_num: int) -> Optional[str]:
    """
    POST /contacts/upsert
    """
    url = f"{GHL_API_BASE}/contacts/upsert"
    body = ghl_build_contact_payload(patient, clinic_num)
    try:
        r = get_session().post(url, headers=ghl_headers(), json=body, timeout=REQUEST_TIMEOUT)
        _debug_write("ghl_contact_upsert_req.json", {"url": url, "body": body})
        if not r.ok:
            logger.error("GHL upsert contact failed %s: %s", r.status_code, r.text)
            return None
        j = r.json()
        _debug_write("ghl_contact_upsert_resp.json", j)
        return (j.get("contact") or {}).get("id") or j.get("id")
    except requests.RequestException as e:
        logger.error(f"GHL upsert contact exception: {e}")
        return None

def ghl_tag_contact(contact_id: str, tag: str = "fromopendental") -> bool:
    if not contact_id:
        return False
    url = f"{GHL_API_BASE}/contacts/{contact_id}/tags"
    body = {"tags": [tag]}
    try:
        r = get_session().post(url, headers=ghl_headers(), json=body, timeout=REQUEST_TIMEOUT)
        _debug_write("ghl_contact_tag_req.json", {"url": url, "body": body, "contactId": contact_id})
        if r.status_code in (200, 201, 204):
            return True
        if r.status_code in (404, 405, 400):
            # Fallback: PATCH-style tags
            url2 = f"{GHL_API_BASE}/contacts/{contact_id}"
            body2 = {"tags": [tag]}
            r2 = get_session().patch(url2, headers=ghl_headers(), json=body2, timeout=REQUEST_TIMEOUT)
            _debug_write("ghl_contact_tag_fallback_req.json", {"url": url2, "body": body2, "contactId": contact_id})
            if r2.status_code in (200, 204):
                return True
            logger.warning("GHL tag contact fallback failed %s: %s", r2.status_code, r2.text)
            return False
        logger.warning("GHL tag contact failed %s: %s", r.status_code, r.text)
        return False
    except requests.RequestException as e:
        logger.warning(f"GHL tag contact exception for {contact_id}: {e}")
        return False

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
        _debug_write("ghl_contact_appts_req.json", {"url": url, "contactId": contact_id})
        if not r.ok:
            logger.error("GHL get contact appointments failed %s: %s", r.status_code, r.text)
            return []
        data = r.json()
        _debug_write("ghl_contact_appts_resp.json", data)
        if isinstance(data, list): return data
        return data.get('appointments', []) if isinstance(data, dict) else []
    except requests.RequestException as e:
        logger.error(f"GHL get contact appointments exception: {e}")
        return []

def pick_latest_same_day_event(contact_events: List[Dict[str, Any]],
                               od_start_local: datetime.datetime,
                               calendar_id: str) -> Optional[str]:
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

def ghl_create_appointment(calendar_id: str, contact_id: str, assigned_user_id: str,
                           title: str, start_dt_local: datetime.datetime, end_dt_local: datetime.datetime,
                           status: str) -> Optional[str]:
    url = f"{GHL_API_BASE}/calendars/events/appointments"
    body = {
        "calendarId": calendar_id,
        "contactId": contact_id,
        "assignedUserId": assigned_user_id,  # REQUIRED
        "title": title or "Dental Appointment",
        "appointmentStatus": status,          # confirmed / cancelled / showed
        "startTime": to_utc_z(start_dt_local),
        "endTime": to_utc_z(end_dt_local),
        "timeZone": clinic_tz_name(),
        "ignoreFreeSlotValidation": True,
        "locationId": GHL_LOCATION_ID
    }
    try:
        r = get_session().post(url, headers=ghl_headers(), json=body, timeout=REQUEST_TIMEOUT)
        _debug_write("ghl_create_event_req.json", {"url": url, "body": body})
        if not r.ok:
            logger.error("GHL create appointment failed %s: %s", r.status_code, r.text)
            return None
        j = r.json()
        _debug_write("ghl_create_event_resp.json", j)
        return j.get('id') or j.get('eventId') or j.get('appointmentId')
    except requests.RequestException as e:
        logger.error(f"GHL create appointment exception: {e}")
        return None

def ghl_update_appointment(event_id: str, calendar_id: str, assigned_user_id: str,
                           title: str, start_dt_local: datetime.datetime, end_dt_local: datetime.datetime,
                           status: str) -> bool:
    if not event_id:
        return False
    url = f"{GHL_API_BASE}/calendars/events/appointments/{event_id}"
    body = {
        "calendarId": calendar_id,
        "assignedUserId": assigned_user_id,  # REQUIRED
        "title": title or "Dental Appointment",
        "appointmentStatus": status,
        "startTime": to_utc_z(start_dt_local),
        "endTime": to_utc_z(end_dt_local),
        "timeZone": clinic_tz_name(),
        "ignoreFreeSlotValidation": True,
        "locationId": GHL_LOCATION_ID
    }
    try:
        r = get_session().put(url, headers=ghl_headers(), json=body, timeout=REQUEST_TIMEOUT)
        _debug_write("ghl_update_event_req.json", {"url": url, "body": body, "eventId": event_id})
        if not r.ok:
            logger.error("GHL update appointment failed %s: %s", r.status_code, r.text)
            return False
        return True
    except requests.RequestException as e:
        logger.error(f"GHL update appointment exception: {e}")
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
# === FILTERING RULES =====
# =========================
def filter_appointments_for_processing(
    appts: List[Dict[str, Any]],
    *,
    is_deep_run: bool,
    is_first_run_for_clinic: bool,
    sent_map: Dict[str, Dict[str, str]]
) -> Tuple[List[Dict[str, Any]], Set[int]]:
    """
    Returns (to_process, pat_nums) based on the rules:
      - First-ever deep run: include ALL appts in window (ignore DateTStamp).
      - Later runs (deep or incremental):
          * If not previously sent → include.
          * If previously sent:
                - include only if DateTStamp > last_sent_tstamp
                - if DateTStamp missing → skip (can't prove change)
    """
    to_process: List[Dict[str, Any]] = []
    pat_nums: Set[int] = set()

    if is_deep_run and is_first_run_for_clinic:
        for a in appts:
            to_process.append(a)
            if a.get('PatNum'):
                try: pat_nums.add(int(a['PatNum']))
                except: pass
        return to_process, pat_nums

    for a in appts:
        aptnum = str(a.get('AptNum', ''))
        if not aptnum:
            continue

        prev_iso = (sent_map.get(aptnum, {}) or {}).get('last_sent_tstamp')
        prev_dt = parse_time(prev_iso) if prev_iso else None

        curr_dt = parse_time(a.get('DateTStamp', ''))  # may be None

        if prev_dt is None:
            # not sent before → include (even if DateTStamp missing)
            to_process.append(a)
        else:
            # was sent before → include ONLY if DateTStamp increased
            if curr_dt and curr_dt > prev_dt:
                to_process.append(a)
            else:
                continue

        if a.get('PatNum'):
            try: pat_nums.add(int(a['PatNum']))
            except: pass

    return to_process, pat_nums

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
    if errs:
        logger.error("Config validation failed:")
        for e in errs: logger.error(" - " + e)
        return False
    return True

def ghl_contact_payload_from_patient_like(first: str, last: str, email: str, phone_norm: str) -> Dict[str, Any]:
    return {
        "FName": first,
        "LName": last,
        "Email": email,
        "WirelessPhone": phone_norm  # empty string means "omit" later
    }

def ensure_contact_id(first: str, last: str, email: str, phone_raw: str,
                      pat_num: int,
                      clinic_num: int,
                      contact_map: Dict[int, Any]) -> Tuple[Optional[str], bool]:
    """
    Returns (contact_id, is_new_contact).
    - Map fast path by PatNum if present.
    - Use ONLY WirelessPhone; if invalid/short: skip search, omit in upsert, log warning.
    - Upsert (create/update) and record mapping.
    """
    mapped = contact_map.get(pat_num)
    if mapped and mapped.get('contactId'):
        return mapped['contactId'], False

    phone_norm, phone_ok = normalize_wireless_for_search(phone_raw)

    found = None
    if phone_ok and ok_for_ghl_search(phone_norm):
        found = ghl_search_contact_by_phone(phone_norm)
    else:
        logger.warning(f"Pat {pat_num}: invalid or missing WirelessPhone '{phone_raw}' → skipping phone search and omitting phone in upsert")

    if found and found.get('id'):
        cid = found['id']
        contact_map[pat_num] = {
            "contactId": cid,
            "phone": phone_norm if phone_ok else "",
            "email": valid_email_or_unknown(email),
            "updatedAt": datetime.datetime.utcnow().replace(tzinfo=timezone.utc).isoformat()
        }
        return cid, False

    # Build minimal payload for upsert (omit phone if invalid)
    patient_like = ghl_contact_payload_from_patient_like(first, last, email, phone_norm if phone_ok else "")
    cid = ghl_upsert_contact(patient_like, clinic_num)
    if cid:
        contact_map[pat_num] = {
            "contactId": cid,
            "phone": phone_norm if phone_ok else "",
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

    # patient fields (WirelessPhone only)
    p = patients.get(pat_num, {}) or {}
    first = (p.get('FName') or appt.get('FName') or '').strip()
    last  = (p.get('LName') or appt.get('LName') or '').strip()
    email = valid_email_or_unknown(p.get('Email', ''))
    phone_raw = (p.get('WirelessPhone') or '').strip()

    # clinic routing
    calendar_id = pick_calendar_id(clinic)
    assigned_user_id = pick_assigned_user_id(clinic)
    if not calendar_id:
        logger.error(f"Clinic {clinic}: missing calendar mapping")
        return None
    if not assigned_user_id:
        logger.error(f"Clinic {clinic}: assignedUserId is REQUIRED; skipping AptNum {apt_num}")
        return None

    # Ensure / create contact
    contact_id, is_new_contact = ensure_contact_id(first, last, email, phone_raw, pat_num, clinic, contact_map)
    if not contact_id:
        logger.error(f"Apt {apt_num}: failed to ensure contact")
        return None

    title = f"{first} {last}".strip() or "Dental Appointment"
    status = map_appt_status_to_ghl(appt.get('AptStatus', ''))

    # If AptNum already mapped → update exact event
    mapped = ghl_map.get(apt_num)
    if mapped and mapped.get('eventId'):
        event_id = mapped['eventId']
        ok = ghl_update_appointment(event_id, calendar_id, assigned_user_id, title, start_local, end_local, status)
        if ok:
            mapped.update({"contactId": contact_id, "calendarId": calendar_id, "clinic": clinic})
            ghl_map[apt_num] = mapped
            logger.info(f"✓ Updated mapped event {event_id} for AptNum {apt_num}")
            ghl_tag_contact(contact_id, "fromopendental")
            return event_id
        else:
            logger.warning(f"Apt {apt_num}: mapped update failed — creating fresh and remapping")
            new_id = ghl_create_appointment(calendar_id, contact_id, assigned_user_id, title, start_local, end_local, status)
            if new_id:
                ghl_map[apt_num] = {"contactId": contact_id, "eventId": new_id, "calendarId": calendar_id, "clinic": clinic}
                ghl_tag_contact(contact_id, "fromopendental")
                return new_id
            return None

    # Not mapped
    if is_new_contact:
        new_event_id = ghl_create_appointment(calendar_id, contact_id, assigned_user_id, title, start_local, end_local, status)
        if new_event_id:
            ghl_map[apt_num] = {"contactId": contact_id, "eventId": new_event_id, "calendarId": calendar_id, "clinic": clinic}
            logger.info(f"＋ Created event {new_event_id} for NEW contact, AptNum {apt_num}")
            ghl_tag_contact(contact_id, "fromopendental")
            return new_event_id
        logger.error(f"Apt {apt_num}: failed to create event for new contact")
        return None

    # Existing contact (but AptNum not mapped) → try reconcile latest same-day event
    contact_events = ghl_get_contact_appointments(contact_id)
    candidate_event_id = pick_latest_same_day_event(contact_events, start_local, calendar_id)
    if candidate_event_id:
        ok = ghl_update_appointment(candidate_event_id, calendar_id, assigned_user_id, title, start_local, end_local, status)
        if ok:
            ghl_map[apt_num] = {"contactId": contact_id, "eventId": candidate_event_id, "calendarId": calendar_id, "clinic": clinic}
            logger.info(f"✓ Reconciled via latest same-day event {candidate_event_id} for AptNum {apt_num}")
            ghl_tag_contact(contact_id, "fromopendental")
            return candidate_event_id
        else:
            logger.warning(f"Apt {apt_num}: same-day update failed — creating fresh")

    # No same-day match → create new
    new_event_id = ghl_create_appointment(calendar_id, contact_id, assigned_user_id, title, start_local, end_local, status)
    if new_event_id:
        ghl_map[aptnum] = {"contactId": contact_id, "eventId": new_event_id, "calendarId": calendar_id, "clinic": clinic}
        logger.info(f"＋ Created event {new_event_id} for AptNum {apt_num}")
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

    log_config_report()

    # Connect Drive and seed state locally if needed
    DRIVE.connect()
    pull_all_from_drive()

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

        # Decide mode flags
        is_deep_run = not is_incr
        is_first_run_for_clinic = (last_sync.get(clinic) is None)
        if is_deep_run and is_first_run_for_clinic:
            logger.info("Clinic %s: FIRST-EVER deep sync → sending all appts in the window (ignoring DateTStamp).", clinic)

        # Apply filtering rules
        to_process, pat_nums = filter_appointments_for_processing(
            appts,
            is_deep_run=is_deep_run,
            is_first_run_for_clinic=is_first_run_for_clinic,
            sent_map=sent_map,
        )

        logger.info(
            "Clinic %s: selected %d of %d appointments for processing (%s%s)",
            clinic, len(to_process), len(appts),
            "deep" if is_deep_run else "incremental",
            " / first-run" if is_first_run_for_clinic else ""
        )

        if not to_process:
            logger.info(f"Clinic {clinic}: nothing to send after filtering.")
            # Still advance last_sync so next incremental window narrows
            last_sync[clinic] = datetime.datetime.utcnow().replace(tzinfo=timezone.utc).isoformat()
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

        # advance last_sync after processing
        last_sync[clinic] = datetime.datetime.utcnow().replace(tzinfo=timezone.utc).isoformat()

    # Persist state + Drive push
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
    p = argparse.ArgumentParser(description="OpenDental → GHL sync (AptNum-first, same-day reconciliation, clinic routing, contact tagging)")
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
