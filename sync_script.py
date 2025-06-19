#!/usr/bin/env python3
"""
OpenDental → Keragon Appointment Sync
- Primary: FHIR `/appointment` (lowercase) for start/end
- Fallback: ChartModules `/ProgNotes` for duration
- Logs detailed info per clinic and operatory
"""

import os
import sys
import json
import logging
import datetime
import requests
import time
from typing import List, Dict, Any, Optional, Tuple
from requests.exceptions import HTTPError, RequestException
from dataclasses import dataclass, field
from concurrent.futures import ThreadPoolExecutor, as_completed
from zoneinfo import ZoneInfo

@dataclass
class Config:
    api_base_url: str = os.environ.get('OPEN_DENTAL_API_URL','https://api.opendental.com/api/v1')
    fhir_base_url: str = os.environ.get('OPEN_DENTAL_FHIR_URL','https://api.opendental.com/fhir/v2')
    developer_key: str = os.environ.get('OPEN_DENTAL_DEVELOPER_KEY','')
    customer_key: str = os.environ.get('OPEN_DENTAL_CUSTOMER_KEY','')
    keragon_webhook_url: str = os.environ.get('KERAGON_WEBHOOK_URL','')
    state_file: str = os.environ.get('STATE_FILE','last_sync_state.json')
    log_level: str = os.environ.get('LOG_LEVEL','INFO')
    lookahead_hours: int = int(os.environ.get('LOOKAHEAD_HOURS','720'))
    max_workers: int = int(os.environ.get('MAX_WORKERS','5'))
    request_timeout: int = int(os.environ.get('REQUEST_TIMEOUT','30'))
    retry_attempts: int = int(os.environ.get('RETRY_ATTEMPTS','3'))
    clinic_nums: List[int] = field(default_factory=lambda: [int(x) for x in os.environ.get('CLINIC_NUMS','').split(',') if x.strip().isdigit()])
    operatory_filters: Dict[int,List[int]] = field(default_factory=lambda:{9034:[11579,11580,11588],9035:[11574,11576,11577]})

config = Config()
logger = logging.getLogger('opendental_sync')
handler = logging.StreamHandler(sys.stdout)
handler.setFormatter(logging.Formatter('%(asctime)s - %(levelname)s - %(message)s'))
logger.addHandler(handler)
logger.setLevel(getattr(logging,config.log_level.upper(),logging.INFO))
logging.getLogger('urllib3').setLevel(logging.WARNING)

def retry(fn):
    def wrapper(*args,**kwargs):
        last_exc=None
        for i in range(config.retry_attempts):
            try: return fn(*args,**kwargs)
            except (RequestException,HTTPError) as e:
                last_exc=e; time.sleep(2**i)
        logger.error(f"All retries failed: {last_exc}")
        raise last_exc
    return wrapper

# State
from typing import Tuple

def load_state() -> Tuple[Dict[int,datetime.datetime],bool]:
    if not os.path.exists(config.state_file): return {},True
    try:
        with open(config.state_file) as f: data=json.load(f)
        return {int(k):datetime.datetime.fromisoformat(v) for k,v in data.items()},False
    except: return {},True

def save_state(state:Dict[int,datetime.datetime]):
    tmp=config.state_file+'.tmp'
    with open(tmp,'w') as f: json.dump({str(k):v.isoformat() for k,v in state.items()},f,indent=2)
    os.replace(tmp,config.state_file)

# Utils

def make_headers():
    return {'Authorization':f"ODFHIR {config.developer_key}/{config.customer_key}"}

def parse_iso(s:Optional[str]) -> Optional[datetime.datetime]:
    if not s: return None
    for fmt in ("%Y-%m-%dT%H:%M:%S.%f","%Y-%m-%dT%H:%M:%S","%Y-%m-%d %H:%M:%S","%Y-%m-%d"): 
        try: return datetime.datetime.strptime(s,fmt)
        except: pass
    return None

# FHIR fetch using lowercase path
@retry
def fetch_fhir_appointment(id:str) -> Dict[str,Any]:
    url=f"{config.fhir_base_url}/appointment/{id}"
    hdr=make_headers().copy(); hdr['Accept']='application/fhir+json'
    r=requests.get(url,headers=hdr,timeout=config.request_timeout)
    r.raise_for_status(); return r.json()

# ProgNotes
@retry
def fetch_prognotes(pat:int):
    url=f"{config.api_base_url}/chartmodules/{pat}/ProgNotes"
    r=requests.get(url,headers={**make_headers(),'Content-Type':'application/json'},timeout=config.request_timeout)
    r.raise_for_status(); return r.json()

# Times
@retry
def get_times(appt:Dict[str,Any]) -> Tuple[Optional[datetime.datetime],Optional[datetime.datetime],int]:
    # FHIR
    try:
        f=fetch_fhir_appointment(str(appt['AptNum']))
        st=parse_iso(f.get('start'))
        ed=parse_iso(f.get('end'))
        if st and ed: return st,ed,int((ed-st).total_seconds()/60)
    except: pass
    # ProgNotes fallback
    st=parse_iso(appt.get('AptDateTime'))
    mins=60
    if st:
        try:
            for n in fetch_prognotes(appt['PatNum']):
                if n.get('AptNum')==appt['AptNum']:
                    h,m=n.get('Length','0:60').split(':'); mins=int(h)*60+int(m); break
        except: pass
    ed=st+datetime.timedelta(minutes=mins) if st else None
    return st,ed,mins

# Fetch appts
@retry
def fetch_appointments(clinic, since, first):
    now=datetime.datetime.utcnow()
    s=now.strftime("%Y-%m-%d"); e=(now+datetime.timedelta(hours=config.lookahead_hours)).strftime("%Y-%m-%d")
    logger.info(f"Clinic {clinic}: fetching from {s} to {e}")
    out=[]; sts=['Scheduled','Complete','Broken']; ops=config.operatory_filters.get(clinic,[None])
    base={'ClinicNum':clinic,'dateStart':s,'dateEnd':e}
    for st in sts:
        for op in ops:
            prm=base.copy(); prm['AptStatus']=st
            if op: prm['Op']=op
            if not first and since: prm['DateTStamp']=since.isoformat()
            r=requests.get(f"{config.api_base_url}/appointments",headers={**make_headers(),'Content-Type':'application/json'},params=prm,timeout=config.request_timeout)
            r.raise_for_status()
            data=r.json()
            for a in (data if isinstance(data,list) else [data]): a.update({'_clinic':clinic,'_oper':op}); out.append(a)
    return out

# Fetch patient
@retry
def fetch_patient(pat):
    if not pat: return {}
    try: r=requests.get(f"{config.api_base_url}/patients/{pat}",headers={**make_headers(),'Content-Type':'application/json'},timeout=config.request_timeout); r.raise_for_status(); return r.json()
    except:
        r=requests.get(f"{config.api_base_url}/patients",headers={**make_headers(),'Content-Type':'application/json'},params={'PatNum':pat},timeout=config.request_timeout)
        r.raise_for_status(); d=r.json(); return d[0] if isinstance(d,list) else {}

# Send
@retry
def send_to_keragon(appt,patient):
    info={}
    st,ed,mins=get_times(appt)
    payload={
        'appointmentId':str(appt['AptNum']),'appointmentTime':st.isoformat() if st else '',
        'appointmentEndTime':ed.isoformat() if ed else '','appointmentDurationMinutes':mins,
        'status':appt.get('AptStatus'),'notes':appt.get('Note',''),
        'patientId':str(appt['PatNum']),
        'firstName':patient.get('FName',''),'lastName':patient.get('LName',''),
        'gender':patient.get('Gender',''),'address':patient.get('Address',''),
        'city':patient.get('City',''),'state':patient.get('State',''),'zipCode':patient.get('Zip',''),
        'balance':patient.get('Balance',0)
    }
    requests.post(config.keragon_webhook_url,json={k:v for k,v in payload.items() if v},timeout=config.request_timeout).raise_for_status()
    return {'clinic':appt['_clinic'],'oper':appt['_oper'],'apt':appt['AptNum'],'first':patient.get('FName',''),'last':patient.get('LName',''),'start':st.isoformat() if st else '','end':ed.isoformat() if ed else ''}

# Main

def run(force):
    if not all([config.developer_key,config.customer_key,config.keragon_webhook_url]): logger.critical("Missing config");sys.exit(1)
    state,first=load_state(); overall=force or first; new={}; proc=[]
    for c in config.clinic_nums:
        since=None if overall else state.get(c)
        appts=fetch_appointments(c,since,overall)
        logger.info(f"Processing {len(appts)} for clinic {c}")
        pats={}
        with ThreadPoolExecutor(max_workers=config.max_workers) as exe:
            futs={exe.submit(fetch_patient,a['PatNum']):a for a in appts}
            for fut in as_completed(futs): ap=futs[fut]; pats[ap['PatNum']]=fut.result() or {}
        for a in appts:
            try: info=send_to_keragon(a,pats.get(a['PatNum'],{}));proc.append(info)
            except Exception as e: logger.error(f"Error apt {a['AptNum']}: {e}")
        logger.info(f"Finished clinic {c}"); new[c]=datetime.datetime.utcnow()
    save_state(new)
    for r in proc: logger.info(f"Synced apt {r['apt']} (clinic {r['clinic']}, oper {r['oper']}) for {r['first']} {r['last']} from {r['start']} to {r['end']}")

if __name__=='__main__':
    import argparse
    p=argparse.ArgumentParser(); p.add_argument('--once',action='store_true');p.add_argument('--reset',action='store_true');p.add_argument('--verbose',action='store_true')
    args=p.parse_args();
    if args.verbose: logger.setLevel(logging.DEBUG)
    if args.reset: os.remove(config.state_file) if os.path.exists(config.state_file) else None; sys.exit(0)
    run(args.once)
