#!/usr/bin/env python3
"""
OpenDental API Field Diagnostic Script
This script will help you identify the exact field names and values 
for appointment types and operatories in your OpenDental API responses.
"""

import os
import sys
import json
import logging
import datetime
import requests
from datetime import timezone, timedelta
from typing import List, Dict, Any, Optional

# === CONFIGURATION ===
API_BASE_URL        = os.environ.get('OPEN_DENTAL_API_URL', 'https://api.opendental.com/api/v1')
DEVELOPER_KEY       = os.environ.get('OPEN_DENTAL_DEVELOPER_KEY')
CUSTOMER_KEY        = os.environ.get('OPEN_DENTAL_CUSTOMER_KEY')
CLINIC_NUMS         = [int(x) for x in os.environ.get('CLINIC_NUMS', '').split(',') if x.strip().isdigit()]

# Set up logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[logging.StreamHandler(sys.stdout)]
)
logger = logging.getLogger('opendental_diagnostic')

def make_auth_header() -> Dict[str, str]:
    return {'Authorization': f'ODFHIR {DEVELOPER_KEY}/{CUSTOMER_KEY}', 'Content-Type': 'application/json'}

def analyze_appointment_fields(clinic: int, limit: int = 10) -> None:
    """Fetch and analyze appointment data to identify field names and values"""
    
    print(f"\n{'='*80}")
    print(f"ANALYZING CLINIC {clinic}")
    print(f"{'='*80}")
    
    # Fetch recent appointments
    now_utc = datetime.datetime.utcnow().replace(tzinfo=timezone.utc)
    params = {
        'ClinicNum': clinic,
        'dateStart': (now_utc - timedelta(days=7)).strftime('%Y-%m-%d'),  # Last 7 days
        'dateEnd': (now_utc + timedelta(days=7)).strftime('%Y-%m-%d')     # Next 7 days
    }
    
    print(f"Fetching appointments for clinic {clinic} from {params['dateStart']} to {params['dateEnd']}")
    
    try:
        r = requests.get(f"{API_BASE_URL}/appointments",
                         headers=make_auth_header(), params=params, timeout=30)
        r.raise_for_status()
        data = r.json()
        appointments = data if isinstance(data, list) else [data]
        
        print(f"Found {len(appointments)} appointments")
        
        if not appointments:
            print("No appointments found for this clinic in the date range.")
            return
            
        # Limit the number we analyze to avoid too much output
        sample_appointments = appointments[:limit]
        
        print(f"\nAnalyzing first {len(sample_appointments)} appointments...")
        print(f"\n{'='*80}")
        print("APPOINTMENT FIELD ANALYSIS")
        print(f"{'='*80}")
        
        # Track all unique field names and values
        all_fields = set()
        operatory_fields = {}
        appointment_type_fields = {}
        
        for i, appt in enumerate(sample_appointments, 1):
            apt_num = appt.get('AptNum', 'Unknown')
            print(f"\n--- APPOINTMENT {i}: ID {apt_num} ---")
            
            # Add all field names to our tracking set
            all_fields.update(appt.keys())
            
            # Look for operatory-related fields
            operatory_candidates = ['Op', 'OperatoryNum', 'Operatory', 'OpNum', 'OperatoryId']
            for field in operatory_candidates:
                if field in appt and appt[field] is not None:
                    operatory_fields[field] = operatory_fields.get(field, set())
                    operatory_fields[field].add(str(appt[field]))
                    print(f"  Operatory field '{field}': {appt[field]}")
            
            # Look for appointment type fields
            type_candidates = ['AptType', 'AppointmentType', 'Type', 'ApptType', 'ProcCode', 'Procedure']
            for field in type_candidates:
                if field in appt and appt[field] is not None:
                    appointment_type_fields[field] = appointment_type_fields.get(field, set())
                    appointment_type_fields[field].add(str(appt[field]))
                    print(f"  Appointment type field '{field}': {appt[field]}")
            
            # Show other potentially relevant fields
            other_interesting = ['AptStatus', 'Pattern', 'AptDateTime', 'Note', 'PatNum']
            for field in other_interesting:
                if field in appt:
                    value = appt[field]
                    if isinstance(value, str) and len(value) > 50:
                        value = value[:50] + "..."
                    print(f"  {field}: {value}")
            
            # Show a sample of all fields for the first appointment
            if i == 1:
                print(f"\n  ALL FIELDS IN FIRST APPOINTMENT:")
                for field, value in sorted(appt.items()):
                    if isinstance(value, str) and len(value) > 50:
                        value = value[:50] + "..."
                    print(f"    {field}: {value}")
        
        # Summary of findings
        print(f"\n{'='*80}")
        print("SUMMARY OF FINDINGS")
        print(f"{'='*80}")
        
        print(f"\nAll unique field names found ({len(all_fields)}):")
        for field in sorted(all_fields):
            print(f"  - {field}")
        
        if operatory_fields:
            print(f"\nOPERATORY FIELDS FOUND:")
            for field, values in operatory_fields.items():
                print(f"  Field '{field}' has values: {sorted(values)}")
        else:
            print(f"\nNO OPERATORY FIELDS FOUND!")
            print("Check these field names in your API documentation:")
            print("  - Op, OperatoryNum, Operatory, OpNum, OperatoryId")
        
        if appointment_type_fields:
            print(f"\nAPPOINTMENT TYPE FIELDS FOUND:")
            for field, values in appointment_type_fields.items():
                print(f"  Field '{field}' has values: {sorted(values)}")
        else:
            print(f"\nNO APPOINTMENT TYPE FIELDS FOUND!")
            print("Check these field names in your API documentation:")
            print("  - AptType, AppointmentType, Type, ApptType, ProcCode")
        
        # Generate suggested configuration
        print(f"\n{'='*80}")
        print("SUGGESTED CONFIGURATION FOR YOUR SCRIPT")
        print(f"{'='*80}")
        
        if operatory_fields:
            primary_op_field = list(operatory_fields.keys())[0]
            print(f"\n# Use this field name for operatory filtering:")
            print(f"# op_num = a.get('{primary_op_field}') or a.get('OperatoryNum')")
        
        if appointment_type_fields:
            primary_type_field = list(appointment_type_fields.keys())[0]
            print(f"\n# Use this field name for appointment type filtering:")
            print(f"# apt_type = a.get('{primary_type_field}') or a.get('AppointmentType', '')")
        
        if operatory_fields and appointment_type_fields:
            print(f"\n# Your OPERATORY_APPOINTMENT_TYPE_FILTERS should use these values:")
            print(f"OPERATORY_APPOINTMENT_TYPE_FILTERS: Dict[int, List[str]] = {{")
            
            # Show examples based on what we found
            op_values = list(list(operatory_fields.values())[0])
            type_values = list(list(appointment_type_fields.values())[0])
            
            if len(op_values) > 0 and len(type_values) > 0:
                print(f"    {op_values[0]}: {type_values[:2]},  # Example operatory with example types")
            print(f"    # Add your specific operatory numbers and appointment types here")
            print(f"}}")
    
    except Exception as e:
        logger.error(f"Error analyzing clinic {clinic}: {e}")
        if hasattr(e, 'response') and e.response is not None:
            try:
                error_detail = e.response.text
                logger.error(f"  Response: {error_detail}")
            except:
                pass

def main():
    print("OpenDental API Field Diagnostic Tool")
    print("="*50)
    
    # Check configuration
    if not (DEVELOPER_KEY and CUSTOMER_KEY and CLINIC_NUMS):
        print("❌ Missing configuration!")
        print("Required environment variables:")
        print("  - OPEN_DENTAL_DEVELOPER_KEY")
        print("  - OPEN_DENTAL_CUSTOMER_KEY") 
        print("  - CLINIC_NUMS")
        sys.exit(1)
    
    print(f"✓ API URL: {API_BASE_URL}")
    print(f"✓ Developer Key: {DEVELOPER_KEY[:10]}...")
    print(f"✓ Customer Key: {CUSTOMER_KEY[:10]}...")
    print(f"✓ Clinics to analyze: {CLINIC_NUMS}")
    
    # Analyze each clinic
    for clinic in CLINIC_NUMS:
        try:
            analyze_appointment_fields(clinic, limit=5)  # Analyze 5 appointments per clinic
        except Exception as e:
            print(f"Error analyzing clinic {clinic}: {e}")
    
    print(f"\n{'='*80}")
    print("DIAGNOSTIC COMPLETE")
    print(f"{'='*80}")
    print("\nNext steps:")
    print("1. Review the field names and values above")
    print("2. Update your main script with the correct field names")
    print("3. Update OPERATORY_APPOINTMENT_TYPE_FILTERS with exact values")
    print("4. Test with --dry-run --verbose")

if __name__ == '__main__':
    main()
