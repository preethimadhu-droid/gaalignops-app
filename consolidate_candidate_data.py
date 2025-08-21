#!/usr/bin/env python3
"""
Consolidate candidate data from dataaggregator table into the enhanced candidate_data table
This completes the unified field strategy implementation
"""

import os
import psycopg2
import json
from datetime import datetime, date
import logging

# Set up logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

def clean_candidate_name(name):
    """Clean and standardize candidate names during import
    
    Fixes formatting issues like:
    - Leading/trailing spaces
    - Quotes and line breaks
    - Multiple spaces
    - Special characters
    """
    if not name:
        return name
    
    # Convert to string and strip whitespace
    cleaned_name = str(name).strip()
    
    # Remove quotes, line breaks, and other unwanted characters
    import re
    cleaned_name = re.sub(r'["\n\r\t]', '', cleaned_name)
    
    # Replace multiple spaces with single space
    cleaned_name = re.sub(r'\s+', ' ', cleaned_name)
    
    # Final trim
    cleaned_name = cleaned_name.strip()
    
    return cleaned_name if cleaned_name else name

def parse_date(date_str):
    """Parse various date formats from imported data"""
    if not date_str or date_str == '':
        return None
    
    try:
        # Try common date formats
        for fmt in ['%Y-%m-%d', '%d-%m-%Y', '%m/%d/%Y', '%d/%m/%Y', '%Y-%m-%d %H:%M:%S']:
            try:
                return datetime.strptime(str(date_str), fmt).date()
            except ValueError:
                continue
        return None
    except:
        return None

def apply_status_transformations(original_status):
    """Apply all status transformations based on dataaggregator status patterns
    
    Maps statuses from dataaggregator table format to standardized candidate_data format.
    Handles GA/Client prefixes and applies consolidation rules.
    
    Returns tuple: (standardized_status, status_flag, drop_reason)
    """
    if not original_status:
        return original_status, None, None
    
    status = original_status.strip()
    
    # Remove number prefixes (e.g., "19 - Candidate RNR/Dropped" → "Candidate RNR/Dropped")
    import re
    status = re.sub(r'^\d+\s*-\s*', '', status)
    
    # Determine status flag based on GA/Client prefixes FIRST
    status_flag = "Greyamp"  # Default
    if "Client" in status:
        status_flag = "Client"
    elif "GA" in status:
        status_flag = "Greyamp"
    
    # Apply specific status consolidation rules with drop reasons
    
    # DROPPED STATUS MAPPINGS
    if "Candidate RNR/Dropped" in status:
        return "Dropped", status_flag, "Candidate RNR/Dropped"
    elif "Duplicate Profile" in status:
        return "Dropped", status_flag, "Duplicate Profile"
    elif "Internal Dropped" in status:
        return "Dropped", status_flag, "Internal Dropped"
    
    # ON HOLD STATUS MAPPINGS  
    elif "Requirement on hold" in status:
        return "On Hold", status_flag, "Requirement on hold"
    elif "On Hold" in status or "On hold" in status:
        drop_reason = "GA On Hold" if status_flag == "Greyamp" else "Client On Hold"
        return "On Hold", status_flag, drop_reason
    
    # REJECTED STATUS MAPPINGS
    elif "Screen Rejected" in status:
        return "Rejected", status_flag, "Screen Rejected"  
    elif "Interview Rejected" in status or ("Rejected" in status and "Interview" in status):
        drop_reason = "GA Interview Rejected" if status_flag == "Greyamp" else "Client Interview Rejected"
        return "Rejected", status_flag, drop_reason
    elif "Rejected" in status:
        drop_reason = "GA Rejected" if status_flag == "Greyamp" else "Client Rejected"
        return "Rejected", status_flag, drop_reason
    
    # OTHER STATUS MAPPINGS (clean up status names)
    elif "Code Pairing" in status or "Assessment/Code Pairing" in status or "Tech + Code Pairing" in status:
        return "Code Pairing", status_flag, None
    elif "Tech Round" in status:
        return "Tech Round", status_flag, None
    elif "Screening" in status:
        return "Screening", status_flag, None
    elif "Sent to client" in status:
        return "Screening", "Client", None
    elif "Client Review" in status:
        return "Review", status_flag, None
    elif "GA Interview" in status:
        return "Interview", status_flag, None
    elif "Staffed" in status:
        return "Staffed", status_flag, None
    
    else:
        # For any remaining statuses, clean them up and assign proper flag
        clean_status = status
        
        # Remove GA/Client prefixes from status name but keep flag
        clean_status = re.sub(r'^(GA|Client)\s*[-_]?\s*', '', clean_status).strip()
        
        return clean_status, status_flag, None

def determine_default_status_flag(status):
    """Determine default status flag for unmatched statuses"""
    if not status:
        return None
    
    status_lower = status.lower()
    
    # Client-driven statuses
    client_indicators = ['hire', 'offer', 'onboard', 'selected', 'client']
    if any(indicator in status_lower for indicator in client_indicators):
        return "Client"
    
    # Internal/Greyamp statuses  
    internal_indicators = ['screen', 'profile', 'internal', 'vendor', 'sourcing']
    if any(indicator in status_lower for indicator in internal_indicators):
        return "Greyamp"
    
    # Default to Greyamp for unknown statuses
    return "Greyamp"

def map_status_to_workflow_stage(status):
    """Map imported status to our workflow stages"""
    if not status:
        return 1  # Default to "New Application"
    
    status_lower = status.lower()
    
    # Map common status patterns to workflow stages
    if 'screen' in status_lower and 'reject' in status_lower:
        return 10 if 'ga' in status_lower else 11
    elif 'interview' in status_lower and 'reject' in status_lower:
        return 12
    elif 'offer' in status_lower:
        return 15 if 'accept' in status_lower else 14
    elif 'hire' in status_lower or 'onboard' in status_lower:
        return 16
    elif 'drop' in status_lower or 'rnr' in status_lower:
        return 19
    else:
        return 1  # Default to "New Application"

def consolidate_candidate_data():
    """Consolidate dataaggregator records into candidate_data table with automatic transformations
    
    CRITICAL PRINCIPLE: APPEND-ONLY LOGIC
    - Only NEW records from dataaggregator are processed and transformed
    - Existing candidate_data records are NEVER updated or modified
    - Transformations apply only during initial import process
    - Preserves all historical data integrity
    
    Applies the following transformations to NEW records only:
    1. Status standardization (removes prefixes, standardizes GA/Client patterns)  
    2. Status Flag assignment (Greyamp vs Client determination)
    3. Source transformations (Vendor mapping logic)
    4. All field transformations integrated
    """
    
    conn = psycopg2.connect(os.environ.get('DATABASE_URL'))
    cursor = conn.cursor()
    
    try:
        # First, get all records from dataaggregator that have candidate names
        cursor.execute("""
            SELECT id, data, created_at 
            FROM dataaggregator 
            WHERE data->>'Candidate name' IS NOT NULL 
            AND data->>'Candidate name' != ''
            ORDER BY id
        """)
        
        aggregator_records = cursor.fetchall()
        logger.info(f"Found {len(aggregator_records)} records to consolidate")
        
        consolidated_count = 0
        
        for record_id, data, created_at in aggregator_records:
            try:
                # Extract fields from JSON data with cleaning
                candidate_name = clean_candidate_name(data.get('Candidate name', ''))
                role = data.get('Role', '').strip() if data.get('Role') else ''
                experience = data.get('Experience', '').strip() if data.get('Experience') else ''
                original_source = data.get('Source', '').strip() if data.get('Source') else ''
                location = data.get('Location', '').strip() if data.get('Location') else ''
                email_id = data.get('Email', '').strip() if data.get('Email') else ''
                contact_number = data.get('Contact Number', '').strip() if data.get('Contact Number') else ''
                expected_ctc = data.get('Expected CTC', '').strip() if data.get('Expected CTC') else ''
                original_status = data.get('Status', '').strip() if data.get('Status') else ''
                potential_client = data.get('Potential Client', '').strip() if data.get('Potential Client') else ''
                vendor_partner = data.get('Vendor Partner', '').strip() if data.get('Vendor Partner') else ''
                next_steps = data.get('Next Steps', '').strip() if data.get('Next Steps') else ''
                interview_feedback = data.get('Interview Feedback', '').strip() if data.get('Interview Feedback') else ''
                notice_period_details = data.get('Notice Period', '').strip() if data.get('Notice Period') else ''
                
                # Parse dates
                position_start_date = parse_date(data.get('Position Start Date'))
                profile_received_date = parse_date(data.get('Profile Received Date'))
                
                # Apply status transformations
                transformed_status, status_flag, drop_reason = apply_status_transformations(original_status)
                
                # Apply source transformations (Vendor mapping logic)
                if original_source and original_source.lower() == 'referral':
                    source = 'Referral'
                    # Keep existing vendor_partner if any
                elif original_source:
                    source = 'Vendor'
                    # Move original source to vendor_partner if not already set
                    if not vendor_partner:
                        vendor_partner = original_source
                else:
                    source = original_source
                
                # Map status to workflow stage
                workflow_stage_id = map_status_to_workflow_stage(transformed_status)
                
                # Find client ID if client name provided
                hire_for_client_id = None
                if potential_client:
                    cursor.execute("SELECT master_client_id FROM master_clients WHERE client_name ILIKE %s LIMIT 1", 
                                 (f"%{potential_client}%",))
                    client_result = cursor.fetchone()
                    if client_result:
                        hire_for_client_id = client_result[0]
                
                # Skip if name is empty after cleaning
                if not candidate_name or candidate_name.strip() == '':
                    logger.debug(f"Skipping record {record_id} - empty candidate name after cleaning")
                    continue
                
                # CRITICAL: APPEND-ONLY LOGIC - Check if candidate already exists
                # This ensures existing records are NEVER updated, only new records appended
                # Check both exact match and cleaned name match to handle formatting variants
                cursor.execute("""
                    SELECT id, status, status_flag, source FROM candidate_data 
                    WHERE TRIM(REGEXP_REPLACE(candidate_name, '["\n\r\t]+', '', 'g')) = %s
                    LIMIT 1
                """, (candidate_name,))
                
                existing_candidate = cursor.fetchone()
                if existing_candidate:
                    # PRESERVE EXISTING DATA - Skip this candidate entirely
                    # No updates, no transformations applied to existing records
                    logger.debug(f"PRESERVING existing candidate: {candidate_name} (ID: {existing_candidate[0]}, Status: {existing_candidate[1]}, Source: {existing_candidate[3]})")
                    continue
                
                # INSERT NEW RECORD ONLY - Apply all transformations to new imports only
                # Existing records remain untouched, preserving historical data integrity
                cursor.execute("""
                    INSERT INTO candidate_data (
                        candidate_name, role, experience_level, source, location,
                        email_id, contact_number, expected_ctc, next_steps, interview_feedback,
                        position_start_date, notice_period_details, status, status_flag, drop_reason,
                        hire_for_client_id, vendor_partner,
                        data_source, last_import_sync, created_date, profile_received_date,
                        import_conflicts, created_flag
                    ) VALUES (
                        %s, %s, %s, %s, %s,
                        %s, %s, %s, %s, %s,
                        %s, %s, %s, %s, %s,
                        %s, %s,
                        'import', %s, %s, %s,
                        %s, 'Y'
                    )
                """, (
                    candidate_name, role, experience, source, location,
                    email_id, contact_number, expected_ctc, next_steps, interview_feedback,
                    position_start_date, notice_period_details, transformed_status, status_flag, drop_reason,
                    hire_for_client_id, vendor_partner,
                    datetime.now(), created_at or datetime.now(), profile_received_date or created_at or datetime.now(),
                    json.dumps({'source_table': 'dataaggregator', 'source_id': record_id, 'original_status': original_status})
                ))
                
                consolidated_count += 1
                
                # Log transformation details for NEW RECORDS ONLY
                if original_status != transformed_status or original_source != source or drop_reason:
                    logger.debug(f"NEW RECORD transformations applied for {candidate_name}: "
                               f"Status '{original_status}' → '{transformed_status}' ({status_flag}), "
                               f"Drop Reason: '{drop_reason}', "
                               f"Source '{original_source}' → '{source}'")
                
                if consolidated_count % 50 == 0:
                    logger.info(f"Consolidated {consolidated_count} records with automatic transformations...")
                    
            except Exception as e:
                logger.error(f"Error consolidating record {record_id}: {str(e)}")
                continue
        
        # Commit all changes
        conn.commit()
        logger.info(f"Successfully consolidated {consolidated_count} records into candidate_data table with automatic transformations")
        
        # Verify the consolidation
        cursor.execute("SELECT COUNT(*) FROM candidate_data")
        total_candidates = cursor.fetchone()[0]
        logger.info(f"Total records in candidate_data table: {total_candidates}")
        
        # Show data source breakdown
        cursor.execute("SELECT data_source, COUNT(*) FROM candidate_data GROUP BY data_source")
        source_breakdown = cursor.fetchall()
        logger.info("Data source breakdown:")
        for source, count in source_breakdown:
            logger.info(f"  {source}: {count} records")
            
        # Show transformation statistics
        cursor.execute("SELECT status_flag, COUNT(*) FROM candidate_data WHERE status_flag IS NOT NULL GROUP BY status_flag")
        flag_breakdown = cursor.fetchall()
        logger.info("Status Flag distribution (automatic assignment):")
        for flag, count in flag_breakdown:
            logger.info(f"  {flag}: {count} records")
            
        # Show source transformation results
        cursor.execute("SELECT source, COUNT(*) FROM candidate_data GROUP BY source")
        source_transform_breakdown = cursor.fetchall()
        logger.info("Source transformation results:")
        for source, count in source_transform_breakdown:
            logger.info(f"  {source}: {count} records")
            
    except Exception as e:
        logger.error(f"Error during consolidation: {str(e)}")
        conn.rollback()
        raise
    finally:
        cursor.close()
        conn.close()

if __name__ == "__main__":
    logger.info("Starting candidate data consolidation...")
    consolidate_candidate_data()
    logger.info("Consolidation complete!")