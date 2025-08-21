"""
Q2 Billed Data Monitor
Specific monitoring for Q2 Billed values to detect and prevent overwrites
"""

import psycopg2
import os
import logging
from datetime import datetime

logger = logging.getLogger(__name__)

class Q2DataMonitor:
    """Monitor Q2 Billed data for changes"""
    
    def __init__(self):
        self.database_url = os.environ.get('DATABASE_URL')
        self.monitored_value = None
        
    def get_current_q2_billed_total(self):
        """Get current Q2 billed total"""
        try:
            conn = psycopg2.connect(self.database_url)
            cursor = conn.cursor()
            
            cursor.execute("""
                SELECT COALESCE(SUM(CASE 
                    WHEN value::text ~ '^[0-9]+\.?[0-9]*$' THEN value::numeric
                    ELSE 0 
                END), 0) as total_billed
                FROM unified_sales_data 
                WHERE metric_type = 'Billed' 
                AND month = 'June' 
                AND year = 2025
                AND value IS NOT NULL 
                AND value::text != ''
            """)
            
            result = cursor.fetchone()
            total = float(result[0]) if result and result[0] else 0
            
            conn.close()
            return total
            
        except Exception as e:
            logger.error(f"Error getting Q2 billed total: {e}")
            return None
    
    def get_q2_billed_details(self):
        """Get detailed Q2 billed data"""
        try:
            conn = psycopg2.connect(self.database_url)
            cursor = conn.cursor()
            
            cursor.execute("""
                SELECT account_name, value, updated_at
                FROM unified_sales_data 
                WHERE metric_type = 'Billed' 
                AND month = 'June' 
                AND year = 2025
                AND value IS NOT NULL 
                AND value::text != ''
                AND value::text ~ '^[0-9]+\.?[0-9]*$'
                ORDER BY account_name
            """)
            
            results = cursor.fetchall()
            conn.close()
            return results
            
        except Exception as e:
            logger.error(f"Error getting Q2 billed details: {e}")
            return []
    
    def establish_baseline(self):
        """Establish baseline Q2 billed values"""
        total = self.get_current_q2_billed_total()
        details = self.get_q2_billed_details()
        
        self.monitored_value = total
        
        if total is not None:
            logger.info(f"Q2 Billed baseline established: ${total:,.2f}")
        else:
            logger.info("Q2 Billed baseline: No valid data found")
            total = 0
        logger.info(f"Q2 Billed details: {len(details)} accounts")
        
        return {"total": total, "details": details, "timestamp": datetime.now()}
    
    def check_for_changes(self, baseline):
        """Check if Q2 billed data has changed"""
        if not baseline:
            return False, "No baseline established"
        
        current_total = self.get_current_q2_billed_total()
        current_details = self.get_q2_billed_details()
        
        # Handle None values
        if current_total is None:
            current_total = 0
        if baseline["total"] is None:
            baseline["total"] = 0
        
        if abs(current_total - baseline["total"]) > 0.01:  # Allow small differences
            logger.error(f"🚨 Q2 BILLED DATA CHANGED!")
            logger.error(f"   Baseline: ${baseline['total']:,.2f}")
            logger.error(f"   Current:  ${current_total:,.2f}")
            logger.error(f"   Change:   ${current_total - baseline['total']:,.2f}")
            
            # Find specific account changes
            baseline_details = {item[0]: float(item[1]) for item in baseline["details"]}
            current_details_dict = {item[0]: float(item[1]) for item in current_details}
            
            for account, current_value in current_details_dict.items():
                baseline_value = baseline_details.get(account, 0)
                if abs(current_value - baseline_value) > 0.01:  # Allow for small rounding differences
                    logger.error(f"   📊 {account}: ${baseline_value:,.2f} → ${current_value:,.2f}")
            
            return True, f"Q2 Billed changed from ${baseline['total']:,.2f} to ${current_total:,.2f}"
        
        return False, "No changes detected"
    
    def protect_q2_data(self):
        """Enable protection for Q2 data"""
        baseline = self.establish_baseline()
        
        # Log protection status
        logger.info("🛡️  Q2 Billed Data Protection ACTIVE")
        logger.info(f"    Monitoring ${baseline['total']:,.2f} across {len(baseline['details'])} accounts")
        
        return baseline

# Global monitor instance
q2_monitor = Q2DataMonitor()

def protect_q2_data():
    """Enable Q2 data protection"""
    return q2_monitor.protect_q2_data()

def check_q2_changes(baseline):
    """Check for Q2 data changes"""
    return q2_monitor.check_for_changes(baseline)