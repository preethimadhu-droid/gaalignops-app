"""
Assignment Synchronization Manager
Prevents data inconsistency between talent_supply and demand_supply_assignments tables
"""

import os
import psycopg2
import logging
from datetime import datetime

logger = logging.getLogger(__name__)

class AssignmentSyncManager:
    def __init__(self):
        self.database_url = os.environ.get('DATABASE_URL')
    
    def create_sync_triggers(self):
        """Create database triggers to automatically sync assignment data"""
        try:
            conn = psycopg2.connect(self.database_url)
            cursor = conn.cursor()
            
            # Create trigger function to sync assignment percentages
            cursor.execute("""
                CREATE OR REPLACE FUNCTION sync_talent_assignments()
                RETURNS TRIGGER AS $$
                BEGIN
                    -- Update talent_supply with current total assignments
                    UPDATE talent_supply 
                    SET 
                        assignment_percentage = COALESCE(
                            (SELECT SUM(assignment_percentage) 
                             FROM demand_supply_assignments 
                             WHERE talent_id = talent_supply.id 
                             AND status = 'Active'), 
                            0
                        ),
                        availability_percentage = GREATEST(0, 100 - COALESCE(
                            (SELECT SUM(assignment_percentage) 
                             FROM demand_supply_assignments 
                             WHERE talent_id = talent_supply.id 
                             AND status = 'Active'), 
                            0
                        )),
                        updated_at = CURRENT_TIMESTAMP
                    WHERE id = COALESCE(NEW.talent_id, OLD.talent_id);
                    
                    RETURN COALESCE(NEW, OLD);
                END;
                $$ LANGUAGE plpgsql;
            """)
            
            # Create triggers for INSERT, UPDATE, DELETE on demand_supply_assignments
            cursor.execute("""
                DROP TRIGGER IF EXISTS sync_on_assignment_insert ON demand_supply_assignments;
                CREATE TRIGGER sync_on_assignment_insert
                    AFTER INSERT ON demand_supply_assignments
                    FOR EACH ROW EXECUTE FUNCTION sync_talent_assignments();
            """)
            
            cursor.execute("""
                DROP TRIGGER IF EXISTS sync_on_assignment_update ON demand_supply_assignments;
                CREATE TRIGGER sync_on_assignment_update
                    AFTER UPDATE ON demand_supply_assignments
                    FOR EACH ROW EXECUTE FUNCTION sync_talent_assignments();
            """)
            
            cursor.execute("""
                DROP TRIGGER IF EXISTS sync_on_assignment_delete ON demand_supply_assignments;
                CREATE TRIGGER sync_on_assignment_delete
                    AFTER DELETE ON demand_supply_assignments
                    FOR EACH ROW EXECUTE FUNCTION sync_talent_assignments();
            """)
            
            conn.commit()
            conn.close()
            logger.info("Assignment synchronization triggers created successfully")
            return True
            
        except Exception as e:
            logger.error(f"Error creating sync triggers: {str(e)}")
            return False
    
    def validate_data_consistency(self):
        """Check for data inconsistencies between tables"""
        try:
            conn = psycopg2.connect(self.database_url)
            cursor = conn.cursor()
            
            # Find inconsistencies
            cursor.execute("""
                SELECT 
                    ts.name,
                    ts.assignment_percentage as supply_assigned,
                    COALESCE(SUM(dsa.assignment_percentage), 0) as actual_assigned,
                    ts.assignment_percentage - COALESCE(SUM(dsa.assignment_percentage), 0) as difference
                FROM talent_supply ts
                LEFT JOIN demand_supply_assignments dsa ON dsa.talent_id = ts.id AND dsa.status = 'Active'
                GROUP BY ts.id, ts.name, ts.assignment_percentage
                HAVING ABS(ts.assignment_percentage - COALESCE(SUM(dsa.assignment_percentage), 0)) > 0.01
                ORDER BY ABS(ts.assignment_percentage - COALESCE(SUM(dsa.assignment_percentage), 0)) DESC
            """)
            
            inconsistencies = cursor.fetchall()
            conn.close()
            
            if inconsistencies:
                logger.warning(f"Found {len(inconsistencies)} data inconsistencies")
                for name, supply_assigned, actual_assigned, difference in inconsistencies:
                    logger.warning(f"{name}: Supply={supply_assigned}%, Actual={actual_assigned}%, Diff={difference}%")
            else:
                logger.info("No data inconsistencies found")
            
            return {
                'consistent': len(inconsistencies) == 0,
                'inconsistencies': inconsistencies
            }
            
        except Exception as e:
            logger.error(f"Error validating data consistency: {str(e)}")
            return {'consistent': False, 'error': str(e)}
    
    def fix_all_inconsistencies(self):
        """Fix all data inconsistencies by syncing from assignments table"""
        try:
            conn = psycopg2.connect(self.database_url)
            cursor = conn.cursor()
            
            # Update all talent records to match assignment totals
            cursor.execute("""
                UPDATE talent_supply 
                SET 
                    assignment_percentage = COALESCE(
                        (SELECT SUM(dsa.assignment_percentage) 
                         FROM demand_supply_assignments dsa 
                         WHERE dsa.talent_id = talent_supply.id 
                         AND dsa.status = 'Active'), 
                        0
                    ),
                    availability_percentage = GREATEST(0, 100 - COALESCE(
                        (SELECT SUM(dsa.assignment_percentage) 
                         FROM demand_supply_assignments dsa 
                         WHERE dsa.talent_id = talent_supply.id 
                         AND dsa.status = 'Active'), 
                        0
                    )),
                    updated_at = CURRENT_TIMESTAMP
            """)
            
            updated_count = cursor.rowcount
            conn.commit()
            conn.close()
            
            logger.info(f"Fixed inconsistencies for {updated_count} talent records")
            return {'success': True, 'updated_count': updated_count}
            
        except Exception as e:
            logger.error(f"Error fixing inconsistencies: {str(e)}")
            return {'success': False, 'error': str(e)}
    
    def create_data_integrity_check(self):
        """Create a scheduled integrity check function"""
        try:
            conn = psycopg2.connect(self.database_url)
            cursor = conn.cursor()
            
            # Create table to log data integrity issues
            cursor.execute("""
                CREATE TABLE IF NOT EXISTS data_integrity_log (
                    id SERIAL PRIMARY KEY,
                    check_time TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    talent_name VARCHAR(255),
                    issue_type VARCHAR(100),
                    supply_value DECIMAL(5,2),
                    actual_value DECIMAL(5,2),
                    difference DECIMAL(5,2),
                    auto_fixed BOOLEAN DEFAULT FALSE
                )
            """)
            
            # Create function to automatically check and fix data integrity
            cursor.execute("""
                CREATE OR REPLACE FUNCTION check_data_integrity()
                RETURNS INTEGER AS $$
                DECLARE
                    inconsistency_record RECORD;
                    fixed_count INTEGER := 0;
                BEGIN
                    -- Log inconsistencies before fixing
                    FOR inconsistency_record IN
                        SELECT 
                            ts.name,
                            ts.assignment_percentage as supply_assigned,
                            COALESCE(SUM(dsa.assignment_percentage), 0) as actual_assigned,
                            ts.assignment_percentage - COALESCE(SUM(dsa.assignment_percentage), 0) as difference
                        FROM talent_supply ts
                        LEFT JOIN demand_supply_assignments dsa ON dsa.talent_id = ts.id AND dsa.status = 'Active'
                        GROUP BY ts.id, ts.name, ts.assignment_percentage
                        HAVING ABS(ts.assignment_percentage - COALESCE(SUM(dsa.assignment_percentage), 0)) > 0.01
                    LOOP
                        -- Log the inconsistency
                        INSERT INTO data_integrity_log (
                            talent_name, issue_type, supply_value, actual_value, difference, auto_fixed
                        ) VALUES (
                            inconsistency_record.name,
                            'assignment_mismatch',
                            inconsistency_record.supply_assigned,
                            inconsistency_record.actual_assigned,
                            inconsistency_record.difference,
                            TRUE
                        );
                        
                        fixed_count := fixed_count + 1;
                    END LOOP;
                    
                    -- Fix all inconsistencies
                    UPDATE talent_supply 
                    SET 
                        assignment_percentage = COALESCE(
                            (SELECT SUM(dsa.assignment_percentage) 
                             FROM demand_supply_assignments dsa 
                             WHERE dsa.talent_id = talent_supply.id 
                             AND dsa.status = 'Active'), 
                            0
                        ),
                        availability_percentage = GREATEST(0, 100 - COALESCE(
                            (SELECT SUM(dsa.assignment_percentage) 
                             FROM demand_supply_assignments dsa 
                             WHERE dsa.talent_id = talent_supply.id 
                             AND dsa.status = 'Active'), 
                            0
                        )),
                        updated_at = CURRENT_TIMESTAMP;
                    
                    RETURN fixed_count;
                END;
                $$ LANGUAGE plpgsql;
            """)
            
            conn.commit()
            conn.close()
            logger.info("Data integrity check system created successfully")
            return True
            
        except Exception as e:
            logger.error(f"Error creating integrity check system: {str(e)}")
            return False
    
    def run_integrity_check(self):
        """Run the integrity check and return results"""
        try:
            conn = psycopg2.connect(self.database_url)
            cursor = conn.cursor()
            
            cursor.execute("SELECT check_data_integrity()")
            fixed_count = cursor.fetchone()[0]
            
            conn.commit()
            conn.close()
            
            logger.info(f"Integrity check completed: {fixed_count} issues fixed")
            return {'success': True, 'fixed_count': fixed_count}
            
        except Exception as e:
            logger.error(f"Error running integrity check: {str(e)}")
            return {'success': False, 'error': str(e)}
    
    def get_integrity_log(self, limit=50):
        """Get recent data integrity issues"""
        try:
            conn = psycopg2.connect(self.database_url)
            cursor = conn.cursor()
            
            cursor.execute("""
                SELECT check_time, talent_name, issue_type, supply_value, actual_value, difference, auto_fixed
                FROM data_integrity_log
                ORDER BY check_time DESC
                LIMIT %s
            """, (limit,))
            
            logs = cursor.fetchall()
            conn.close()
            
            return {
                'success': True,
                'logs': [
                    {
                        'check_time': log[0],
                        'talent_name': log[1],
                        'issue_type': log[2],
                        'supply_value': float(log[3]),
                        'actual_value': float(log[4]),
                        'difference': float(log[5]),
                        'auto_fixed': log[6]
                    }
                    for log in logs
                ]
            }
            
        except Exception as e:
            logger.error(f"Error getting integrity log: {str(e)}")
            return {'success': False, 'error': str(e)}

def setup_complete_sync_system():
    """Complete setup of the synchronization system"""
    sync_manager = AssignmentSyncManager()
    
    # Step 1: Fix current inconsistencies
    print("Step 1: Fixing current data inconsistencies...")
    fix_result = sync_manager.fix_all_inconsistencies()
    if fix_result['success']:
        print(f"✅ Fixed {fix_result['updated_count']} records")
    else:
        print(f"❌ Error fixing inconsistencies: {fix_result['error']}")
        return False
    
    # Step 2: Create triggers for automatic sync
    print("Step 2: Creating automatic synchronization triggers...")
    if sync_manager.create_sync_triggers():
        print("✅ Synchronization triggers created")
    else:
        print("❌ Failed to create triggers")
        return False
    
    # Step 3: Create integrity check system
    print("Step 3: Creating data integrity monitoring...")
    if sync_manager.create_data_integrity_check():
        print("✅ Data integrity monitoring system created")
    else:
        print("❌ Failed to create integrity monitoring")
        return False
    
    # Step 4: Validate everything is working
    print("Step 4: Validating data consistency...")
    validation = sync_manager.validate_data_consistency()
    if validation['consistent']:
        print("✅ All data is now consistent")
    else:
        print(f"⚠️ Found {len(validation['inconsistencies'])} remaining issues")
    
    print("🎉 Assignment synchronization system setup complete!")
    return True

if __name__ == "__main__":
    setup_complete_sync_system()