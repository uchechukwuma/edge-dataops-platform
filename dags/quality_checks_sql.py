import psycopg2
import os
import logging

logger = logging.getLogger(__name__)

def run_sql_quality_checks():
    """Run 12 data quality checks using direct SQL (EN 50159 compliant)"""
    SUPABASE_URL = os.environ.get('SUPABASE_URL')
    if not SUPABASE_URL:
        raise ValueError("SUPABASE_URL not set")
    
    results = {'success': True, 'checks': {}, 'total_checks': 12}
    
    try:
        conn = psycopg2.connect(SUPABASE_URL)
        cur = conn.cursor()
        
        # Check 1: Row count (EN 50159 - Overflow Protection)
        cur.execute("SELECT COUNT(*) FROM dataops.silver_sensor_data")
        count = cur.fetchone()[0]
        if count < 1 or count > 500000:
            results['success'] = False
            results['checks']['row_count'] = {'passed': False, 'value': count}
            logger.warning(f'Check 1 - Row count {count} outside allowed range (1-500000)')
        else:
            results['checks']['row_count'] = {'passed': True, 'value': count}
            logger.info(f'Check 1 - Row count: {count} ✅')
        
        # Check 2: Column 'sensor_id' exists (EN 50159 - Schema Integrity)
        cur.execute("""
            SELECT EXISTS (
                SELECT 1 FROM information_schema.columns 
                WHERE table_schema = 'dataops' 
                AND table_name = 'silver_sensor_data' 
                AND column_name = 'sensor_id'
            )
        """)
        sensor_id_exists = cur.fetchone()[0]
        if not sensor_id_exists:
            results['success'] = False
            results['checks']['column_sensor_id'] = {'passed': False}
            logger.warning('Check 2 - Column sensor_id missing ❌')
        else:
            results['checks']['column_sensor_id'] = {'passed': True}
            logger.info('Check 2 - Column sensor_id exists ✅')
        
        # Check 3: Column 'value' exists (EN 50159 - Schema Integrity)
        cur.execute("""
            SELECT EXISTS (
                SELECT 1 FROM information_schema.columns 
                WHERE table_schema = 'dataops' 
                AND table_name = 'silver_sensor_data' 
                AND column_name = 'value'
            )
        """)
        value_exists = cur.fetchone()[0]
        if not value_exists:
            results['success'] = False
            results['checks']['column_value'] = {'passed': False}
            logger.warning('Check 3 - Column value missing ❌')
        else:
            results['checks']['column_value'] = {'passed': True}
            logger.info('Check 3 - Column value exists ✅')
        
        # Check 4: Column 'unit' exists (EN 50159 - Schema Integrity)
        cur.execute("""
            SELECT EXISTS (
                SELECT 1 FROM information_schema.columns 
                WHERE table_schema = 'dataops' 
                AND table_name = 'silver_sensor_data' 
                AND column_name = 'unit'
            )
        """)
        unit_exists = cur.fetchone()[0]
        if not unit_exists:
            results['success'] = False
            results['checks']['column_unit'] = {'passed': False}
            logger.warning('Check 4 - Column unit missing ❌')
        else:
            results['checks']['column_unit'] = {'passed': True}
            logger.info('Check 4 - Column unit exists ✅')
        
        # Check 5: Column 'checksum' exists (EN 50159 - Schema Integrity)
        cur.execute("""
            SELECT EXISTS (
                SELECT 1 FROM information_schema.columns 
                WHERE table_schema = 'dataops' 
                AND table_name = 'silver_sensor_data' 
                AND column_name = 'checksum'
            )
        """)
        checksum_exists = cur.fetchone()[0]
        if not checksum_exists:
            results['success'] = False
            results['checks']['column_checksum'] = {'passed': False}
            logger.warning('Check 5 - Column checksum missing ❌')
        else:
            results['checks']['column_checksum'] = {'passed': True}
            logger.info('Check 5 - Column checksum exists ✅')
        
        # Check 6: Column 'validated' exists (EN 50159 - Schema Integrity)
        cur.execute("""
            SELECT EXISTS (
                SELECT 1 FROM information_schema.columns 
                WHERE table_schema = 'dataops' 
                AND table_name = 'silver_sensor_data' 
                AND column_name = 'validated'
            )
        """)
        validated_exists = cur.fetchone()[0]
        if not validated_exists:
            results['success'] = False
            results['checks']['column_validated'] = {'passed': False}
            logger.warning('Check 6 - Column validated missing ❌')
        else:
            results['checks']['column_validated'] = {'passed': True}
            logger.info('Check 6 - Column validated exists ✅')
        
        # Check 7: Column 'source_timestamp' exists (EN 50159 - Schema Integrity)
        cur.execute("""
            SELECT EXISTS (
                SELECT 1 FROM information_schema.columns 
                WHERE table_schema = 'dataops' 
                AND table_name = 'silver_sensor_data' 
                AND column_name = 'source_timestamp'
            )
        """)
        timestamp_exists = cur.fetchone()[0]
        if not timestamp_exists:
            results['success'] = False
            results['checks']['column_timestamp'] = {'passed': False}
            logger.warning('Check 7 - Column source_timestamp missing ❌')
        else:
            results['checks']['column_timestamp'] = {'passed': True}
            logger.info('Check 7 - Column source_timestamp exists ✅')
        
        # Check 8: sensor_id pattern (EN 50159 - Masquerading Prevention)
        cur.execute("""
            SELECT COUNT(*) FROM dataops.silver_sensor_data 
            WHERE sensor_id !~ '^[a-z]+_sensor_[0-9]+$'
        """)
        bad_sensor_ids = cur.fetchone()[0]
        if bad_sensor_ids > 0:
            results['success'] = False
            results['checks']['sensor_id_pattern'] = {'passed': False, 'value': bad_sensor_ids}
            logger.warning(f'Check 8 - Sensor ID pattern: {bad_sensor_ids} invalid ❌')
        else:
            results['checks']['sensor_id_pattern'] = {'passed': True, 'value': bad_sensor_ids}
            logger.info('Check 8 - Sensor ID pattern: all valid ✅')
        
        # Check 9: value range (EN 50159 - Corruption Detection)
        cur.execute("SELECT COUNT(*) FROM dataops.silver_sensor_data WHERE value < -50 OR value > 2000")
        out_of_range = cur.fetchone()[0]
        if out_of_range > 0:
            results['success'] = False
            results['checks']['value_range'] = {'passed': False, 'value': out_of_range}
            logger.warning(f'Check 9 - Value range: {out_of_range} out of range ❌')
        else:
            results['checks']['value_range'] = {'passed': True, 'value': out_of_range}
            logger.info('Check 9 - Value range: all within limits ✅')
        
        # Check 10: validated = True (EN 50159 - Authentication)
        cur.execute("SELECT COUNT(*) FROM dataops.silver_sensor_data WHERE validated != true")
        unvalidated = cur.fetchone()[0]
        if unvalidated > 0:
            results['success'] = False
            results['checks']['validated_flag'] = {'passed': False, 'value': unvalidated}
            logger.warning(f'Check 10 - Validated flag: {unvalidated} invalid ❌')
        else:
            results['checks']['validated_flag'] = {'passed': True, 'value': unvalidated}
            logger.info('Check 10 - Validated flag: all valid ✅')
        
        # Check 11: checksum format (EN 50159 - Safety Code Verification)
        cur.execute("SELECT COUNT(*) FROM dataops.silver_sensor_data WHERE checksum !~ '^[0-9A-Fa-f]{2}$'")
        bad_checksums = cur.fetchone()[0]
        if bad_checksums > 0:
            results['success'] = False
            results['checks']['checksum_format'] = {'passed': False, 'value': bad_checksums}
            logger.warning(f'Check 11 - Checksum format: {bad_checksums} invalid ❌')
        else:
            results['checks']['checksum_format'] = {'passed': True, 'value': bad_checksums}
            logger.info('Check 11 - Checksum format: all valid ✅')
        
        # Check 12: unit allowed set (EN 50159 - Masquerading Prevention)
        cur.execute("""
            SELECT COUNT(*) FROM dataops.silver_sensor_data 
            WHERE unit NOT IN ('C', '%', 'hPa', 'mm/s', 'L/min')
        """)
        invalid_units = cur.fetchone()[0]
        if invalid_units > 0:
            results['success'] = False
            results['checks']['unit_allowed'] = {'passed': False, 'value': invalid_units}
            logger.warning(f'Check 12 - Unit allowed set: {invalid_units} invalid ❌')
        else:
            results['checks']['unit_allowed'] = {'passed': True, 'value': invalid_units}
            logger.info('Check 12 - Unit allowed set: all valid ✅')
        
        conn.close()
        
        passed_checks = sum(1 for c in results['checks'].values() if c.get('passed', False))
        total_checks = len(results['checks'])
        
        logger.info("=" * 60)
        logger.info(f"SUMMARY: {passed_checks}/{total_checks} checks passed")
        
        if results['success']:
            logger.info("✅ ALL 12 DATA QUALITY CHECKS PASSED!")
        else:
            failed = total_checks - passed_checks
            logger.error(f"❌ {failed} QUALITY CHECKS FAILED!")
            for name, check in results['checks'].items():
                if not check.get('passed', False):
                    value = check.get('value', 'N/A')
                    logger.error(f"  - Failed: {name} (value: {value})")
        
        return results['success']
        
    except Exception as e:
        logger.error(f"Error running SQL quality checks: {e}")
        return False
