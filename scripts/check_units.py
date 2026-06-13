# to check for entries to get complete units for Great expectation.
# scripts/check_units.py
import psycopg2
import os
from dotenv import load_dotenv

load_dotenv()
url = os.environ.get('SUPABASE_URL')

conn = psycopg2.connect(url)
cur = conn.cursor()

# Get all unique unit values
cur.execute("""
    SELECT DISTINCT unit, COUNT(*) 
    FROM dataops.silver_sensor_data 
    GROUP BY unit 
    ORDER BY COUNT(*) DESC
""")

print("Unique unit values in your data:")
print("-" * 40)
for unit, count in cur.fetchall():
    print(f"  '{unit}': {count} records")

conn.close()