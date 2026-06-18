# scripts/clear_all_databases.py
"""
Clear both MongoDB and Supabase for a fresh test
"""

from pymongo import MongoClient
import psycopg2
import os
from dotenv import load_dotenv

load_dotenv()

print("=" * 50)
print("CLEARING DATABASES FOR FRESH TEST")
print("=" * 50)

# 1. Clear MongoDB
print("\n1. Clearing MongoDB...")
mongodb_url = os.environ.get('MONGODB_ATLAS_URL')
if mongodb_url:
    client = MongoClient(mongodb_url)
    db = client['edge_platform_bronze']
    collection = db['raw_sensor_telemetry']
    
    count = collection.count_documents({})
    print(f"   Documents before: {count}")
    
    result = collection.delete_many({})
    print(f"   Deleted: {result.deleted_count} documents")
    
    client.close()

# 2. Clear Supabase Silver Layer
print("\n2. Clearing Supabase Silver Layer...")
supabase_url = os.environ.get('SUPABASE_URL')
if supabase_url:
    conn = psycopg2.connect(supabase_url)
    cur = conn.cursor()
    
    cur.execute("SELECT COUNT(*) FROM dataops.silver_sensor_data")
    count = cur.fetchone()[0]
    print(f"   Records before: {count}")
    
    cur.execute("TRUNCATE TABLE dataops.silver_sensor_data RESTART IDENTITY")
    conn.commit()
    print(f"   Truncated silver_sensor_data table")
    
    cur.close()
    conn.close()

print("\n" + "=" * 50)
print(">> DATABASES CLEARED")
print("=" * 50)