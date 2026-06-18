# scripts/gx_full_pipeline_v3.py
import great_expectations as gx
import os
import json
from dotenv import load_dotenv

# Force loading from the explicit Airflow home or active working directory root
load_dotenv(dotenv_path="/opt/airflow/.env" if os.path.exists("/opt/airflow/.env") else None)

SUPABASE_URL = os.environ.get('SUPABASE_URL')
if not SUPABASE_URL:
    print(">> CRITICAL FAILURE: SUPABASE_URL environment variable missing or unmapped.")
    print(">> Action: Add your connection parameters to the .env file.")
    exit(1)

print("=" * 60)
print("GREAT EXPECTATIONS V3 DATA QUALITY PIPELINE (GX 1.18.1 FIXED)")
print("=" * 60)

# Initialize context directly mapping the path inside the container
context = gx.get_context(context_root_dir="/opt/airflow/gx")
print(f"Context root verified at: {context.root_directory}")

datasource_name = "supabase_silver_v3"
asset_name = "silver_sensor_data"
suite_name = "silver_sensor_data_suite"

# 0. SAFETY FIX: Detect and remove invalid legacy suite files to avoid Marshmallow panic
suite_file_path = f"/opt/airflow/gx/expectations/{suite_name}.json"
if os.path.exists(suite_file_path):
    print(f">> Legacy suite file detected at {suite_file_path}.")
    try:
        # Check if it contains invalid format by attempting a test get
        context.suites.get(suite_name)
        print(">> Existing suite format is valid.")
    except Exception:
        print(">> Legacy suite format is broken/incompatible. Purging file for clean initialization...")
        os.remove(suite_file_path)
        # Force a quick metadata clear by recreating an empty valid schema file if needed
        if suite_name in [s.name for s in context.suites.all()]:
            context.suites.delete(suite_name)

# 1. Cleanly check and establish the Fluent Postgres Datasource
try:
    datasource = context.data_sources.get(datasource_name)
    print(f">> Using existing fluent datasource: {datasource_name}")
except Exception:
    print(f">> Creating new fluent datasource from secure environment path...")
    datasource = context.data_sources.add_postgres(
        name=datasource_name,
        connection_string=SUPABASE_URL
    )
    print(f">> Fluent Datasource created: {datasource_name}")

# 2. Cleanly check and establish the Table Asset
try:
    data_asset = datasource.get_asset(asset_name)
    print(f">> Using existing fluent asset: {asset_name}")
except Exception:
    print(f">> Creating new fluent table asset: {asset_name}")
    data_asset = datasource.add_table_asset(
        name=asset_name,
        table_name="silver_sensor_data",
        schema_name="dataops"
    )
    print(f">> Fluent Asset created: {asset_name}")

# 3. Establish Expectation Suite cleanly
try:
    suite = context.suites.get(suite_name)
    print(f">> Using existing suite: {suite_name}")
except Exception:
    print(f">> Instantiating fresh v1.x suite schema: {suite_name}")
    suite = context.suites.add(gx.ExpectationSuite(name=suite_name))
    print(f">> Suite initialized: {suite_name}")

# 4. Build batch request and link to validator
batch_request = data_asset.build_batch_request()
validator = context.get_validator(
    batch_request=batch_request,
    expectation_suite=suite
)

print("\n>> Enforcing safety rulesets over active records...")
validator.expect_table_row_count_to_be_between(min_value=1, max_value=50000)
validator.expect_column_to_exist("sensor_id")
validator.expect_column_to_exist("value")
validator.expect_column_to_exist("unit")
validator.expect_column_to_exist("checksum")
validator.expect_column_to_exist("validated")
validator.expect_column_to_exist("source_timestamp")

validator.expect_column_values_to_match_regex(column="sensor_id", regex=r"^[a-z]+_sensor_\d+$")
validator.expect_column_values_to_be_between(column="value", min_value=-50, max_value=2000)
validator.expect_column_values_to_be_in_set(column="validated", value_set=[True])
validator.expect_column_values_to_match_regex(column="checksum", regex=r"^[0-9A-Fa-f]{2}$")
validator.expect_column_values_to_be_in_set(column="unit", value_set=["C", "%", "hPa", "mm/s", "L/min"])
validator.expect_column_values_to_not_be_null("source_timestamp")

# Save expectations using the idempotent add_or_update factory method
context.suites.add_or_update(validator.expectation_suite)
print(">> Expectations synced and updated successfully.")

# 5. Run validation engine
results = validator.validate()

print(f"\n{'='*50}")
print(f"VALIDATION ENGINE METRICS")
print(f"{'='*50}")
print(f"Success Status: {'>> PASS' if results.success else '❌ FAIL'}")
print(f"Evaluated Assertions: {results.statistics['evaluated_expectations']}")
print(f"Successful Assertions: {results.statistics['successful_expectations']}")

if not results.success:
    failed = results.statistics['evaluated_expectations'] - results.statistics['successful_expectations']
    print(f">> Critical Fault: {failed} safety rules broken.")
    
    print("\n" + "!" * 50)
    print(">> EXPLICIT FAILURE DETAILS ")
    print("!" * 50)
    for validation_result in results.results:
        if not validation_result.success:
            # GX 1.x promoted 'type' directly to the configuration object
            rule_type = getattr(validation_result.expectation_config, "type", "Unknown Rule")
            column_name = validation_result.expectation_config.kwargs.get('column', 'Table-Level')
            
            print(f"\n>> FAILED EXPLICIT RULE: {rule_type}")
            print(f"   Target Column: {column_name}")
            
            res_details = validation_result.result
            if 'unexpected_count' in res_details:
                print(f"   Total Anomaly Count: {res_details.get('unexpected_count')}")
                print(f"   Sample Corrupted Values: {res_details.get('unexpected_list', [])[:5]}")
            elif 'observed_value' in res_details:
                print(f"   Observed Value: {res_details.get('observed_value')}")
    print("!" * 50 + "\n")
    exit(1)
else:
    print(">> Success: All 12 EN 50159 semantic constraints verified successfully.")

print("\n>> Compiling Human-Readable Safety Documentation Matrix...")
context.build_data_docs()
print(">> Verification Complete. Data Docs Materialized.")