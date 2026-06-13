# scripts/gx_full_pipeline.py
"""
Complete Great Expectations Pipeline
- Creates datasource, asset, expectations (if not exist)
- Runs validation
- All in one session
"""
import great_expectations as gx
import os
from dotenv import load_dotenv

load_dotenv()

SUPABASE_URL = os.environ.get('SUPABASE_URL')
if not SUPABASE_URL:
    raise ValueError("SUPABASE_URL not found in .env file")

print("=" * 60)
print("GREAT EXPECTATIONS DATA QUALITY PIPELINE")
print("=" * 60)

# Create persistent context
context = gx.get_context(project_root_dir=".")
print(f"Context root: {context.root_directory}")

# Check if datasource exists, create if not
datasource_name = "supabase_silver"
try:
    datasource = context.get_datasource(datasource_name)
    print(f">> Using existing datasource: {datasource_name}")
except ValueError:
    print(f"📦 Creating new datasource: {datasource_name}")
    datasource = context.sources.add_postgres(
        name=datasource_name,
        connection_string=SUPABASE_URL
    )
    print(f">> Datasource created: {datasource_name}")

# Check if asset exists, create if not
asset_name = "silver_sensor_data"
try:
    data_asset = datasource.get_asset(asset_name)
    print(f">> Using existing asset: {asset_name}")
except Exception:
    print(f"📦 Creating new asset: {asset_name}")
    data_asset = datasource.add_table_asset(
        name=asset_name,
        table_name="silver_sensor_data",
        schema_name="dataops"
    )
    print(f">> Asset created: {asset_name}")

# Create or get expectation suite
suite_name = "silver_sensor_data_suite"
try:
    suite = context.get_expectation_suite(suite_name)
    print(f">> Using existing suite: {suite_name}")
except Exception:
    print(f"📦 Creating new suite: {suite_name}")
    suite = context.add_expectation_suite(suite_name)
    print(f">> Suite created: {suite_name}")

# Get validator
batch_request = data_asset.build_batch_request()
validator = context.get_validator(
    batch_request=batch_request,
    expectation_suite=suite
)

print("\n>> Adding expectations...")

# Add expectations (idempotent - will update if already exist)
validator.expect_table_row_count_to_be_between(min_value=40000, max_value=50000)

validator.expect_column_to_exist("sensor_id")
validator.expect_column_to_exist("value")
validator.expect_column_to_exist("unit")
validator.expect_column_to_exist("checksum")
validator.expect_column_to_exist("validated")
validator.expect_column_to_exist("source_timestamp")

validator.expect_column_values_to_match_regex(
    column="sensor_id",
    regex=r"^[a-z]+_sensor_\d+$"
)

validator.expect_column_values_to_be_between(
    column="value",
    min_value=-50,
    max_value=2000
)

validator.expect_column_values_to_be_in_set(
    column="validated",
    value_set=[True]
)

validator.expect_column_values_to_match_regex(
    column="checksum",
    regex=r"^[0-9A-Fa-f]{2}$"
)

validator.expect_column_values_to_be_in_set(
    column="unit",
    value_set=["C", "%", "hPa", "mm/s", "L/min"]
)

validator.expect_column_values_to_not_be_null("source_timestamp")

# Save expectations
validator.save_expectation_suite()
print(">> Expectations saved")

print("\n" + "=" * 60)
print("RUNNING VALIDATION")
print("=" * 60)

# Run validation
results = validator.validate()

print(f"\n{'='*50}")
print(f"VALIDATION RESULTS")
print(f"{'='*50}")
print(f"Success: {'>>' if results.success else '❌'} {results.success}")
print(f"Evaluated: {results.statistics['evaluated_expectations']}")
print(f"Successful: {results.statistics['successful_expectations']}")

if not results.success:
    failed = results.statistics['evaluated_expectations'] - results.statistics['successful_expectations']
    print(f"Failed: {failed}")
    
    print("\n>> DETAILS OF FAILED EXPECTATIONS:")
    for check in results.results:
        if not check.success:
            print(f"\n  Expectation: {check.expectation_config.expectation_type}")
            print(f"  Column: {check.expectation_config.kwargs.get('column', 'N/A')}")
            
            if 'unexpected_list' in check.result:
                unexpected = check.result['unexpected_list'][:5]
                print(f"  Unexpected values: {unexpected}")
                print(f"  Unexpected count: {check.result.get('unexpected_count', 0)}")
else:
    print(f"\n>> ALL DATA QUALITY CHECKS PASSED!")

# Build data docs
print("\n" + "=" * 60)
print("BUILDING DATA DOCS")
print("=" * 60)

context.build_data_docs()
print(">> Data Docs built")
print(">> Open: great_expectations/uncommitted/data_docs/local_site/index.html")

print("\n" + "=" * 60)
print("PIPELINE COMPLETE")
print("=" * 60)