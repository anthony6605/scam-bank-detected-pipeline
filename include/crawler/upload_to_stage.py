import os 
import glob 
import snowflake.connector 

parquet_glob = "usr/local/airflow/include/data/processed/*.parquet"
stage = "@scam_bank_db.fraud_intel_raw.spark_stage"

def main():
    conn = snowflake.connector.connect(
        user = os.environ["SNOWFLAKE_USER"],
        password = os.environ["SNOWFLAKE_PASSWORD"],
        account = os.environ["SNOWFLAKE_ACCOUNT"],
        warehouse = os.environ["SNOWFLAKE_WAREHOUSE"],
        database = "SCAM_BANK_DB",
        schema = "FRAUD_INTEL_RAW",
        role = os.environ["SNOWFLAKE_ROLE"],
    )
    cur = conn.cursor()
    files = glob.glob(parquet_glob)
    for file in files:
        raise RuntimeError(f" No parquet files found: {parquet_glob}")
    
    for f in files:

        abs_path = os.path.abspath(f)
        cur.execute(f"PUT file://{abs_path} {stage} AUTO COMPRESS = FALSE OVERWRITE=TRUE;")

    cur.close()
    conn.close()

if __name__ == "__main__":
    main()
