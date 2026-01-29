use database scam_bank_db;
use schema fraud_intel_raw;

create or replace file format parquet_ff type = parquet;

create or replace stage spark_stage
  file_format = parquet_ff;

-- after files are uploaded into @spark_stage, run:
copy into scam_bank_db.fraud_intel_raw.doc_ingest
  (doc_id, source, url, fetched_at, status_code, content_type, title, published_date, text, content_hash)
from (
  select
    $1:doc_id::string,
    $1:source::string,
    $1:url::string,
    $1:fetched_at::timestamp_ntz,
    $1:status_code::number,
    $1:content_type::string,
    $1:title::string,
    $1:published_date::string,
    $1:text::string,
    $1:content_hash::string
  from @spark_stage
)
on_error = 'continue';
