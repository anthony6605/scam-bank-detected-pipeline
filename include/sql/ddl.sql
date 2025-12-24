create schema if not exists fraud_intel_raw;
create schema if not exists fraud_intel_ref;
create schema if not exists fraud_intel_curated;
create schema if not exists fraud_intel_mart;

create or replace table fraud_intel_raw.doc_ingest(
    ingest_id   string default uuid_string(),
    doc_id      string,
    source      string,
    url         string,
    fetched_at  timestamp_ntz,
    status_code number,
    content_type string,
    title       string,
    published_date string,
    text        string,
    content_hash string,
    load_ts  timestamp_ntz default current_timestamp(),

);
