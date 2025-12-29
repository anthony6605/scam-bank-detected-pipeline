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
    load_ts  timestamp_ntz default current_timestamp()

);
create or replace table fraud_intel_curated.doc_version (
    doc_id      string,
    version_id  string default uuid_string(),
    source      string,
    url         string,
    title       string,
    published_date string,
    text        string,
    content_hash string,
    fetched_at  timestamp_ntz,
    valid_from  timestamp_ntz,
    valid_to    timestamp_ntz,
    is_current  boolean default true
);

create or replace table fraud_intel_ref.typology_rules (
    typology_id   string,
    pattern       string,
    weight        number
);

create or replace table fraud_intel_ref.signal_rules (
    signal_type   string,
    signal_value  string,
    pattern      string,
    weight     number
);

create or replace table fraud_intel_curated.doc_typology_score (
   version_id  string,
   typology   string,
   score     number,
   scored_at timestamp_ntz default current_timestamp()
);

create or replace table fraud_intel_curated.doc_signals (
   version_id  string,
   signal_type string,
   signal_value string,
   score     number,
   extracted_at timestamp_ntz default current_timestamp()
);
