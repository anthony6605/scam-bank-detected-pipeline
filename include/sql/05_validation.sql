-- 05_validation.sql
use database scam_bank_db;

-- raw counts
select source, count(*) cnt
from scam_bank_db.fraud_intel_raw.doc_ingest
group by 1
order by cnt desc;

-- curated counts
select count(*) as doc_versions from scam_bank_db.fraud_intel_curated.doc_version;

select typology, count(*) cnt, avg(score) avg_score
from scam_bank_db.fraud_intel_curated.doc_typology_score
group by 1
order by cnt desc;

select signal_type, signal_value, count(*) cnt
from scam_bank_db.fraud_intel_curated.doc_signals
group by 1,2
order by cnt desc
limit 20;
