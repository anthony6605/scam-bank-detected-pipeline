USE DATABASE SCAM_BANK_DB;

-- Close existing current versions when content changed
UPDATE SCAM_BANK_DB.FRAUD_INTEL_CURATED.DOC_VERSION tgt
SET
  valid_to = src.fetched_at,
  is_current = FALSE
FROM (
  SELECT
    doc_id,
    content_hash,
    MAX(fetched_at) AS fetched_at
  FROM SCAM_BANK_DB.FRAUD_INTEL_RAW.DOC_INGEST
  WHERE status_code BETWEEN 200 AND 299
    AND text IS NOT NULL
  GROUP BY doc_id, content_hash
) src
WHERE tgt.doc_id = src.doc_id
  AND tgt.is_current = TRUE
  AND tgt.content_hash <> src.content_hash;

-- Insert new versions (first time doc_id OR changed content_hash)
INSERT INTO SCAM_BANK_DB.FRAUD_INTEL_CURATED.DOC_VERSION
  (doc_id, source, url, title, published_date, text, content_hash, fetched_at, valid_from, valid_to, is_current)
SELECT
  i.doc_id,
  i.source,
  i.url,
  i.title,
  i.published_date,
  i.text,
  i.content_hash,
  i.fetched_at,
  i.fetched_at AS valid_from,
  NULL        AS valid_to,
  TRUE        AS is_current
FROM (
  SELECT *
  FROM SCAM_BANK_DB.FRAUD_INTEL_RAW.DOC_INGEST
  WHERE status_code BETWEEN 200 AND 299
    AND text IS NOT NULL
  QUALIFY ROW_NUMBER() OVER (PARTITION BY doc_id, content_hash ORDER BY fetched_at DESC) = 1
) i
LEFT JOIN SCAM_BANK_DB.FRAUD_INTEL_CURATED.DOC_VERSION v
  ON v.doc_id = i.doc_id
 AND v.content_hash = i.content_hash
WHERE v.doc_id IS NULL;

-- =========================
-- 2) Rebuild scores for current versions (simple approach)
-- =========================
DELETE FROM SCAM_BANK_DB.FRAUD_INTEL_CURATED.DOC_TYPOLOGY_SCORE;
DELETE FROM SCAM_BANK_DB.FRAUD_INTEL_CURATED.DOC_SIGNALS;

-- Typology scoring (regex match * weight)
INSERT INTO SCAM_BANK_DB.FRAUD_INTEL_CURATED.DOC_TYPOLOGY_SCORE (version_id, typology, score)
SELECT
  v.version_id,
  r.typology_id AS typology,
  SUM(r.weight) AS score
FROM SCAM_BANK_DB.FRAUD_INTEL_CURATED.DOC_VERSION v
JOIN SCAM_BANK_DB.FRAUD_INTEL_REF.TYPOLOGY_RULES r
  ON REGEXP_LIKE(v.text, r.pattern, 'i')
WHERE v.is_current = TRUE
GROUP BY v.version_id, r.typology_id;

-- Signal extraction (one row per matched signal)
INSERT INTO SCAM_BANK_DB.FRAUD_INTEL_CURATED.DOC_SIGNALS (version_id, signal_type, signal_value, score)
SELECT
  v.version_id,
  s.signal_type,
  s.signal_value,
  s.weight AS score
FROM SCAM_BANK_DB.FRAUD_INTEL_CURATED.DOC_VERSION v
JOIN SCAM_BANK_DB.FRAUD_INTEL_REF.SIGNAL_RULES s
  ON REGEXP_LIKE(v.text, s.pattern, 'i')
WHERE v.is_current = TRUE;
