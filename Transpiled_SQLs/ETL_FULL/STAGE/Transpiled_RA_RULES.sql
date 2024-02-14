DROP TABLE IF EXISTS bronze_bec_ods_stg.RA_RULES;
CREATE TABLE bronze_bec_ods_stg.RA_RULES AS
SELECT
  *
FROM bec_raw_dl_ext.RA_RULES
WHERE
  kca_operation <> 'DELETE'
  AND (RULE_ID, last_update_date) IN (
    SELECT
      RULE_ID,
      MAX(last_update_date)
    FROM bec_raw_dl_ext.RA_RULES
    WHERE
      kca_operation <> 'DELETE' AND COALESCE(kca_seq_id, '') = ''
    GROUP BY
      RULE_ID
  );