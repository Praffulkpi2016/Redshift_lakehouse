DROP TABLE IF EXISTS bronze_bec_ods_stg.MRP_SOURCING_RULES;
CREATE TABLE bronze_bec_ods_stg.MRP_SOURCING_RULES AS
SELECT
  *
FROM bec_raw_dl_ext.MRP_SOURCING_RULES
WHERE
  kca_operation <> 'DELETE'
  AND (SOURCING_RULE_ID, last_update_date) IN (
    SELECT
      SOURCING_RULE_ID,
      MAX(last_update_date)
    FROM bec_raw_dl_ext.MRP_SOURCING_RULES
    WHERE
      kca_operation <> 'DELETE' AND COALESCE(kca_seq_id, '') = ''
    GROUP BY
      SOURCING_RULE_ID
  );