DROP TABLE IF EXISTS bronze_bec_ods_stg.MSC_SUPPLIES;
CREATE TABLE bronze_bec_ods_stg.MSC_SUPPLIES AS
SELECT
  *
FROM bec_raw_dl_ext.MSC_SUPPLIES
WHERE
  kca_operation <> 'DELETE'
  AND (COALESCE(PLAN_ID, 0), COALESCE(SR_INSTANCE_ID, 0), COALESCE(TRANSACTION_ID, 0), last_update_date) IN (
    SELECT
      COALESCE(PLAN_ID, 0) AS PLAN_ID,
      COALESCE(SR_INSTANCE_ID, 0) AS SR_INSTANCE_ID,
      COALESCE(TRANSACTION_ID, 0) AS TRANSACTION_ID,
      MAX(last_update_date)
    FROM bec_raw_dl_ext.MSC_SUPPLIES
    WHERE
      kca_operation <> 'DELETE' AND COALESCE(kca_seq_id, '') = ''
    GROUP BY
      COALESCE(PLAN_ID, 0),
      COALESCE(SR_INSTANCE_ID, 0),
      COALESCE(TRANSACTION_ID, 0)
  );