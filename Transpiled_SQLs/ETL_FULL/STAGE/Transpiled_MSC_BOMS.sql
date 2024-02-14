DROP TABLE IF EXISTS bronze_bec_ods_stg.MSC_BOMS;
CREATE TABLE bronze_bec_ods_stg.MSC_BOMS AS
SELECT
  *
FROM bec_raw_dl_ext.MSC_BOMS
WHERE
  kca_operation <> 'DELETE'
  AND (PLAN_ID, SR_INSTANCE_ID, BILL_SEQUENCE_ID, last_update_date) IN (
    SELECT
      PLAN_ID,
      SR_INSTANCE_ID,
      BILL_SEQUENCE_ID,
      MAX(last_update_date)
    FROM bec_raw_dl_ext.MSC_BOMS
    WHERE
      kca_operation <> 'DELETE' AND COALESCE(kca_seq_id, '') = ''
    GROUP BY
      PLAN_ID,
      SR_INSTANCE_ID,
      BILL_SEQUENCE_ID
  );