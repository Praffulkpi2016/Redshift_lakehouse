DROP TABLE IF EXISTS bronze_bec_ods_stg.CS_TRANSACTION_TYPES_B;
CREATE TABLE bronze_bec_ods_stg.CS_TRANSACTION_TYPES_B AS
SELECT
  *
FROM bec_raw_dl_ext.CS_TRANSACTION_TYPES_B
WHERE
  kca_operation <> 'DELETE'
  AND (COALESCE(TRANSACTION_TYPE_ID, 0), last_update_date) IN (
    SELECT
      COALESCE(TRANSACTION_TYPE_ID, 0) AS TRANSACTION_TYPE_ID,
      MAX(last_update_date) AS last_update_date
    FROM bec_raw_dl_ext.CS_TRANSACTION_TYPES_B
    WHERE
      kca_operation <> 'DELETE' AND COALESCE(kca_seq_id, '') = ''
    GROUP BY
      COALESCE(TRANSACTION_TYPE_ID, 0)
  );