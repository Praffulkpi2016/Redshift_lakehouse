DROP TABLE IF EXISTS bronze_bec_ods_stg.BOM_STRUCTURES_B;
CREATE TABLE bronze_bec_ods_stg.BOM_STRUCTURES_B AS
SELECT
  *
FROM bec_raw_dl_ext.BOM_STRUCTURES_B
WHERE
  kca_operation <> 'DELETE'
  AND (BILL_SEQUENCE_ID, last_update_date) IN (
    SELECT
      BILL_SEQUENCE_ID,
      MAX(last_update_date)
    FROM bec_raw_dl_ext.BOM_STRUCTURES_B
    WHERE
      kca_operation <> 'DELETE' AND COALESCE(kca_seq_id, '') = ''
    GROUP BY
      BILL_SEQUENCE_ID
  );