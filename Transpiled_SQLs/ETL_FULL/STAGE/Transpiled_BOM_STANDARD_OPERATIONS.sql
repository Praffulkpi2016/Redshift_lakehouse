DROP TABLE IF EXISTS bronze_bec_ods_stg.BOM_STANDARD_OPERATIONS;
CREATE TABLE bronze_bec_ods_stg.bom_standard_operations AS
SELECT
  *
FROM bec_raw_dl_ext.bom_standard_operations
WHERE
  kca_operation <> 'DELETE'
  AND (COALESCE(LINE_ID, 0), COALESCE(OPERATION_TYPE, 0), COALESCE(STANDARD_OPERATION_ID, 0), last_update_date) IN (
    SELECT
      COALESCE(LINE_ID, 0) AS LINE_ID,
      COALESCE(OPERATION_TYPE, 0) AS OPERATION_TYPE,
      COALESCE(STANDARD_OPERATION_ID, 0) AS STANDARD_OPERATION_ID,
      MAX(last_update_date) AS last_update_date
    FROM bec_raw_dl_ext.bom_standard_operations
    WHERE
      kca_operation <> 'DELETE' AND COALESCE(kca_seq_id, '') = ''
    GROUP BY
      COALESCE(LINE_ID, 0),
      COALESCE(OPERATION_TYPE, 0),
      COALESCE(STANDARD_OPERATION_ID, 0)
  );