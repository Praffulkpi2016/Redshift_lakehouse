DROP table IF EXISTS bronze_bec_ods_stg.BOM_OPERATION_SEQUENCES;
CREATE TABLE bronze_bec_ods_stg.bom_operation_sequences AS
(
  SELECT
    *
  FROM bec_raw_dl_ext.bom_operation_sequences
  WHERE
    kca_operation <> 'DELETE'
    AND (COALESCE(OPERATION_SEQUENCE_ID, '0'), last_update_date) IN (
      SELECT
        COALESCE(OPERATION_SEQUENCE_ID, '0') AS OPERATION_SEQUENCE_ID,
        MAX(last_update_date)
      FROM bec_raw_dl_ext.bom_operation_sequences
      WHERE
        kca_operation <> 'DELETE' AND COALESCE(kca_seq_id, '') = ''
      GROUP BY
        COALESCE(OPERATION_SEQUENCE_ID, '0')
    )
);