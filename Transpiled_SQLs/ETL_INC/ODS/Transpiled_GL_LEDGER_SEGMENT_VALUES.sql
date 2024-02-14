/* Delete Records */
DELETE FROM silver_bec_ods.GL_LEDGER_SEGMENT_VALUES
WHERE
  (LEDGER_ID, SEGMENT_TYPE_CODE, SEGMENT_VALUE) IN (
    SELECT
      stg.LEDGER_ID,
      stg.SEGMENT_TYPE_CODE,
      stg.SEGMENT_VALUE
    FROM silver_bec_ods.GL_LEDGER_SEGMENT_VALUES AS ods, bronze_bec_ods_stg.GL_LEDGER_SEGMENT_VALUES AS stg
    WHERE
      ods.LEDGER_ID = stg.LEDGER_ID
      AND ods.SEGMENT_TYPE_CODE = stg.SEGMENT_TYPE_CODE
      AND ods.SEGMENT_VALUE = stg.SEGMENT_VALUE
      AND stg.kca_operation IN ('INSERT', 'UPDATE')
  );
/* Insert records */
INSERT INTO silver_bec_ods.GL_LEDGER_SEGMENT_VALUES (
  ledger_id,
  segment_type_code,
  segment_value,
  parent_record_id,
  last_update_date,
  last_updated_by,
  last_update_login,
  creation_date,
  created_by,
  end_date,
  start_date,
  status_code,
  legal_entity_id,
  sla_sequencing_flag,
  kca_operation,
  is_deleted_flg,
  kca_seq_id,
  kca_seq_date
)
SELECT
  ledger_id,
  segment_type_code,
  segment_value,
  parent_record_id,
  last_update_date,
  last_updated_by,
  last_update_login,
  creation_date,
  created_by,
  end_date,
  start_date,
  status_code,
  legal_entity_id,
  sla_sequencing_flag,
  kca_operation,
  'N' AS IS_DELETED_FLG,
  CAST(NULLIF(KCA_SEQ_ID, '') AS DECIMAL(36, 0)) AS KCA_SEQ_ID,
  kca_seq_date
FROM bronze_bec_ods_stg.GL_LEDGER_SEGMENT_VALUES
WHERE
  kca_operation IN ('INSERT', 'UPDATE')
  AND (LEDGER_ID, SEGMENT_TYPE_CODE, SEGMENT_VALUE, kca_seq_id) IN (
    SELECT
      LEDGER_ID,
      SEGMENT_TYPE_CODE,
      SEGMENT_VALUE,
      MAX(kca_seq_id)
    FROM bronze_bec_ods_stg.GL_LEDGER_SEGMENT_VALUES
    WHERE
      kca_operation IN ('INSERT', 'UPDATE')
    GROUP BY
      LEDGER_ID,
      SEGMENT_TYPE_CODE,
      SEGMENT_VALUE
  );
/* Soft delete */
UPDATE silver_bec_ods.GL_LEDGER_SEGMENT_VALUES SET IS_DELETED_FLG = 'N';
UPDATE silver_bec_ods.GL_LEDGER_SEGMENT_VALUES SET IS_DELETED_FLG = 'Y'
WHERE
  (LEDGER_ID, SEGMENT_TYPE_CODE, SEGMENT_VALUE) IN (
    SELECT
      LEDGER_ID,
      SEGMENT_TYPE_CODE,
      SEGMENT_VALUE
    FROM bec_raw_dl_ext.GL_LEDGER_SEGMENT_VALUES
    WHERE
      (LEDGER_ID, SEGMENT_TYPE_CODE, SEGMENT_VALUE, KCA_SEQ_ID) IN (
        SELECT
          LEDGER_ID,
          SEGMENT_TYPE_CODE,
          SEGMENT_VALUE,
          MAX(KCA_SEQ_ID) AS KCA_SEQ_ID
        FROM bec_raw_dl_ext.GL_LEDGER_SEGMENT_VALUES
        GROUP BY
          LEDGER_ID,
          SEGMENT_TYPE_CODE,
          SEGMENT_VALUE
      )
      AND kca_operation = 'DELETE'
  );
UPDATE bec_etl_ctrl.batch_ods_info SET last_refresh_date = CURRENT_TIMESTAMP()
WHERE
  ods_table_name = 'gl_ledger_segment_values';