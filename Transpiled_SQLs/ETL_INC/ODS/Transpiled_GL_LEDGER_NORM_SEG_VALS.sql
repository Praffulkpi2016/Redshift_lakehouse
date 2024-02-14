/* Delete Records */
DELETE FROM silver_bec_ods.GL_LEDGER_NORM_SEG_VALS
WHERE
  (
    RECORD_ID
  ) IN (
    SELECT
      stg.RECORD_ID
    FROM silver_bec_ods.GL_LEDGER_NORM_SEG_VALS AS ods, bronze_bec_ods_stg.GL_LEDGER_NORM_SEG_VALS AS stg
    WHERE
      ods.RECORD_ID = stg.RECORD_ID AND stg.kca_operation IN ('INSERT', 'UPDATE')
  );
/* Insert records */
INSERT INTO silver_bec_ods.GL_LEDGER_NORM_SEG_VALS (
  ledger_id,
  segment_type_code,
  segment_value,
  segment_value_type_code,
  record_id,
  last_update_date,
  last_updated_by,
  last_update_login,
  creation_date,
  created_by,
  start_date,
  end_date,
  status_code,
  request_id,
  legal_entity_id,
  sla_sequencing_flag,
  context,
  attribute1,
  attribute2,
  attribute3,
  attribute4,
  attribute5,
  attribute6,
  attribute7,
  attribute8,
  attribute9,
  attribute10,
  attribute11,
  attribute12,
  attribute13,
  attribute14,
  attribute15,
  kca_operation,
  is_deleted_flg,
  kca_seq_id,
  kca_seq_date
)
SELECT
  ledger_id,
  segment_type_code,
  segment_value,
  segment_value_type_code,
  record_id,
  last_update_date,
  last_updated_by,
  last_update_login,
  creation_date,
  created_by,
  start_date,
  end_date,
  status_code,
  request_id,
  legal_entity_id,
  sla_sequencing_flag,
  context,
  attribute1,
  attribute2,
  attribute3,
  attribute4,
  attribute5,
  attribute6,
  attribute7,
  attribute8,
  attribute9,
  attribute10,
  attribute11,
  attribute12,
  attribute13,
  attribute14,
  attribute15,
  kca_operation,
  'N' AS IS_DELETED_FLG,
  CAST(NULLIF(KCA_SEQ_ID, '') AS DECIMAL(36, 0)) AS KCA_SEQ_ID,
  kca_seq_date
FROM bronze_bec_ods_stg.GL_LEDGER_NORM_SEG_VALS
WHERE
  kca_operation IN ('INSERT', 'UPDATE')
  AND (RECORD_ID, kca_seq_id) IN (
    SELECT
      RECORD_ID,
      MAX(kca_seq_id)
    FROM bronze_bec_ods_stg.GL_LEDGER_NORM_SEG_VALS
    WHERE
      kca_operation IN ('INSERT', 'UPDATE')
    GROUP BY
      RECORD_ID
  );
/* Soft delete */
UPDATE silver_bec_ods.GL_LEDGER_NORM_SEG_VALS SET IS_DELETED_FLG = 'N';
UPDATE silver_bec_ods.GL_LEDGER_NORM_SEG_VALS SET IS_DELETED_FLG = 'Y'
WHERE
  (
    RECORD_ID
  ) IN (
    SELECT
      RECORD_ID
    FROM bec_raw_dl_ext.GL_LEDGER_NORM_SEG_VALS
    WHERE
      (RECORD_ID, KCA_SEQ_ID) IN (
        SELECT
          RECORD_ID,
          MAX(KCA_SEQ_ID) AS KCA_SEQ_ID
        FROM bec_raw_dl_ext.GL_LEDGER_NORM_SEG_VALS
        GROUP BY
          RECORD_ID
      )
      AND kca_operation = 'DELETE'
  );
UPDATE bec_etl_ctrl.batch_ods_info SET last_refresh_date = CURRENT_TIMESTAMP()
WHERE
  ods_table_name = 'gl_ledger_norm_seg_vals';