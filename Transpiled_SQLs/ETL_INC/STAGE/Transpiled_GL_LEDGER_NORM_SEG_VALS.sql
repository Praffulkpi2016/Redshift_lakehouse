TRUNCATE table bronze_bec_ods_stg.GL_LEDGER_NORM_SEG_VALS;
INSERT INTO bronze_bec_ods_stg.GL_LEDGER_NORM_SEG_VALS (
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
  kca_seq_id,
  kca_seq_date
)
(
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
    kca_seq_id,
    kca_seq_date
  FROM bec_raw_dl_ext.GL_LEDGER_NORM_SEG_VALS
  WHERE
    kca_operation <> 'DELETE'
    AND COALESCE(kca_seq_id, '') <> ''
    AND (RECORD_ID, kca_seq_id) IN (
      SELECT
        RECORD_ID,
        MAX(kca_seq_id)
      FROM bec_raw_dl_ext.GL_LEDGER_NORM_SEG_VALS
      WHERE
        kca_operation <> 'DELETE' AND COALESCE(kca_seq_id, '') <> ''
      GROUP BY
        RECORD_ID
    )
    AND (
      kca_seq_date > (
        SELECT
          (
            executebegints - prune_days
          )
        FROM bec_etl_ctrl.batch_ods_info
        WHERE
          ods_table_name = 'gl_ledger_norm_seg_vals'
      )
    )
);