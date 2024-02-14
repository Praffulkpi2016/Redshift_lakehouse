TRUNCATE table bronze_bec_ods_stg.GL_LEDGER_SEGMENT_VALUES;
INSERT INTO bronze_bec_ods_stg.GL_LEDGER_SEGMENT_VALUES (
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
  kca_seq_id,
  kca_seq_date
)
(
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
    kca_seq_id,
    kca_seq_date
  FROM bec_raw_dl_ext.GL_LEDGER_SEGMENT_VALUES
  WHERE
    kca_operation <> 'DELETE'
    AND COALESCE(kca_seq_id, '') <> ''
    AND (LEDGER_ID, SEGMENT_TYPE_CODE, SEGMENT_VALUE, kca_seq_id) IN (
      SELECT
        LEDGER_ID,
        SEGMENT_TYPE_CODE,
        SEGMENT_VALUE,
        MAX(kca_seq_id)
      FROM bec_raw_dl_ext.GL_LEDGER_SEGMENT_VALUES
      WHERE
        kca_operation <> 'DELETE' AND COALESCE(kca_seq_id, '') <> ''
      GROUP BY
        LEDGER_ID,
        SEGMENT_TYPE_CODE,
        SEGMENT_VALUE
    )
    AND (
      kca_seq_date > (
        SELECT
          (
            executebegints - prune_days
          )
        FROM bec_etl_ctrl.batch_ods_info
        WHERE
          ods_table_name = 'gl_ledger_segment_values'
      )
    )
);