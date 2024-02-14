TRUNCATE table bronze_bec_ods_stg.GL_IMPORT_REFERENCES;
INSERT INTO bronze_bec_ods_stg.GL_IMPORT_REFERENCES (
  je_batch_id,
  je_header_id,
  je_line_num,
  last_update_date,
  last_updated_by,
  creation_date,
  created_by,
  last_update_login,
  reference_1,
  reference_2,
  reference_3,
  reference_4,
  reference_5,
  reference_6,
  reference_7,
  reference_8,
  reference_9,
  reference_10,
  subledger_doc_sequence_id,
  subledger_doc_sequence_value,
  gl_sl_link_id,
  gl_sl_link_table,
  kca_operation,
  kca_seq_id,
  KCA_SEQ_DATE
)
(
  SELECT
    je_batch_id,
    je_header_id,
    je_line_num,
    last_update_date,
    last_updated_by,
    creation_date,
    created_by,
    last_update_login,
    reference_1,
    reference_2,
    reference_3,
    reference_4,
    reference_5,
    reference_6,
    reference_7,
    reference_8,
    reference_9,
    reference_10,
    subledger_doc_sequence_id,
    subledger_doc_sequence_value,
    gl_sl_link_id,
    gl_sl_link_table,
    kca_operation,
    kca_seq_id,
    KCA_SEQ_DATE
  FROM bec_raw_dl_ext.GL_IMPORT_REFERENCES
  WHERE
    kca_operation <> 'DELETE'
    AND COALESCE(kca_seq_id, '') <> ''
    AND (COALESCE(JE_HEADER_ID, 0), COALESCE(JE_LINE_NUM, 0), COALESCE(SUBLEDGER_DOC_SEQUENCE_ID, 0), COALESCE(SUBLEDGER_DOC_SEQUENCE_VALUE, 0), COALESCE(GL_SL_LINK_ID, 0), kca_seq_id) IN (
      SELECT
        COALESCE(JE_HEADER_ID, 0) AS JE_HEADER_ID,
        COALESCE(JE_LINE_NUM, 0) AS JE_LINE_NUM,
        COALESCE(SUBLEDGER_DOC_SEQUENCE_ID, 0) AS SUBLEDGER_DOC_SEQUENCE_ID,
        COALESCE(SUBLEDGER_DOC_SEQUENCE_VALUE, 0) AS SUBLEDGER_DOC_SEQUENCE_VALUE,
        COALESCE(GL_SL_LINK_ID, 0) AS GL_SL_LINK_ID,
        MAX(kca_seq_id)
      FROM bec_raw_dl_ext.GL_IMPORT_REFERENCES
      WHERE
        kca_operation <> 'DELETE' AND COALESCE(kca_seq_id, '') <> ''
      GROUP BY
        COALESCE(JE_HEADER_ID, 0),
        COALESCE(JE_LINE_NUM, 0),
        COALESCE(SUBLEDGER_DOC_SEQUENCE_ID, 0),
        COALESCE(SUBLEDGER_DOC_SEQUENCE_VALUE, 0),
        COALESCE(GL_SL_LINK_ID, 0)
    )
    AND (
      KCA_SEQ_DATE > (
        SELECT
          (
            executebegints - prune_days
          )
        FROM bec_etl_ctrl.batch_ods_info
        WHERE
          ods_table_name = 'gl_import_references'
      )
    )
);