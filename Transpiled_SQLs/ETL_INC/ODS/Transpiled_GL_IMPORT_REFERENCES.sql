/* Delete Records */
DELETE FROM silver_bec_ods.GL_IMPORT_REFERENCES
WHERE
  (COALESCE(JE_HEADER_ID, 0), COALESCE(JE_LINE_NUM, 0), COALESCE(SUBLEDGER_DOC_SEQUENCE_ID, 0), COALESCE(SUBLEDGER_DOC_SEQUENCE_VALUE, 0), COALESCE(GL_SL_LINK_ID, 0)) IN (
    SELECT
      COALESCE(stg.JE_HEADER_ID, 0) AS JE_HEADER_ID,
      COALESCE(stg.JE_LINE_NUM, 0) AS JE_LINE_NUM,
      COALESCE(stg.SUBLEDGER_DOC_SEQUENCE_ID, 0) AS SUBLEDGER_DOC_SEQUENCE_ID,
      COALESCE(stg.SUBLEDGER_DOC_SEQUENCE_VALUE, 0) AS SUBLEDGER_DOC_SEQUENCE_VALUE,
      COALESCE(stg.GL_SL_LINK_ID, 0) AS GL_SL_LINK_ID
    FROM silver_bec_ods.GL_IMPORT_REFERENCES AS ods, bronze_bec_ods_stg.GL_IMPORT_REFERENCES AS stg
    WHERE
      COALESCE(ods.JE_HEADER_ID, 0) = COALESCE(stg.JE_HEADER_ID, 0)
      AND COALESCE(ods.JE_LINE_NUM, 0) = COALESCE(stg.JE_LINE_NUM, 0)
      AND COALESCE(ods.SUBLEDGER_DOC_SEQUENCE_ID, 0) = COALESCE(stg.SUBLEDGER_DOC_SEQUENCE_ID, 0)
      AND COALESCE(ods.SUBLEDGER_DOC_SEQUENCE_VALUE, 0) = COALESCE(stg.SUBLEDGER_DOC_SEQUENCE_VALUE, 0)
      AND COALESCE(ods.GL_SL_LINK_ID, 0) = COALESCE(stg.GL_SL_LINK_ID, 0)
      AND stg.kca_operation IN ('INSERT', 'UPDATE')
  );
/* Insert records */
INSERT INTO silver_bec_ods.GL_IMPORT_REFERENCES (
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
  KCA_OPERATION,
  IS_DELETED_FLG,
  kca_seq_id,
  kca_seq_date
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
    KCA_OPERATION,
    'N' AS IS_DELETED_FLG,
    CAST(NULLIF(KCA_SEQ_ID, '') AS DECIMAL(36, 0)) AS KCA_SEQ_ID,
    kca_seq_date
  FROM bronze_bec_ods_stg.GL_IMPORT_REFERENCES
  WHERE
    kca_operation IN ('INSERT', 'UPDATE')
    AND (COALESCE(JE_HEADER_ID, 0), COALESCE(JE_LINE_NUM, 0), COALESCE(SUBLEDGER_DOC_SEQUENCE_ID, 0), COALESCE(SUBLEDGER_DOC_SEQUENCE_VALUE, 0), COALESCE(GL_SL_LINK_ID, 0), KCA_SEQ_ID) IN (
      SELECT
        COALESCE(JE_HEADER_ID, 0) AS JE_HEADER_ID,
        COALESCE(JE_LINE_NUM, 0) AS JE_LINE_NUM,
        COALESCE(SUBLEDGER_DOC_SEQUENCE_ID, 0) AS SUBLEDGER_DOC_SEQUENCE_ID,
        COALESCE(SUBLEDGER_DOC_SEQUENCE_VALUE, 0) AS SUBLEDGER_DOC_SEQUENCE_VALUE,
        COALESCE(GL_SL_LINK_ID, 0) AS GL_SL_LINK_ID,
        MAX(KCA_SEQ_ID)
      FROM bronze_bec_ods_stg.GL_IMPORT_REFERENCES
      WHERE
        kca_operation IN ('INSERT', 'UPDATE')
      GROUP BY
        COALESCE(JE_HEADER_ID, 0),
        COALESCE(JE_LINE_NUM, 0),
        COALESCE(SUBLEDGER_DOC_SEQUENCE_ID, 0),
        COALESCE(SUBLEDGER_DOC_SEQUENCE_VALUE, 0),
        COALESCE(GL_SL_LINK_ID, 0)
    )
);
/* Soft delete */
UPDATE silver_bec_ods.GL_IMPORT_REFERENCES SET IS_DELETED_FLG = 'Y'
WHERE
  (COALESCE(JE_HEADER_ID, 0), COALESCE(JE_LINE_NUM, 0), COALESCE(SUBLEDGER_DOC_SEQUENCE_ID, 0), COALESCE(SUBLEDGER_DOC_SEQUENCE_VALUE, 0), COALESCE(GL_SL_LINK_ID, 0)) IN (
    SELECT
      COALESCE(ext.JE_HEADER_ID, 0) AS JE_HEADER_ID,
      COALESCE(ext.JE_LINE_NUM, 0) AS JE_LINE_NUM,
      COALESCE(ext.SUBLEDGER_DOC_SEQUENCE_ID, 0) AS SUBLEDGER_DOC_SEQUENCE_ID,
      COALESCE(ext.SUBLEDGER_DOC_SEQUENCE_VALUE, 0) AS SUBLEDGER_DOC_SEQUENCE_VALUE,
      COALESCE(ext.GL_SL_LINK_ID, 0) AS GL_SL_LINK_ID
    FROM silver_bec_ods.GL_IMPORT_REFERENCES AS ods, bec_raw_dl_ext.GL_IMPORT_REFERENCES AS ext
    WHERE
      COALESCE(ods.JE_HEADER_ID, 0) = COALESCE(ext.JE_HEADER_ID, 0)
      AND COALESCE(ods.JE_LINE_NUM, 0) = COALESCE(ext.JE_LINE_NUM, 0)
      AND COALESCE(ods.SUBLEDGER_DOC_SEQUENCE_ID, 0) = COALESCE(ext.SUBLEDGER_DOC_SEQUENCE_ID, 0)
      AND COALESCE(ods.SUBLEDGER_DOC_SEQUENCE_VALUE, 0) = COALESCE(ext.SUBLEDGER_DOC_SEQUENCE_VALUE, 0)
      AND COALESCE(ods.GL_SL_LINK_ID, 0) = COALESCE(ext.GL_SL_LINK_ID, 0)
      AND ext.kca_operation = 'DELETE'
  );
UPDATE bec_etl_ctrl.batch_ods_info SET last_refresh_date = CURRENT_TIMESTAMP()
WHERE
  ods_table_name = 'gl_import_references';