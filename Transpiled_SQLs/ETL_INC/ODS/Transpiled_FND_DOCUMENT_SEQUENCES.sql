/* Delete Records */
DELETE FROM silver_bec_ods.fnd_document_sequences
WHERE
  DOC_SEQUENCE_ID IN (
    SELECT
      stg.DOC_SEQUENCE_ID
    FROM silver_bec_ods.fnd_document_sequences AS ods, bronze_bec_ods_stg.fnd_document_sequences AS stg
    WHERE
      ods.DOC_SEQUENCE_ID = stg.DOC_SEQUENCE_ID
      AND stg.kca_operation IN ('INSERT', 'UPDATE')
  );
/* Insert records */
INSERT INTO silver_bec_ods.fnd_document_sequences (
  DOC_SEQUENCE_ID,
  NAME,
  LAST_UPDATE_DATE,
  LAST_UPDATED_BY,
  CREATION_DATE,
  CREATED_BY,
  LAST_UPDATE_LOGIN,
  APPLICATION_ID,
  AUDIT_TABLE_NAME,
  MESSAGE_FLAG,
  START_DATE,
  TYPE,
  DB_SEQUENCE_NAME,
  END_DATE,
  INITIAL_VALUE,
  TABLE_NAME,
  ATTRIBUTE_CATEGORY,
  ATTRIBUTE1,
  ATTRIBUTE2,
  ATTRIBUTE3,
  ATTRIBUTE4,
  ATTRIBUTE5,
  ATTRIBUTE6,
  ATTRIBUTE7,
  ATTRIBUTE8,
  ATTRIBUTE9,
  ATTRIBUTE10,
  ATTRIBUTE11,
  ATTRIBUTE12,
  ATTRIBUTE13,
  ATTRIBUTE14,
  ATTRIBUTE15,
  KCA_OPERATION,
  IS_DELETED_FLG,
  kca_seq_id,
  kca_seq_date
)
(
  SELECT
    DOC_SEQUENCE_ID,
    NAME,
    LAST_UPDATE_DATE,
    LAST_UPDATED_BY,
    CREATION_DATE,
    CREATED_BY,
    LAST_UPDATE_LOGIN,
    APPLICATION_ID,
    AUDIT_TABLE_NAME,
    MESSAGE_FLAG,
    START_DATE,
    TYPE,
    DB_SEQUENCE_NAME,
    END_DATE,
    INITIAL_VALUE,
    TABLE_NAME,
    ATTRIBUTE_CATEGORY,
    ATTRIBUTE1,
    ATTRIBUTE2,
    ATTRIBUTE3,
    ATTRIBUTE4,
    ATTRIBUTE5,
    ATTRIBUTE6,
    ATTRIBUTE7,
    ATTRIBUTE8,
    ATTRIBUTE9,
    ATTRIBUTE10,
    ATTRIBUTE11,
    ATTRIBUTE12,
    ATTRIBUTE13,
    ATTRIBUTE14,
    ATTRIBUTE15,
    KCA_OPERATION,
    'N' AS IS_DELETED_FLG,
    CAST(NULLIF(KCA_SEQ_ID, '') AS DECIMAL(36, 0)) AS KCA_SEQ_ID,
    kca_seq_date
  FROM bronze_bec_ods_stg.fnd_document_sequences
  WHERE
    kca_operation IN ('INSERT', 'UPDATE')
    AND (DOC_SEQUENCE_ID, kca_seq_id) IN (
      SELECT
        DOC_SEQUENCE_ID,
        MAX(kca_seq_id)
      FROM bronze_bec_ods_stg.fnd_document_sequences
      WHERE
        kca_operation IN ('INSERT', 'UPDATE')
      GROUP BY
        DOC_SEQUENCE_ID
    )
);
/* Soft delete */
UPDATE silver_bec_ods.fnd_document_sequences SET IS_DELETED_FLG = 'N';
UPDATE silver_bec_ods.fnd_document_sequences SET IS_DELETED_FLG = 'Y'
WHERE
  (
    DOC_SEQUENCE_ID
  ) IN (
    SELECT
      DOC_SEQUENCE_ID
    FROM bec_raw_dl_ext.fnd_document_sequences
    WHERE
      (DOC_SEQUENCE_ID, KCA_SEQ_ID) IN (
        SELECT
          DOC_SEQUENCE_ID,
          MAX(KCA_SEQ_ID) AS KCA_SEQ_ID
        FROM bec_raw_dl_ext.fnd_document_sequences
        GROUP BY
          DOC_SEQUENCE_ID
      )
      AND kca_operation = 'DELETE'
  );
UPDATE bec_etl_ctrl.batch_ods_info SET last_refresh_date = CURRENT_TIMESTAMP()
WHERE
  ods_table_name = 'fnd_document_sequences';