TRUNCATE table bronze_bec_ods_stg.fnd_document_sequences;
INSERT INTO bronze_bec_ods_stg.fnd_document_sequences (
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
    kca_seq_id,
    kca_seq_date
  FROM bec_raw_dl_ext.fnd_document_sequences
  WHERE
    kca_operation <> 'DELETE'
    AND COALESCE(kca_seq_id, '') <> ''
    AND (DOC_SEQUENCE_ID, kca_seq_id) IN (
      SELECT
        DOC_SEQUENCE_ID,
        MAX(kca_seq_id)
      FROM bec_raw_dl_ext.fnd_document_sequences
      WHERE
        kca_operation <> 'DELETE' AND COALESCE(kca_seq_id, '') <> ''
      GROUP BY
        DOC_SEQUENCE_ID
    )
    AND kca_seq_date > (
      SELECT
        (
          executebegints - prune_days
        )
      FROM bec_etl_ctrl.batch_ods_info
      WHERE
        ods_table_name = 'fnd_document_sequences'
    )
);