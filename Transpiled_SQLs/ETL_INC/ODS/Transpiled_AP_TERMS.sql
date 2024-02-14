/* Delete Records */
DELETE FROM silver_bec_ods.AP_TERMS
WHERE
  (term_id, language) IN (
    SELECT
      stg.term_id,
      stg.language
    FROM silver_bec_ods.AP_TERMS AS ods, bronze_bec_ods_stg.AP_TERMS AS stg
    WHERE
      ods.term_id = stg.term_id
      AND ods.language = stg.language
      AND stg.kca_operation IN ('INSERT', 'UPDATE')
  );
INSERT INTO silver_bec_ods.ap_terms (
  TERM_ID,
  LAST_UPDATE_DATE,
  LAST_UPDATED_BY,
  CREATION_DATE,
  CREATED_BY,
  LAST_UPDATE_LOGIN,
  ENABLED_FLAG,
  DUE_CUTOFF_DAY,
  TYPE,
  START_DATE_ACTIVE,
  END_DATE_ACTIVE,
  RANK,
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
  NAME,
  DESCRIPTION,
  KCA_OPERATION,
  IS_DELETED_FLG,
  KCA_SEQ_ID,
  kca_seq_date
)
(
  SELECT
    TERM_ID,
    LAST_UPDATE_DATE,
    LAST_UPDATED_BY,
    CREATION_DATE,
    CREATED_BY,
    LAST_UPDATE_LOGIN,
    ENABLED_FLAG,
    DUE_CUTOFF_DAY,
    TYPE,
    START_DATE_ACTIVE,
    END_DATE_ACTIVE,
    RANK,
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
    NAME,
    DESCRIPTION,
    KCA_OPERATION,
    'N' AS IS_DELETED_FLG,
    CAST(NULLIF(KCA_SEQ_ID, '') AS DECIMAL(36, 0)) AS KCA_SEQ_ID,
    kca_seq_date
  FROM bronze_bec_ods_stg.AP_TERMS
  WHERE
    kca_operation IN ('INSERT', 'UPDATE')
    AND (TERM_ID, language, kca_seq_id) IN (
      SELECT
        TERM_ID,
        language,
        MAX(kca_seq_id)
      FROM bronze_bec_ods_stg.AP_TERMS
      WHERE
        kca_operation IN ('INSERT', 'UPDATE')
      GROUP BY
        TERM_ID,
        language
    )
);
/* Soft delete */
UPDATE silver_bec_ods.AP_TERMS SET IS_DELETED_FLG = 'N';
UPDATE silver_bec_ods.AP_TERMS SET IS_DELETED_FLG = 'Y'
WHERE
  (TERM_ID, language) IN (
    SELECT
      TERM_ID,
      language
    FROM bec_raw_dl_ext.AP_TERMS_TL
    WHERE
      (TERM_ID, language, KCA_SEQ_ID) IN (
        SELECT
          TERM_ID,
          language,
          MAX(KCA_SEQ_ID) AS KCA_SEQ_ID
        FROM bec_raw_dl_ext.AP_TERMS_TL
        GROUP BY
          TERM_ID,
          language
      )
      AND kca_operation = 'DELETE'
  );
UPDATE bec_etl_ctrl.batch_ods_info SET load_type = 'I', last_refresh_date = CURRENT_TIMESTAMP()
WHERE
  ods_table_name = 'ap_terms';