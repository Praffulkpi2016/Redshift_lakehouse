/* Delete Records */
DELETE FROM silver_bec_ods.gl_je_sources_tl
WHERE
  (JE_SOURCE_NAME, LANGUAGE) IN (
    SELECT
      stg.JE_SOURCE_NAME,
      stg.LANGUAGE
    FROM silver_bec_ods.gl_je_sources_tl AS ods, bronze_bec_ods_stg.gl_je_sources_tl AS stg
    WHERE
      ods.JE_SOURCE_NAME = stg.JE_SOURCE_NAME
      AND ods.LANGUAGE = stg.LANGUAGE
      AND stg.kca_operation IN ('INSERT', 'UPDATE')
  );
/* Insert records */
INSERT INTO silver_bec_ods.gl_je_sources_tl (
  JE_SOURCE_NAME,
  LAST_UPDATE_DATE,
  LAST_UPDATED_BY,
  OVERRIDE_EDITS_FLAG,
  USER_JE_SOURCE_NAME,
  JOURNAL_REFERENCE_FLAG,
  CREATION_DATE,
  CREATED_BY,
  LAST_UPDATE_LOGIN,
  DESCRIPTION,
  ATTRIBUTE1,
  ATTRIBUTE2,
  ATTRIBUTE3,
  ATTRIBUTE4,
  ATTRIBUTE5,
  CONTEXT,
  EFFECTIVE_DATE_RULE_CODE,
  JOURNAL_APPROVAL_FLAG,
  LANGUAGE,
  SOURCE_LANG,
  IMPORT_USING_KEY_FLAG,
  JE_SOURCE_KEY,
  KCA_OPERATION,
  IS_DELETED_FLG,
  KCA_SEQ_ID,
  kca_seq_date
)
(
  SELECT
    JE_SOURCE_NAME,
    LAST_UPDATE_DATE,
    LAST_UPDATED_BY,
    OVERRIDE_EDITS_FLAG,
    USER_JE_SOURCE_NAME,
    JOURNAL_REFERENCE_FLAG,
    CREATION_DATE,
    CREATED_BY,
    LAST_UPDATE_LOGIN,
    DESCRIPTION,
    ATTRIBUTE1,
    ATTRIBUTE2,
    ATTRIBUTE3,
    ATTRIBUTE4,
    ATTRIBUTE5,
    CONTEXT,
    EFFECTIVE_DATE_RULE_CODE,
    JOURNAL_APPROVAL_FLAG,
    LANGUAGE,
    SOURCE_LANG,
    IMPORT_USING_KEY_FLAG,
    JE_SOURCE_KEY,
    KCA_OPERATION,
    'N' AS IS_DELETED_FLG,
    CAST(NULLIF(KCA_SEQ_ID, '') AS DECIMAL(36, 0)) AS KCA_SEQ_ID,
    kca_seq_date
  FROM bronze_bec_ods_stg.gl_je_sources_tl
  WHERE
    kca_operation IN ('INSERT', 'UPDATE')
    AND (JE_SOURCE_NAME, LANGUAGE, kca_seq_id) IN (
      SELECT
        JE_SOURCE_NAME,
        LANGUAGE,
        MAX(kca_seq_id)
      FROM bronze_bec_ods_stg.gl_je_sources_tl
      WHERE
        kca_operation IN ('INSERT', 'UPDATE')
      GROUP BY
        JE_SOURCE_NAME,
        LANGUAGE
    )
);
/* Soft Delete */
UPDATE silver_bec_ods.gl_je_sources_tl SET IS_DELETED_FLG = 'N';
UPDATE silver_bec_ods.gl_je_sources_tl SET IS_DELETED_FLG = 'Y'
WHERE
  (JE_SOURCE_NAME, LANGUAGE) IN (
    SELECT
      JE_SOURCE_NAME,
      LANGUAGE
    FROM bec_raw_dl_ext.gl_je_sources_tl
    WHERE
      (JE_SOURCE_NAME, LANGUAGE, KCA_SEQ_ID) IN (
        SELECT
          JE_SOURCE_NAME,
          LANGUAGE,
          MAX(KCA_SEQ_ID) AS KCA_SEQ_ID
        FROM bec_raw_dl_ext.gl_je_sources_tl
        GROUP BY
          JE_SOURCE_NAME,
          LANGUAGE
      )
      AND kca_operation = 'DELETE'
  );
UPDATE bec_etl_ctrl.batch_ods_info SET last_refresh_date = CURRENT_TIMESTAMP()
WHERE
  ods_table_name = 'gl_je_sources_tl';