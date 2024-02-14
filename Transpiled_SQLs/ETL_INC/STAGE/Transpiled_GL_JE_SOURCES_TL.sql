TRUNCATE table
	table bronze_bec_ods_stg.gl_je_sources_tl;
INSERT INTO bronze_bec_ods_stg.gl_je_sources_tl (
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
  kca_operation,
  kca_seq_id,
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
    kca_operation,
    kca_seq_id,
    kca_seq_date
  FROM bec_raw_dl_ext.gl_je_sources_tl
  WHERE
    kca_operation <> 'DELETE'
    AND COALESCE(kca_seq_id, '') <> ''
    AND (JE_SOURCE_NAME, LANGUAGE, kca_seq_id) IN (
      SELECT
        JE_SOURCE_NAME,
        LANGUAGE,
        MAX(kca_seq_id)
      FROM bec_raw_dl_ext.gl_je_sources_tl
      WHERE
        kca_operation <> 'DELETE' AND COALESCE(kca_seq_id, '') <> ''
      GROUP BY
        JE_SOURCE_NAME,
        LANGUAGE
    )
    AND (
      kca_seq_date > (
        SELECT
          (
            executebegints - prune_days
          )
        FROM bec_etl_ctrl.batch_ods_info
        WHERE
          ods_table_name = 'gl_je_sources_tl'
      )
    )
);