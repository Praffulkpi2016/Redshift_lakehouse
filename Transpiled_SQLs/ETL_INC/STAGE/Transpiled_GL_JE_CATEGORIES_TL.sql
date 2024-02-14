TRUNCATE table bronze_bec_ods_stg.gl_je_categories_tl;
INSERT INTO bronze_bec_ods_stg.gl_je_categories_tl (
  JE_CATEGORY_NAME,
  LANGUAGE,
  SOURCE_LANG,
  USER_JE_CATEGORY_NAME,
  LAST_UPDATE_DATE,
  LAST_UPDATED_BY,
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
  CONSOLIDATION_FLAG,
  JE_CATEGORY_KEY,
  ZD_EDITION_NAME,
  ZD_SYNC,
  KCA_OPERATION,
  kca_seq_id,
  kca_seq_date
)
(
  SELECT
    JE_CATEGORY_NAME,
    LANGUAGE,
    SOURCE_LANG,
    USER_JE_CATEGORY_NAME,
    LAST_UPDATE_DATE,
    LAST_UPDATED_BY,
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
    CONSOLIDATION_FLAG,
    JE_CATEGORY_KEY,
    ZD_EDITION_NAME,
    ZD_SYNC,
    KCA_OPERATION,
    kca_seq_id,
    kca_seq_date
  FROM bec_raw_dl_ext.gl_je_categories_tl
  WHERE
    kca_operation <> 'DELETE'
    AND COALESCE(kca_seq_id, '') <> ''
    AND (JE_CATEGORY_NAME, LANGUAGE, kca_seq_id) IN (
      SELECT
        JE_CATEGORY_NAME,
        LANGUAGE,
        MAX(kca_seq_id)
      FROM bec_raw_dl_ext.gl_je_categories_tl
      WHERE
        kca_operation <> 'DELETE' AND COALESCE(kca_seq_id, '') <> ''
      GROUP BY
        JE_CATEGORY_NAME,
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
          ods_table_name = 'gl_je_categories_tl'
      )
    )
);