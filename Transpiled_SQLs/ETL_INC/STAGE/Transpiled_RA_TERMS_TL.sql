TRUNCATE table bronze_bec_ods_stg.RA_TERMS_TL;
INSERT INTO bronze_bec_ods_stg.RA_TERMS_TL (
  TERM_ID,
  DESCRIPTION,
  NAME,
  LANGUAGE,
  SOURCE_LANG,
  LAST_UPDATE_DATE,
  CREATION_DATE,
  CREATED_BY,
  LAST_UPDATED_BY,
  LAST_UPDATE_LOGIN,
  ZD_EDITION_NAME,
  ZD_SYNC,
  KCA_OPERATION,
  KCA_SEQ_ID,
  KCA_SEQ_DATE
)
(
  SELECT
    TERM_ID,
    DESCRIPTION,
    NAME,
    LANGUAGE,
    SOURCE_LANG,
    LAST_UPDATE_DATE,
    CREATION_DATE,
    CREATED_BY,
    LAST_UPDATED_BY,
    LAST_UPDATE_LOGIN,
    ZD_EDITION_NAME,
    ZD_SYNC,
    KCA_OPERATION,
    KCA_SEQ_ID,
    KCA_SEQ_DATE
  FROM bec_raw_dl_ext.RA_TERMS_TL
  WHERE
    kca_operation <> 'DELETE'
    AND COALESCE(kca_seq_id, '') <> ''
    AND (TERM_ID, LANGUAGE, kca_seq_id) IN (
      SELECT
        TERM_ID,
        LANGUAGE,
        MAX(kca_seq_id)
      FROM bec_raw_dl_ext.RA_TERMS_TL
      WHERE
        kca_operation <> 'DELETE' AND COALESCE(kca_seq_id, '') <> ''
      GROUP BY
        TERM_ID,
        LANGUAGE
    )
    AND (
      KCA_SEQ_DATE > (
        SELECT
          (
            executebegints - prune_days
          )
        FROM bec_etl_ctrl.batch_ods_info
        WHERE
          ods_table_name = 'ra_terms_tl'
      )
    )
);