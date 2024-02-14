TRUNCATE table bronze_bec_ods_stg.FND_APPLICATION_TL;
INSERT INTO bronze_bec_ods_stg.FND_APPLICATION_TL (
  APPLICATION_ID,
  LANGUAGE,
  APPLICATION_NAME,
  CREATED_BY,
  CREATION_DATE,
  LAST_UPDATED_BY,
  LAST_UPDATE_DATE,
  LAST_UPDATE_LOGIN,
  DESCRIPTION,
  SOURCE_LANG,
  ZD_EDITION_NAME,
  ZD_SYNC,
  KCA_OPERATION,
  kca_seq_id,
  kca_seq_date
)
(
  SELECT
    APPLICATION_ID,
    LANGUAGE,
    APPLICATION_NAME,
    CREATED_BY,
    CREATION_DATE,
    LAST_UPDATED_BY,
    LAST_UPDATE_DATE,
    LAST_UPDATE_LOGIN,
    DESCRIPTION,
    SOURCE_LANG,
    ZD_EDITION_NAME,
    ZD_SYNC,
    KCA_OPERATION,
    kca_seq_id,
    kca_seq_date
  FROM bec_raw_dl_ext.FND_APPLICATION_TL
  WHERE
    kca_operation <> 'DELETE'
    AND COALESCE(kca_seq_id, '') <> ''
    AND (APPLICATION_ID, LANGUAGE, KCA_SEQ_ID) IN (
      SELECT
        APPLICATION_ID,
        LANGUAGE,
        MAX(KCA_SEQ_ID)
      FROM bec_raw_dl_ext.FND_APPLICATION_TL
      WHERE
        kca_operation <> 'DELETE' AND COALESCE(kca_seq_id, '') <> ''
      GROUP BY
        APPLICATION_ID,
        LANGUAGE
    )
    AND kca_seq_date > (
      SELECT
        (
          executebegints - prune_days
        )
      FROM bec_etl_ctrl.batch_ods_info
      WHERE
        ods_table_name = 'fnd_application_tl'
    )
);