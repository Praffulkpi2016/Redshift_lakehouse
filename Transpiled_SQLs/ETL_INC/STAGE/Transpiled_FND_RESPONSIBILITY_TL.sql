TRUNCATE table bronze_bec_ods_stg.fnd_responsibility_tl;
INSERT INTO bronze_bec_ods_stg.fnd_responsibility_tl (
  APPLICATION_ID,
  RESPONSIBILITY_ID,
  LANGUAGE,
  RESPONSIBILITY_NAME,
  CREATED_BY,
  CREATION_DATE,
  LAST_UPDATED_BY,
  LAST_UPDATE_DATE,
  LAST_UPDATE_LOGIN,
  DESCRIPTION,
  SOURCE_LANG,
  KCA_OPERATION,
  kca_seq_id,
  kca_seq_date
)
(
  SELECT
    APPLICATION_ID,
    RESPONSIBILITY_ID,
    LANGUAGE,
    RESPONSIBILITY_NAME,
    CREATED_BY,
    CREATION_DATE,
    LAST_UPDATED_BY,
    LAST_UPDATE_DATE,
    LAST_UPDATE_LOGIN,
    DESCRIPTION,
    SOURCE_LANG,
    KCA_OPERATION,
    kca_seq_id,
    kca_seq_date
  FROM bec_raw_dl_ext.fnd_responsibility_tl
  WHERE
    kca_operation <> 'DELETE'
    AND COALESCE(kca_seq_id, '') <> ''
    AND (COALESCE(APPLICATION_ID, 0), COALESCE(RESPONSIBILITY_ID, 0), COALESCE(LANGUAGE, 'NA'), kca_seq_id) IN (
      SELECT
        COALESCE(APPLICATION_ID, 0) AS APPLICATION_ID,
        COALESCE(RESPONSIBILITY_ID, 0) AS RESPONSIBILITY_ID,
        COALESCE(LANGUAGE, 'NA') AS LANGUAGE,
        MAX(kca_seq_id)
      FROM bec_raw_dl_ext.fnd_responsibility_tl
      WHERE
        kca_operation <> 'DELETE' AND COALESCE(kca_seq_id, '') <> ''
      GROUP BY
        COALESCE(APPLICATION_ID, 0),
        COALESCE(RESPONSIBILITY_ID, 0),
        COALESCE(LANGUAGE, 'NA')
    )
    AND (
      kca_seq_date > (
        SELECT
          (
            executebegints - prune_days
          )
        FROM bec_etl_ctrl.batch_ods_info
        WHERE
          ods_table_name = 'fnd_responsibility_tl'
      )
    )
);