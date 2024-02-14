TRUNCATE table bronze_bec_ods_stg.FA_LOOKUPS_TL;
INSERT INTO bronze_bec_ods_stg.FA_LOOKUPS_TL (
  LOOKUP_TYPE,
  LOOKUP_CODE,
  LANGUAGE,
  SOURCE_LANG,
  MEANING,
  DESCRIPTION,
  LAST_UPDATE_DATE,
  LAST_UPDATED_BY,
  CREATED_BY,
  CREATION_DATE,
  LAST_UPDATE_LOGIN,
  ZD_EDITION_NAME,
  ZD_SYNC,
  KCA_OPERATION,
  kca_seq_id,
  kca_seq_date
)
(
  SELECT
    LOOKUP_TYPE,
    LOOKUP_CODE,
    LANGUAGE,
    SOURCE_LANG,
    MEANING,
    DESCRIPTION,
    LAST_UPDATE_DATE,
    LAST_UPDATED_BY,
    CREATED_BY,
    CREATION_DATE,
    LAST_UPDATE_LOGIN,
    ZD_EDITION_NAME,
    ZD_SYNC,
    KCA_OPERATION,
    kca_seq_id,
    kca_seq_date
  FROM bec_raw_dl_ext.FA_LOOKUPS_TL
  WHERE
    kca_operation <> 'DELETE'
    AND COALESCE(kca_seq_id, '') <> ''
    AND (COALESCE(LOOKUP_TYPE, 'NA'), COALESCE(LOOKUP_CODE, 'NA'), COALESCE(language, 'NA'), kca_seq_id) IN (
      SELECT
        COALESCE(LOOKUP_TYPE, 'NA') AS LOOKUP_TYPE,
        COALESCE(LOOKUP_CODE, 'NA') AS LOOKUP_CODE,
        COALESCE(language, 'NA') AS language,
        MAX(kca_seq_id) AS kca_seq_id
      FROM bec_raw_dl_ext.FA_LOOKUPS_TL
      WHERE
        kca_operation <> 'DELETE' AND COALESCE(kca_seq_id, '') <> ''
      GROUP BY
        COALESCE(LOOKUP_TYPE, 'NA'),
        COALESCE(LOOKUP_CODE, 'NA'),
        COALESCE(language, 'NA')
    )
    AND kca_seq_date > (
      SELECT
        (
          executebegints - prune_days
        )
      FROM bec_etl_ctrl.batch_ods_info
      WHERE
        ods_table_name = 'fa_lookups_tl'
    )
);