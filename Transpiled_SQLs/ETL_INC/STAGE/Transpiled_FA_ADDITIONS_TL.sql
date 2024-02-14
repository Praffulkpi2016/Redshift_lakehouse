TRUNCATE table bronze_bec_ods_stg.FA_ADDITIONS_TL;
INSERT INTO bronze_bec_ods_stg.FA_ADDITIONS_TL (
  ASSET_ID,
  LANGUAGE,
  SOURCE_LANG,
  DESCRIPTION,
  LAST_UPDATE_DATE,
  LAST_UPDATED_BY,
  CREATED_BY,
  CREATION_DATE,
  LAST_UPDATE_LOGIN,
  KCA_OPERATION,
  kca_seq_id,
  kca_seq_date
)
(
  SELECT
    ASSET_ID,
    LANGUAGE,
    SOURCE_LANG,
    DESCRIPTION,
    LAST_UPDATE_DATE,
    LAST_UPDATED_BY,
    CREATED_BY,
    CREATION_DATE,
    LAST_UPDATE_LOGIN,
    KCA_OPERATION,
    kca_seq_id,
    kca_seq_date
  FROM bec_raw_dl_ext.FA_ADDITIONS_TL
  WHERE
    kca_operation <> 'DELETE'
    AND COALESCE(kca_seq_id, '') <> ''
    AND (COALESCE(ASSET_ID, 0), COALESCE(LANGUAGE, 'NA'), kca_seq_id) IN (
      SELECT
        COALESCE(ASSET_ID, 0),
        COALESCE(LANGUAGE, 'NA'),
        MAX(kca_seq_id)
      FROM bec_raw_dl_ext.FA_ADDITIONS_TL
      WHERE
        kca_operation <> 'DELETE' AND COALESCE(kca_seq_id, '') <> ''
      GROUP BY
        COALESCE(ASSET_ID, 0),
        COALESCE(LANGUAGE, 'NA')
    )
    AND kca_seq_date > (
      SELECT
        (
          executebegints - prune_days
        )
      FROM bec_etl_ctrl.batch_ods_info
      WHERE
        ods_table_name = 'fa_additions_tl'
    )
);