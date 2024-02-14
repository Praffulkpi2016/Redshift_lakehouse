TRUNCATE table
	table bronze_bec_ods_stg.MTL_CATEGORY_SETS_TL;
INSERT INTO bronze_bec_ods_stg.MTL_CATEGORY_SETS_TL (
  CATEGORY_SET_ID,
  LANGUAGE,
  SOURCE_LANG,
  CATEGORY_SET_NAME,
  DESCRIPTION,
  LAST_UPDATE_DATE,
  LAST_UPDATED_BY,
  CREATION_DATE,
  CREATED_BY,
  LAST_UPDATE_LOGIN,
  ZD_EDITION_NAME,
  ZD_SYNC,
  KCA_OPERATION,
  kca_seq_id,
  kca_seq_date
)
(
  SELECT
    CATEGORY_SET_ID,
    LANGUAGE,
    SOURCE_LANG,
    CATEGORY_SET_NAME,
    DESCRIPTION,
    LAST_UPDATE_DATE,
    LAST_UPDATED_BY,
    CREATION_DATE,
    CREATED_BY,
    LAST_UPDATE_LOGIN,
    ZD_EDITION_NAME,
    ZD_SYNC,
    KCA_OPERATION,
    kca_seq_id,
    kca_seq_date
  FROM bec_raw_dl_ext.MTL_CATEGORY_SETS_TL
  WHERE
    kca_operation <> 'DELETE'
    AND COALESCE(kca_seq_id, '') <> ''
    AND (CATEGORY_SET_ID, LANGUAGE, kca_seq_id) IN (
      SELECT
        CATEGORY_SET_ID,
        LANGUAGE,
        MAX(kca_seq_id)
      FROM bec_raw_dl_ext.MTL_CATEGORY_SETS_TL
      WHERE
        kca_operation <> 'DELETE' AND COALESCE(kca_seq_id, '') <> ''
      GROUP BY
        CATEGORY_SET_ID,
        LANGUAGE
    )
    AND kca_seq_date > (
      SELECT
        (
          executebegints - prune_days
        )
      FROM bec_etl_ctrl.batch_ods_info
      WHERE
        ods_table_name = 'mtl_category_sets_tl'
    )
);