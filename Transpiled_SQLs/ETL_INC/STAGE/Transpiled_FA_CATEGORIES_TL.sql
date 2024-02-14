TRUNCATE table bronze_bec_ods_stg.fa_categories_tl;
INSERT INTO bronze_bec_ods_stg.fa_categories_tl (
  CATEGORY_ID,
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
    CATEGORY_ID,
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
  FROM bec_raw_dl_ext.fa_categories_tl
  WHERE
    kca_operation <> 'DELETE'
    AND COALESCE(kca_seq_id, '') <> ''
    AND (COALESCE(CATEGORY_ID, 0), COALESCE(LANGUAGE, 'NA'), kca_seq_id) IN (
      SELECT
        COALESCE(CATEGORY_ID, 0) AS CATEGORY_ID,
        COALESCE(LANGUAGE, 'NA') AS LANGUAGE,
        MAX(kca_seq_id)
      FROM bec_raw_dl_ext.fa_categories_tl
      WHERE
        kca_operation <> 'DELETE' AND COALESCE(kca_seq_id, '') <> ''
      GROUP BY
        COALESCE(CATEGORY_ID, 0),
        COALESCE(LANGUAGE, 'NA')
    )
    AND kca_seq_date > (
      SELECT
        (
          executebegints - prune_days
        )
      FROM bec_etl_ctrl.batch_ods_info
      WHERE
        ods_table_name = 'fa_categories_tl'
    )
);