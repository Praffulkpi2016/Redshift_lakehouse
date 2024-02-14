TRUNCATE table bronze_bec_ods_stg.OKC_LINE_STYLES_TL;
INSERT INTO bronze_bec_ods_stg.OKC_LINE_STYLES_TL (
  id,
  `language`,
  source_lang,
  sfwt_flag,
  `name`,
  description,
  created_by,
  creation_date,
  last_updated_by,
  last_update_date,
  last_update_login,
  security_group_id,
  zd_edition_name,
  zd_sync,
  KCA_OPERATION,
  kca_seq_id,
  kca_seq_date
)
(
  SELECT
    id,
    `language`,
    source_lang,
    sfwt_flag,
    `name`,
    description,
    created_by,
    creation_date,
    last_updated_by,
    last_update_date,
    last_update_login,
    security_group_id,
    zd_edition_name,
    zd_sync,
    KCA_OPERATION,
    kca_seq_id,
    kca_seq_date
  FROM bec_raw_dl_ext.OKC_LINE_STYLES_TL
  WHERE
    kca_operation <> 'DELETE'
    AND COALESCE(kca_seq_id, '') <> ''
    AND (COALESCE(ID, 0), COALESCE(LANGUAGE, 'NA'), kca_seq_id) IN (
      SELECT
        COALESCE(ID, 0) AS ID,
        COALESCE(LANGUAGE, 'NA') AS LANGUAGE,
        MAX(kca_seq_id)
      FROM bec_raw_dl_ext.OKC_LINE_STYLES_TL
      WHERE
        kca_operation <> 'DELETE' AND COALESCE(kca_seq_id, '') <> ''
      GROUP BY
        COALESCE(ID, 0),
        COALESCE(LANGUAGE, 'NA')
    )
    AND kca_seq_date > (
      SELECT
        (
          executebegints - prune_days
        )
      FROM bec_etl_ctrl.batch_ods_info
      WHERE
        ods_table_name = 'okc_line_styles_tl'
    )
);