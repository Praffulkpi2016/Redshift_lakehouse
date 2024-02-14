TRUNCATE table bronze_bec_ods_stg.WF_ACTIVITIES_TL;
INSERT INTO bronze_bec_ods_stg.WF_ACTIVITIES_TL (
  item_type,
  `name`,
  version,
  display_name,
  `language`,
  protect_level,
  custom_level,
  description,
  source_lang,
  security_group_id,
  zd_edition_name,
  zd_sync,
  KCA_OPERATION,
  kca_seq_id,
  KCA_SEQ_DATE
)
(
  SELECT
    item_type,
    `name`,
    version,
    display_name,
    `language`,
    protect_level,
    custom_level,
    description,
    source_lang,
    security_group_id,
    zd_edition_name,
    zd_sync,
    KCA_OPERATION,
    kca_seq_id,
    KCA_SEQ_DATE
  FROM bec_raw_dl_ext.WF_ACTIVITIES_TL
  WHERE
    kca_operation <> 'DELETE'
    AND COALESCE(kca_seq_id, '') <> ''
    AND (COALESCE(ITEM_TYPE, 'NA'), COALESCE(NAME, 'NA'), COALESCE(VERSION, 0), COALESCE(language, 'NA'), kca_seq_id) IN (
      SELECT
        COALESCE(ITEM_TYPE, 'NA') AS ITEM_TYPE,
        COALESCE(NAME, 'NA') AS NAME,
        COALESCE(VERSION, 0) AS VERSION,
        COALESCE(language, 'NA') AS language,
        MAX(kca_seq_id)
      FROM bec_raw_dl_ext.WF_ACTIVITIES_TL
      WHERE
        kca_operation <> 'DELETE' AND COALESCE(kca_seq_id, '') <> ''
      GROUP BY
        COALESCE(ITEM_TYPE, 'NA'),
        COALESCE(NAME, 'NA'),
        COALESCE(VERSION, 0),
        COALESCE(language, 'NA')
    )
    AND (
      KCA_SEQ_DATE > (
        SELECT
          (
            executebegints - prune_days
          )
        FROM bec_etl_ctrl.batch_ods_info
        WHERE
          ods_table_name = 'wf_activities_tl'
      )
    )
);