TRUNCATE table bronze_bec_ods_stg.WF_ITEM_TYPES_TL;
INSERT INTO bronze_bec_ods_stg.WF_ITEM_TYPES_TL (
  `name`,
  `language`,
  display_name,
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
    `name`,
    `language`,
    display_name,
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
  FROM bec_raw_dl_ext.WF_ITEM_TYPES_TL
  WHERE
    kca_operation <> 'DELETE'
    AND COALESCE(kca_seq_id, '') <> ''
    AND (COALESCE(NAME, 'NA'), COALESCE(LANGUAGE, 'NA'), kca_seq_id) IN (
      SELECT
        COALESCE(NAME, 'NA') AS NAME,
        COALESCE(LANGUAGE, 'NA') AS LANGUAGE,
        MAX(kca_seq_id)
      FROM bec_raw_dl_ext.WF_ITEM_TYPES_TL
      WHERE
        kca_operation <> 'DELETE' AND COALESCE(kca_seq_id, '') <> ''
      GROUP BY
        COALESCE(NAME, 'NA'),
        COALESCE(LANGUAGE, 'NA')
    )
    AND KCA_SEQ_DATE > (
      SELECT
        (
          executebegints - prune_days
        )
      FROM bec_etl_ctrl.batch_ods_info
      WHERE
        ods_table_name = 'wf_item_types_tl'
    )
);