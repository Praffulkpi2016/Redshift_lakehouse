TRUNCATE table bronze_bec_ods_stg.FND_TERRITORIES_TL;
INSERT INTO bronze_bec_ods_stg.FND_TERRITORIES_TL (
  territory_code,
  `language`,
  territory_short_name,
  created_by,
  creation_date,
  last_updated_by,
  last_update_date,
  last_update_login,
  description,
  source_lang,
  zd_edition_name,
  zd_sync,
  KCA_OPERATION,
  kca_seq_id,
  kca_seq_date
)
(
  SELECT
    territory_code,
    `language`,
    territory_short_name,
    created_by,
    creation_date,
    last_updated_by,
    last_update_date,
    last_update_login,
    description,
    source_lang,
    zd_edition_name,
    zd_sync,
    KCA_OPERATION,
    kca_seq_id,
    kca_seq_date
  FROM bec_raw_dl_ext.FND_TERRITORIES_TL
  WHERE
    kca_operation <> 'DELETE'
    AND COALESCE(kca_seq_id, '') <> ''
    AND (COALESCE(TERRITORY_CODE, 'NA'), COALESCE(LANGUAGE, 'NA'), kca_seq_id) IN (
      SELECT
        COALESCE(TERRITORY_CODE, 'NA') AS TERRITORY_CODE,
        COALESCE(LANGUAGE, 'NA') AS LANGUAGE,
        MAX(kca_seq_id)
      FROM bec_raw_dl_ext.FND_TERRITORIES_TL
      WHERE
        kca_operation <> 'DELETE' AND COALESCE(kca_seq_id, '') <> ''
      GROUP BY
        COALESCE(TERRITORY_CODE, 'NA'),
        COALESCE(LANGUAGE, 'NA')
    )
    AND kca_seq_date > (
      SELECT
        (
          executebegints - prune_days
        )
      FROM bec_etl_ctrl.batch_ods_info
      WHERE
        ods_table_name = 'fnd_territories_tl'
    )
);