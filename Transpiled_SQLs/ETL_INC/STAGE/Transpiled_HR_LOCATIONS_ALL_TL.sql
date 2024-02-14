TRUNCATE table bronze_bec_ods_stg.HR_LOCATIONS_ALL_TL;
INSERT INTO bronze_bec_ods_stg.HR_LOCATIONS_ALL_TL (
  location_id,
  `language`,
  source_lang,
  location_code,
  description,
  last_update_date,
  last_updated_by,
  last_update_login,
  created_by,
  creation_date,
  kca_operation,
  kca_seq_id,
  kca_seq_date
)
(
  SELECT
    location_id,
    `language`,
    source_lang,
    location_code,
    description,
    last_update_date,
    last_updated_by,
    last_update_login,
    created_by,
    creation_date,
    kca_operation,
    kca_seq_id,
    kca_seq_date
  FROM bec_raw_dl_ext.HR_LOCATIONS_ALL_TL
  WHERE
    kca_operation <> 'DELETE'
    AND COALESCE(kca_seq_id, '') <> ''
    AND (location_id, language, kca_seq_id) IN (
      SELECT
        location_id,
        language,
        MAX(kca_seq_id)
      FROM bec_raw_dl_ext.HR_LOCATIONS_ALL_TL
      WHERE
        kca_operation <> 'DELETE' AND COALESCE(kca_seq_id, '') <> ''
      GROUP BY
        location_id,
        language
    )
    AND (
      kca_seq_date > (
        SELECT
          (
            executebegints - prune_days
          )
        FROM bec_etl_ctrl.batch_ods_info
        WHERE
          ods_table_name = 'hr_locations_all_tl'
      )
    )
);