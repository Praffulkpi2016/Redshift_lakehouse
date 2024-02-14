/* Delete Records */
DELETE FROM silver_bec_ods.HR_LOCATIONS_ALL_TL
WHERE
  (location_id, language) IN (
    SELECT
      stg.location_id,
      stg.language
    FROM silver_bec_ods.HR_LOCATIONS_ALL_TL AS ods, bronze_bec_ods_stg.HR_LOCATIONS_ALL_TL AS stg
    WHERE
      ods.location_id = stg.location_id
      AND ods.language = stg.language
      AND stg.kca_operation IN ('INSERT', 'UPDATE')
  );
/* Insert records */
INSERT INTO silver_bec_ods.HR_LOCATIONS_ALL_TL (
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
  is_deleted_flg,
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
    KCA_OPERATION,
    'N' AS IS_DELETED_FLG,
    CAST(NULLIF(KCA_SEQ_ID, '') AS DECIMAL(36, 0)) AS KCA_SEQ_ID,
    kca_seq_date
  FROM bronze_bec_ods_stg.HR_LOCATIONS_ALL_TL
  WHERE
    kca_operation IN ('INSERT', 'UPDATE')
    AND (location_id, language, kca_seq_id) IN (
      SELECT
        location_id,
        language,
        MAX(kca_seq_id)
      FROM bronze_bec_ods_stg.HR_LOCATIONS_ALL_TL
      WHERE
        kca_operation IN ('INSERT', 'UPDATE')
      GROUP BY
        location_id,
        language
    )
);
/* Soft delete */
UPDATE silver_bec_ods.HR_LOCATIONS_ALL_TL SET IS_DELETED_FLG = 'N';
UPDATE silver_bec_ods.HR_LOCATIONS_ALL_TL SET IS_DELETED_FLG = 'Y'
WHERE
  (location_id, language) IN (
    SELECT
      location_id,
      language
    FROM bec_raw_dl_ext.HR_LOCATIONS_ALL_TL
    WHERE
      (location_id, language, KCA_SEQ_ID) IN (
        SELECT
          location_id,
          language,
          MAX(KCA_SEQ_ID) AS KCA_SEQ_ID
        FROM bec_raw_dl_ext.HR_LOCATIONS_ALL_TL
        GROUP BY
          location_id,
          language
      )
      AND kca_operation = 'DELETE'
  );
UPDATE bec_etl_ctrl.batch_ods_info SET last_refresh_date = CURRENT_TIMESTAMP()
WHERE
  ods_table_name = 'hr_locations_all_tl';