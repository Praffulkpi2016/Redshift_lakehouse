/* Delete Records */
DELETE FROM silver_bec_ods.MTL_UNITS_OF_MEASURE_TL
WHERE
  (COALESCE(UNIT_OF_MEASURE, 'NA'), COALESCE(language, 'NA')) IN (
    SELECT
      COALESCE(stg.UNIT_OF_MEASURE, 'NA') AS UNIT_OF_MEASURE,
      COALESCE(stg.LANGUAGE, 'NA') AS language
    FROM silver_bec_ods.MTL_UNITS_OF_MEASURE_TL AS ods, bronze_bec_ods_stg.MTL_UNITS_OF_MEASURE_TL AS stg
    WHERE
      COALESCE(ods.UNIT_OF_MEASURE, 'NA') = COALESCE(stg.UNIT_OF_MEASURE, 'NA')
      AND COALESCE(ods.LANGUAGE, 'NA') = COALESCE(stg.LANGUAGE, 'NA')
      AND stg.kca_operation IN ('INSERT', 'UPDATE')
  );
/* Insert records */
INSERT INTO silver_bec_ods.MTL_UNITS_OF_MEASURE_TL (
  unit_of_measure,
  uom_code,
  uom_class,
  base_uom_flag,
  unit_of_measure_tl,
  last_update_date,
  last_updated_by,
  created_by,
  creation_date,
  last_update_login,
  disable_date,
  description,
  `language`,
  source_lang,
  attribute_category,
  attribute1,
  attribute2,
  attribute3,
  attribute4,
  attribute5,
  attribute6,
  attribute7,
  attribute8,
  attribute9,
  attribute10,
  attribute11,
  attribute12,
  attribute13,
  attribute14,
  attribute15,
  request_id,
  program_application_id,
  program_id,
  program_update_date,
  zd_edition_name,
  zd_sync,
  kca_operation,
  is_deleted_flg,
  kca_seq_id,
  kca_seq_date
)
(
  SELECT
    unit_of_measure,
    uom_code,
    uom_class,
    base_uom_flag,
    unit_of_measure_tl,
    last_update_date,
    last_updated_by,
    created_by,
    creation_date,
    last_update_login,
    disable_date,
    description,
    `language`,
    source_lang,
    attribute_category,
    attribute1,
    attribute2,
    attribute3,
    attribute4,
    attribute5,
    attribute6,
    attribute7,
    attribute8,
    attribute9,
    attribute10,
    attribute11,
    attribute12,
    attribute13,
    attribute14,
    attribute15,
    request_id,
    program_application_id,
    program_id,
    program_update_date,
    zd_edition_name,
    zd_sync,
    kca_operation,
    'N' AS IS_DELETED_FLG,
    CAST(NULLIF(KCA_SEQ_ID, '') AS DECIMAL(36, 0)) AS KCA_SEQ_ID,
    kca_seq_date
  FROM bronze_bec_ods_stg.MTL_UNITS_OF_MEASURE_TL
  WHERE
    kca_operation IN ('INSERT', 'UPDATE')
    AND (COALESCE(UNIT_OF_MEASURE, 'NA'), COALESCE(language, 'NA'), kca_seq_id) IN (
      SELECT
        COALESCE(UNIT_OF_MEASURE, 'NA') AS UNIT_OF_MEASURE,
        COALESCE(language, 'NA') AS language,
        MAX(kca_seq_id)
      FROM bronze_bec_ods_stg.MTL_UNITS_OF_MEASURE_TL
      WHERE
        kca_operation IN ('INSERT', 'UPDATE')
      GROUP BY
        COALESCE(UNIT_OF_MEASURE, 'NA'),
        COALESCE(language, 'NA')
    )
);
/* Soft delete */
UPDATE silver_bec_ods.MTL_UNITS_OF_MEASURE_TL SET IS_DELETED_FLG = 'N';
UPDATE silver_bec_ods.MTL_UNITS_OF_MEASURE_TL SET IS_DELETED_FLG = 'Y'
WHERE
  (UNIT_OF_MEASURE, language) IN (
    SELECT
      UNIT_OF_MEASURE,
      language
    FROM bec_raw_dl_ext.MTL_UNITS_OF_MEASURE_TL
    WHERE
      (UNIT_OF_MEASURE, language, KCA_SEQ_ID) IN (
        SELECT
          UNIT_OF_MEASURE,
          language,
          MAX(KCA_SEQ_ID) AS KCA_SEQ_ID
        FROM bec_raw_dl_ext.MTL_UNITS_OF_MEASURE_TL
        GROUP BY
          UNIT_OF_MEASURE,
          language
      )
      AND kca_operation = 'DELETE'
  );
UPDATE bec_etl_ctrl.batch_ods_info SET last_refresh_date = CURRENT_TIMESTAMP()
WHERE
  ods_table_name = 'mtl_units_of_measure_tl';