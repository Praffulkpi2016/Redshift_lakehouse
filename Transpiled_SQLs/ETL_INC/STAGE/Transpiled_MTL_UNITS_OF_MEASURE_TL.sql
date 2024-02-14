TRUNCATE table
	table bronze_bec_ods_stg.MTL_UNITS_OF_MEASURE_TL;
INSERT INTO bronze_bec_ods_stg.MTL_UNITS_OF_MEASURE_TL (
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
  KCA_OPERATION,
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
    KCA_OPERATION,
    kca_seq_id,
    kca_seq_date
  FROM bec_raw_dl_ext.MTL_UNITS_OF_MEASURE_TL
  WHERE
    kca_operation <> 'DELETE'
    AND COALESCE(kca_seq_id, '') <> ''
    AND (COALESCE(UNIT_OF_MEASURE, 'NA'), COALESCE(language, 'NA'), KCA_SEQ_ID) IN (
      SELECT
        COALESCE(UNIT_OF_MEASURE, 'NA') AS UNIT_OF_MEASURE,
        COALESCE(language, 'NA') AS language,
        MAX(KCA_SEQ_ID)
      FROM bec_raw_dl_ext.MTL_UNITS_OF_MEASURE_TL
      WHERE
        kca_operation <> 'DELETE' AND COALESCE(kca_seq_id, '') <> ''
      GROUP BY
        COALESCE(UNIT_OF_MEASURE, 'NA'),
        COALESCE(language, 'NA')
    )
    AND (
      kca_seq_date > (
        SELECT
          (
            executebegints - prune_days
          )
        FROM bec_etl_ctrl.batch_ods_info
        WHERE
          ods_table_name = 'mtl_units_of_measure_tl'
      )
    )
);