TRUNCATE table
	table bronze_bec_ods_stg.FA_LOCATIONS;
INSERT INTO bronze_bec_ods_stg.FA_LOCATIONS (
  location_id,
  segment1,
  segment2,
  segment3,
  segment4,
  segment5,
  segment6,
  segment7,
  summary_flag,
  enabled_flag,
  start_date_active,
  end_date_active,
  last_update_date,
  last_updated_by,
  last_update_login,
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
  attribute_category_code,
  KCA_OPERATION,
  kca_seq_id,
  kca_seq_date
)
(
  SELECT
    location_id,
    segment1,
    segment2,
    segment3,
    segment4,
    segment5,
    segment6,
    segment7,
    summary_flag,
    enabled_flag,
    start_date_active,
    end_date_active,
    last_update_date,
    last_updated_by,
    last_update_login,
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
    attribute_category_code,
    KCA_OPERATION,
    kca_seq_id,
    kca_seq_date
  FROM bec_raw_dl_ext.FA_LOCATIONS
  WHERE
    kca_operation <> 'DELETE'
    AND COALESCE(kca_seq_id, '') <> ''
    AND (COALESCE(LOCATION_ID, 0), KCA_SEQ_ID) IN (
      SELECT
        COALESCE(LOCATION_ID, 0) AS LOCATION_ID,
        MAX(KCA_SEQ_ID)
      FROM bec_raw_dl_ext.FA_LOCATIONS
      WHERE
        kca_operation <> 'DELETE' AND COALESCE(kca_seq_id, '') <> ''
      GROUP BY
        COALESCE(LOCATION_ID, 0)
    )
    AND kca_seq_date > (
      SELECT
        (
          executebegints - prune_days
        )
      FROM bec_etl_ctrl.batch_ods_info
      WHERE
        ods_table_name = 'fa_locations'
    )
);