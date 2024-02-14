TRUNCATE table
	table bronze_bec_ods_stg.GL_DAILY_CONVERSION_TYPES;
INSERT INTO bronze_bec_ods_stg.GL_DAILY_CONVERSION_TYPES (
  conversion_type,
  user_conversion_type,
  last_update_date,
  last_updated_by,
  creation_date,
  created_by,
  last_update_login,
  description,
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
  context,
  fem_enabled_flag,
  fem_scenario,
  fem_rate_type_code,
  fem_timeframe,
  security_flag,
  kca_operation,
  kca_seq_id,
  kca_seq_date
)
(
  SELECT
    conversion_type,
    user_conversion_type,
    last_update_date,
    last_updated_by,
    creation_date,
    created_by,
    last_update_login,
    description,
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
    context,
    fem_enabled_flag,
    fem_scenario,
    fem_rate_type_code,
    fem_timeframe,
    security_flag,
    kca_operation,
    kca_seq_id,
    kca_seq_date
  FROM bec_raw_dl_ext.GL_DAILY_CONVERSION_TYPES
  WHERE
    kca_operation <> 'DELETE'
    AND COALESCE(kca_seq_id, '') <> ''
    AND (conversion_type, kca_seq_id) IN (
      SELECT
        conversion_type,
        MAX(kca_seq_id)
      FROM bec_raw_dl_ext.GL_DAILY_CONVERSION_TYPES
      WHERE
        kca_operation <> 'DELETE' AND COALESCE(kca_seq_id, '') <> ''
      GROUP BY
        conversion_type
    )
    AND (
      kca_seq_date > (
        SELECT
          (
            executebegints - prune_days
          )
        FROM bec_etl_ctrl.batch_ods_info
        WHERE
          ods_table_name = 'gl_daily_conversion_types'
      )
    )
);