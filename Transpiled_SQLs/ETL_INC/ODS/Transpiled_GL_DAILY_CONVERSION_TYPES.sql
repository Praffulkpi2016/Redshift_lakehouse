/* Delete Records */
DELETE FROM silver_bec_ods.GL_DAILY_CONVERSION_TYPES
WHERE
  conversion_type IN (
    SELECT
      stg.conversion_type
    FROM silver_bec_ods.GL_DAILY_CONVERSION_TYPES AS ods, bronze_bec_ods_stg.GL_DAILY_CONVERSION_TYPES AS stg
    WHERE
      ods.conversion_type = stg.conversion_type
      AND stg.kca_operation IN ('INSERT', 'UPDATE')
  );
/* Insert records */
INSERT INTO silver_bec_ods.GL_DAILY_CONVERSION_TYPES (
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
  is_deleted_flg,
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
    'N' AS IS_DELETED_FLG,
    CAST(NULLIF(KCA_SEQ_ID, '') AS DECIMAL(36, 0)) AS KCA_SEQ_ID,
    kca_seq_date
  FROM bronze_bec_ods_stg.GL_DAILY_CONVERSION_TYPES
  WHERE
    kca_operation IN ('INSERT', 'UPDATE')
    AND (conversion_type, kca_seq_id) IN (
      SELECT
        conversion_type,
        MAX(kca_seq_id)
      FROM bronze_bec_ods_stg.GL_DAILY_CONVERSION_TYPES
      WHERE
        kca_operation IN ('INSERT', 'UPDATE')
      GROUP BY
        conversion_type
    )
);
/* Soft delete */
UPDATE silver_bec_ods.GL_DAILY_CONVERSION_TYPES SET IS_DELETED_FLG = 'N';
UPDATE silver_bec_ods.GL_DAILY_CONVERSION_TYPES SET IS_DELETED_FLG = 'Y'
WHERE
  (
    conversion_type
  ) IN (
    SELECT
      conversion_type
    FROM bec_raw_dl_ext.GL_DAILY_CONVERSION_TYPES
    WHERE
      (conversion_type, KCA_SEQ_ID) IN (
        SELECT
          conversion_type,
          MAX(KCA_SEQ_ID) AS KCA_SEQ_ID
        FROM bec_raw_dl_ext.GL_DAILY_CONVERSION_TYPES
        GROUP BY
          conversion_type
      )
      AND kca_operation = 'DELETE'
  );
UPDATE bec_etl_ctrl.batch_ods_info SET last_refresh_date = CURRENT_TIMESTAMP()
WHERE
  ods_table_name = 'gl_daily_conversion_types';