/* Delete Records */
DELETE FROM silver_bec_ods.FA_LOCATIONS
WHERE
  COALESCE(LOCATION_ID, 0) IN (
    SELECT
      COALESCE(stg.LOCATION_ID, 0) AS LOCATION_ID
    FROM silver_bec_ods.FA_LOCATIONS AS ods, bronze_bec_ods_stg.FA_LOCATIONS AS stg
    WHERE
      COALESCE(ods.LOCATION_ID, 0) = COALESCE(stg.LOCATION_ID, 0)
      AND stg.kca_operation IN ('INSERT', 'UPDATE')
  );
/* Insert records */
INSERT INTO silver_bec_ods.FA_LOCATIONS (
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
  kca_operation,
  IS_DELETED_FLG,
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
    kca_operation,
    'N' AS IS_DELETED_FLG,
    CAST(NULLIF(KCA_SEQ_ID, '') AS DECIMAL(36, 0)) AS KCA_SEQ_ID,
    kca_seq_date
  FROM bronze_bec_ods_stg.FA_LOCATIONS
  WHERE
    kca_operation IN ('INSERT', 'UPDATE')
    AND (COALESCE(LOCATION_ID, 0), KCA_SEQ_ID) IN (
      SELECT
        COALESCE(LOCATION_ID, 0) AS LOCATION_ID,
        MAX(KCA_SEQ_ID)
      FROM bronze_bec_ods_stg.FA_LOCATIONS
      WHERE
        kca_operation IN ('INSERT', 'UPDATE')
      GROUP BY
        COALESCE(LOCATION_ID, 0)
    )
);
/* Soft delete */
UPDATE silver_bec_ods.FA_LOCATIONS SET IS_DELETED_FLG = 'N';
UPDATE silver_bec_ods.FA_LOCATIONS SET IS_DELETED_FLG = 'Y'
WHERE
  (
    LOCATION_ID
  ) IN (
    SELECT
      LOCATION_ID
    FROM bec_raw_dl_ext.FA_LOCATIONS
    WHERE
      (LOCATION_ID, KCA_SEQ_ID) IN (
        SELECT
          LOCATION_ID,
          MAX(KCA_SEQ_ID) AS KCA_SEQ_ID
        FROM bec_raw_dl_ext.FA_LOCATIONS
        GROUP BY
          LOCATION_ID
      )
      AND kca_operation = 'DELETE'
  );
UPDATE bec_etl_ctrl.batch_ods_info SET last_refresh_date = CURRENT_TIMESTAMP()
WHERE
  ods_table_name = 'fa_locations';