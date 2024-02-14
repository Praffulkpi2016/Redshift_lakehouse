/* Delete Records */
DELETE FROM silver_bec_ods.MSC_COMPANY_SITES
WHERE
  (COALESCE(COMPANY_ID, 0), COALESCE(COMPANY_SITE_ID, 0)) IN (
    SELECT
      COALESCE(stg.COMPANY_ID, 0) AS COMPANY_ID,
      COALESCE(stg.COMPANY_SITE_ID, 0) AS COMPANY_SITE_ID
    FROM silver_bec_ods.MSC_COMPANY_SITES AS ods, bronze_bec_ods_stg.MSC_COMPANY_SITES AS stg
    WHERE
      COALESCE(ods.COMPANY_ID, 0) = COALESCE(stg.COMPANY_ID, 0)
      AND COALESCE(ods.COMPANY_SITE_ID, 0) = COALESCE(stg.COMPANY_SITE_ID, 0)
      AND stg.kca_operation IN ('INSERT', 'UPDATE')
  );
/* Insert records */
INSERT INTO silver_bec_ods.MSC_COMPANY_SITES (
  company_site_id,
  company_id,
  company_site_name,
  sr_instance_id,
  deleted_flag,
  longitude,
  latitude,
  refresh_number,
  disable_date,
  planning_enabled,
  creation_date,
  created_by,
  last_update_date,
  last_updated_by,
  last_update_login,
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
  program_id,
  program_update_date,
  `location`,
  address1,
  address2,
  address3,
  address4,
  country,
  state,
  city,
  county,
  province,
  postal_code,
  KCA_OPERATION,
  IS_DELETED_FLG,
  kca_seq_id,
  kca_seq_date
)
(
  SELECT
    company_site_id,
    company_id,
    company_site_name,
    sr_instance_id,
    deleted_flag,
    longitude,
    latitude,
    refresh_number,
    disable_date,
    planning_enabled,
    creation_date,
    created_by,
    last_update_date,
    last_updated_by,
    last_update_login,
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
    program_id,
    program_update_date,
    `location`,
    address1,
    address2,
    address3,
    address4,
    country,
    state,
    city,
    county,
    province,
    postal_code,
    KCA_OPERATION,
    'N' AS IS_DELETED_FLG,
    CAST(NULLIF(KCA_SEQ_ID, '') AS DECIMAL(36, 0)) AS KCA_SEQ_ID,
    kca_seq_date
  FROM bronze_bec_ods_stg.MSC_COMPANY_SITES
  WHERE
    kca_operation IN ('INSERT', 'UPDATE')
    AND (COALESCE(COMPANY_ID, 0), COALESCE(COMPANY_SITE_ID, 0), KCA_SEQ_ID) IN (
      SELECT
        COALESCE(COMPANY_ID, 0) AS COMPANY_ID,
        COALESCE(COMPANY_SITE_ID, 0) AS COMPANY_SITE_ID,
        MAX(KCA_SEQ_ID)
      FROM bronze_bec_ods_stg.MSC_COMPANY_SITES
      WHERE
        kca_operation IN ('INSERT', 'UPDATE')
      GROUP BY
        COALESCE(COMPANY_ID, 0),
        COALESCE(COMPANY_SITE_ID, 0)
    )
);
/* Soft delete */
UPDATE silver_bec_ods.MSC_COMPANY_SITES SET IS_DELETED_FLG = 'N';
UPDATE silver_bec_ods.MSC_COMPANY_SITES SET IS_DELETED_FLG = 'Y'
WHERE
  (COMPANY_ID, COMPANY_SITE_ID) IN (
    SELECT
      COMPANY_ID,
      COMPANY_SITE_ID
    FROM bec_raw_dl_ext.MSC_COMPANY_SITES
    WHERE
      (COMPANY_ID, COMPANY_SITE_ID, KCA_SEQ_ID) IN (
        SELECT
          COMPANY_ID,
          COMPANY_SITE_ID,
          MAX(KCA_SEQ_ID) AS KCA_SEQ_ID
        FROM bec_raw_dl_ext.MSC_COMPANY_SITES
        GROUP BY
          COMPANY_ID,
          COMPANY_SITE_ID
      )
      AND kca_operation = 'DELETE'
  );
UPDATE bec_etl_ctrl.batch_ods_info SET last_refresh_date = CURRENT_TIMESTAMP()
WHERE
  ods_table_name = 'msc_company_sites';