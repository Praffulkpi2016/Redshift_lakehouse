/* Delete Records */
DELETE FROM silver_bec_ods.MSC_TRADING_PARTNER_SITES
WHERE
  (
    COALESCE(PARTNER_SITE_ID, 0)
  ) IN (
    SELECT
      COALESCE(stg.PARTNER_SITE_ID, 0) AS PARTNER_SITE_ID
    FROM silver_bec_ods.MSC_TRADING_PARTNER_SITES AS ods, bronze_bec_ods_stg.MSC_TRADING_PARTNER_SITES AS stg
    WHERE
      COALESCE(ods.PARTNER_SITE_ID, 0) = COALESCE(stg.PARTNER_SITE_ID, 0)
      AND stg.kca_operation IN ('INSERT', 'UPDATE')
  );
/* Insert records */
INSERT INTO silver_bec_ods.MSC_TRADING_PARTNER_SITES (
  partner_site_id,
  sr_tp_site_id,
  sr_instance_id,
  partner_id,
  partner_address,
  tp_site_code,
  sr_tp_id,
  `location`,
  partner_type,
  deleted_flag,
  longitude,
  latitude,
  refresh_number,
  last_update_date,
  last_updated_by,
  creation_date,
  created_by,
  last_update_login,
  request_id,
  program_application_id,
  program_id,
  program_update_date,
  attribute_category,
  attribute1,
  attribute2,
  attribute3,
  attribute4,
  attribute5,
  attribute6,
  attribute7,
  attribute9,
  attribute10,
  attribute11,
  attribute12,
  attribute13,
  attribute14,
  attribute15,
  operating_unit_name,
  country,
  state,
  city,
  postal_code,
  shipping_control,
  KCA_OPERATION,
  IS_DELETED_FLG,
  kca_seq_id,
  kca_seq_date
)
(
  SELECT
    partner_site_id,
    sr_tp_site_id,
    sr_instance_id,
    partner_id,
    partner_address,
    tp_site_code,
    sr_tp_id,
    `location`,
    partner_type,
    deleted_flag,
    longitude,
    latitude,
    refresh_number,
    last_update_date,
    last_updated_by,
    creation_date,
    created_by,
    last_update_login,
    request_id,
    program_application_id,
    program_id,
    program_update_date,
    attribute_category,
    attribute1,
    attribute2,
    attribute3,
    attribute4,
    attribute5,
    attribute6,
    attribute7,
    attribute9,
    attribute10,
    attribute11,
    attribute12,
    attribute13,
    attribute14,
    attribute15,
    operating_unit_name,
    country,
    state,
    city,
    postal_code,
    shipping_control,
    KCA_OPERATION,
    'N' AS IS_DELETED_FLG,
    CAST(NULLIF(KCA_SEQ_ID, '') AS DECIMAL(36, 0)) AS KCA_SEQ_ID,
    kca_seq_date
  FROM bronze_bec_ods_stg.MSC_TRADING_PARTNER_SITES
  WHERE
    kca_operation IN ('INSERT', 'UPDATE')
    AND (COALESCE(PARTNER_SITE_ID, 0), KCA_SEQ_ID) IN (
      SELECT
        COALESCE(PARTNER_SITE_ID, 0) AS PARTNER_SITE_ID,
        MAX(KCA_SEQ_ID)
      FROM bronze_bec_ods_stg.MSC_TRADING_PARTNER_SITES
      WHERE
        kca_operation IN ('INSERT', 'UPDATE')
      GROUP BY
        COALESCE(PARTNER_SITE_ID, 0)
    )
);
/* Soft delete */
UPDATE silver_bec_ods.MSC_TRADING_PARTNER_SITES SET IS_DELETED_FLG = 'N';
UPDATE silver_bec_ods.MSC_TRADING_PARTNER_SITES SET IS_DELETED_FLG = 'Y'
WHERE
  (
    PARTNER_SITE_ID
  ) IN (
    SELECT
      PARTNER_SITE_ID
    FROM bec_raw_dl_ext.MSC_TRADING_PARTNER_SITES
    WHERE
      (PARTNER_SITE_ID, KCA_SEQ_ID) IN (
        SELECT
          PARTNER_SITE_ID,
          MAX(KCA_SEQ_ID) AS KCA_SEQ_ID
        FROM bec_raw_dl_ext.MSC_TRADING_PARTNER_SITES
        GROUP BY
          PARTNER_SITE_ID
      )
      AND kca_operation = 'DELETE'
  );
UPDATE bec_etl_ctrl.batch_ods_info SET last_refresh_date = CURRENT_TIMESTAMP()
WHERE
  ods_table_name = 'msc_trading_partner_sites';