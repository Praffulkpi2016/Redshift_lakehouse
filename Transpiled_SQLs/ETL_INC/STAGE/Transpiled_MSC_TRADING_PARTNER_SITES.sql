TRUNCATE table bronze_bec_ods_stg.MSC_TRADING_PARTNER_SITES;
INSERT INTO bronze_bec_ods_stg.MSC_TRADING_PARTNER_SITES (
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
  KCA_SEQ_ID,
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
    KCA_SEQ_ID,
    kca_seq_date
  FROM bec_raw_dl_ext.MSC_TRADING_PARTNER_SITES
  WHERE
    kca_operation <> 'DELETE'
    AND COALESCE(kca_seq_id, '') <> ''
    AND (COALESCE(PARTNER_SITE_ID, 0), KCA_SEQ_ID) IN (
      SELECT
        COALESCE(PARTNER_SITE_ID, 0) AS PARTNER_SITE_ID,
        MAX(KCA_SEQ_ID)
      FROM bec_raw_dl_ext.MSC_TRADING_PARTNER_SITES
      WHERE
        kca_operation <> 'DELETE' AND COALESCE(kca_seq_id, '') <> ''
      GROUP BY
        COALESCE(PARTNER_SITE_ID, 0)
    )
    AND (
      kca_seq_date > (
        SELECT
          (
            executebegints - prune_days
          )
        FROM bec_etl_ctrl.batch_ods_info
        WHERE
          ods_table_name = 'msc_trading_partner_sites'
      )
    )
);