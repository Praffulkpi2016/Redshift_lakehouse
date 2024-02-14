TRUNCATE table bronze_bec_ods_stg.MSC_COMPANY_SITES;
INSERT INTO bronze_bec_ods_stg.MSC_COMPANY_SITES (
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
  KCA_SEQ_ID,
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
    KCA_SEQ_ID,
    kca_seq_date
  FROM bec_raw_dl_ext.MSC_COMPANY_SITES
  WHERE
    kca_operation <> 'DELETE'
    AND COALESCE(kca_seq_id, '') <> ''
    AND (COALESCE(COMPANY_ID, 0), COALESCE(COMPANY_SITE_ID, 0), KCA_SEQ_ID) IN (
      SELECT
        COALESCE(COMPANY_ID, 0) AS COMPANY_ID,
        COALESCE(COMPANY_SITE_ID, 0) AS COMPANY_SITE_ID,
        MAX(KCA_SEQ_ID)
      FROM bec_raw_dl_ext.MSC_COMPANY_SITES
      WHERE
        kca_operation <> 'DELETE' AND COALESCE(kca_seq_id, '') <> ''
      GROUP BY
        COALESCE(COMPANY_ID, 0),
        COALESCE(COMPANY_SITE_ID, 0)
    )
    AND (
      kca_seq_date > (
        SELECT
          (
            executebegints - prune_days
          )
        FROM bec_etl_ctrl.batch_ods_info
        WHERE
          ods_table_name = 'msc_company_sites'
      )
    )
);