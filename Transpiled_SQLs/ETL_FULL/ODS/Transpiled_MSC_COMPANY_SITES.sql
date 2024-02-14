DROP TABLE IF EXISTS silver_bec_ods.MSC_COMPANY_SITES;
CREATE TABLE IF NOT EXISTS silver_bec_ods.MSC_COMPANY_SITES (
  company_site_id DECIMAL(15, 0),
  company_id DECIMAL(15, 0),
  company_site_name STRING,
  sr_instance_id DECIMAL(15, 0),
  deleted_flag DECIMAL(15, 0),
  longitude DECIMAL(28, 10),
  latitude DECIMAL(28, 10),
  refresh_number DECIMAL(15, 0),
  disable_date TIMESTAMP,
  planning_enabled STRING,
  creation_date TIMESTAMP,
  created_by DECIMAL(15, 0),
  last_update_date TIMESTAMP,
  last_updated_by DECIMAL(15, 0),
  last_update_login DECIMAL(15, 0),
  attribute_category STRING,
  attribute1 STRING,
  attribute2 STRING,
  attribute3 STRING,
  attribute4 STRING,
  attribute5 STRING,
  attribute6 STRING,
  attribute7 STRING,
  attribute8 STRING,
  attribute9 STRING,
  attribute10 STRING,
  attribute11 STRING,
  attribute12 STRING,
  attribute13 STRING,
  attribute14 STRING,
  attribute15 STRING,
  request_id DECIMAL(15, 0),
  program_id DECIMAL(15, 0),
  program_update_date TIMESTAMP,
  `location` STRING,
  address1 STRING,
  address2 STRING,
  address3 STRING,
  address4 STRING,
  country STRING,
  state STRING,
  city STRING,
  county STRING,
  province STRING,
  postal_code STRING,
  KCA_OPERATION STRING,
  IS_DELETED_FLG STRING,
  kca_seq_id DECIMAL(36, 0),
  kca_seq_date TIMESTAMP
);
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
  kca_operation,
  IS_DELETED_FLG,
  kca_seq_id,
  kca_seq_date
)
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
FROM bronze_bec_ods_stg.MSC_COMPANY_SITES;
UPDATE bec_etl_ctrl.batch_ods_info SET load_type = 'I', last_refresh_date = CURRENT_TIMESTAMP()
WHERE
  ods_table_name = 'msc_company_sites';