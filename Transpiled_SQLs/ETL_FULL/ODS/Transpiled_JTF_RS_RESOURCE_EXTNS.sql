DROP TABLE IF EXISTS silver_bec_ods.jtf_rs_resource_extns;
CREATE TABLE IF NOT EXISTS silver_bec_ods.jtf_rs_resource_extns (
  resource_id DECIMAL(15, 0),
  created_by DECIMAL(15, 0),
  creation_date TIMESTAMP,
  last_updated_by DECIMAL(15, 0),
  last_update_date TIMESTAMP,
  last_update_login DECIMAL(15, 0),
  category STRING,
  resource_number STRING,
  source_id DECIMAL(15, 0),
  address_id DECIMAL(15, 0),
  contact_id DECIMAL(15, 0),
  managing_employee_id DECIMAL(15, 0),
  start_date_active TIMESTAMP,
  end_date_active TIMESTAMP,
  time_zone DECIMAL(15, 0),
  cost_per_hr DECIMAL(28, 10),
  primary_language STRING,
  secondary_language STRING,
  ies_agent_login STRING,
  server_group_id DECIMAL(15, 0),
  assigned_to_group_id DECIMAL(15, 0),
  cost_center STRING,
  charge_to_cost_center STRING,
  compensation_currency_code STRING,
  commissionable_flag STRING,
  hold_reason_code STRING,
  hold_payment STRING,
  comp_service_team_id DECIMAL(15, 0),
  transaction_number DECIMAL(15, 0),
  object_version_number DECIMAL(15, 0),
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
  attribute_category STRING,
  user_id DECIMAL(15, 0),
  support_site_id DECIMAL(15, 0),
  security_group_id DECIMAL(15, 0),
  support_site STRING,
  source_name STRING,
  source_number STRING,
  source_job_title STRING,
  source_email STRING,
  source_phone STRING,
  source_org_id DECIMAL(15, 0),
  source_org_name STRING,
  source_address1 STRING,
  source_address2 STRING,
  source_address3 STRING,
  source_address4 STRING,
  source_city STRING,
  source_postal_code STRING,
  source_state STRING,
  source_province STRING,
  source_county STRING,
  source_country STRING,
  source_mgr_id DECIMAL(15, 0),
  source_mgr_name STRING,
  source_business_grp_id DECIMAL(15, 0),
  source_business_grp_name STRING,
  source_first_name STRING,
  source_middle_name STRING,
  source_last_name STRING,
  source_category STRING,
  source_status STRING,
  user_name STRING,
  source_office STRING,
  source_location STRING,
  source_mailstop STRING,
  source_mobile_phone STRING,
  source_pager STRING,
  source_job_id DECIMAL(15, 0),
  person_party_id DECIMAL(15, 0),
  FS_SETUP_COMPLETE STRING,
  KCA_OPERATION STRING,
  IS_DELETED_FLG STRING,
  kca_seq_id DECIMAL(36, 0),
  kca_seq_date TIMESTAMP
);
INSERT INTO silver_bec_ods.jtf_rs_resource_extns (
  resource_id,
  created_by,
  creation_date,
  last_updated_by,
  last_update_date,
  last_update_login,
  category,
  resource_number,
  source_id,
  address_id,
  contact_id,
  managing_employee_id,
  start_date_active,
  end_date_active,
  time_zone,
  cost_per_hr,
  primary_language,
  secondary_language,
  ies_agent_login,
  server_group_id,
  assigned_to_group_id,
  cost_center,
  charge_to_cost_center,
  compensation_currency_code,
  commissionable_flag,
  hold_reason_code,
  hold_payment,
  comp_service_team_id,
  transaction_number,
  object_version_number,
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
  attribute_category,
  user_id,
  support_site_id,
  security_group_id,
  support_site,
  source_name,
  source_number,
  source_job_title,
  source_email,
  source_phone,
  source_org_id,
  source_org_name,
  source_address1,
  source_address2,
  source_address3,
  source_address4,
  source_city,
  source_postal_code,
  source_state,
  source_province,
  source_county,
  source_country,
  source_mgr_id,
  source_mgr_name,
  source_business_grp_id,
  source_business_grp_name,
  source_first_name,
  source_middle_name,
  source_last_name,
  source_category,
  source_status,
  user_name,
  source_office,
  source_location,
  source_mailstop,
  source_mobile_phone,
  source_pager,
  source_job_id,
  person_party_id,
  FS_SETUP_COMPLETE,
  kca_operation,
  is_deleted_flg,
  kca_seq_id,
  kca_seq_date
)
SELECT
  resource_id,
  created_by,
  creation_date,
  last_updated_by,
  last_update_date,
  last_update_login,
  category,
  resource_number,
  source_id,
  address_id,
  contact_id,
  managing_employee_id,
  start_date_active,
  end_date_active,
  time_zone,
  cost_per_hr,
  primary_language,
  secondary_language,
  ies_agent_login,
  server_group_id,
  assigned_to_group_id,
  cost_center,
  charge_to_cost_center,
  compensation_currency_code,
  commissionable_flag,
  hold_reason_code,
  hold_payment,
  comp_service_team_id,
  transaction_number,
  object_version_number,
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
  attribute_category,
  user_id,
  support_site_id,
  security_group_id,
  support_site,
  source_name,
  source_number,
  source_job_title,
  source_email,
  source_phone,
  source_org_id,
  source_org_name,
  source_address1,
  source_address2,
  source_address3,
  source_address4,
  source_city,
  source_postal_code,
  source_state,
  source_province,
  source_county,
  source_country,
  source_mgr_id,
  source_mgr_name,
  source_business_grp_id,
  source_business_grp_name,
  source_first_name,
  source_middle_name,
  source_last_name,
  source_category,
  source_status,
  user_name,
  source_office,
  source_location,
  source_mailstop,
  source_mobile_phone,
  source_pager,
  source_job_id,
  person_party_id,
  FS_SETUP_COMPLETE,
  KCA_OPERATION,
  'N' AS IS_DELETED_FLG,
  CAST(NULLIF(KCA_SEQ_ID, '') AS DECIMAL(36, 0)) AS KCA_SEQ_ID,
  kca_seq_date
FROM bronze_bec_ods_stg.jtf_rs_resource_extns;
UPDATE bec_etl_ctrl.batch_ods_info SET load_type = 'I', last_refresh_date = CURRENT_TIMESTAMP()
WHERE
  ods_table_name = 'jtf_rs_resource_extns';