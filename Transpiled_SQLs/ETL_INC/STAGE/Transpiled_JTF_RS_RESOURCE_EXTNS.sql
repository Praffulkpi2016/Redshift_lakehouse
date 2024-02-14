TRUNCATE table bronze_bec_ods_stg.JTF_RS_RESOURCE_EXTNS;
INSERT INTO bronze_bec_ods_stg.jtf_rs_resource_extns (
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
  user_name, /*	rse_cal_resource_assign_id, */
  source_office,
  source_location,
  source_mailstop,
  source_mobile_phone,
  source_pager,
  source_job_id, /*	party_id, */
  person_party_id,
  FS_SETUP_COMPLETE,
  KCA_OPERATION,
  KCA_SEQ_ID, /* 'N' as IS_DELETED_FLG, */
  kca_seq_date
FROM bec_raw_dl_ext.jtf_rs_resource_extns
WHERE
  kca_operation <> 'DELETE'
  AND COALESCE(kca_seq_id, '') <> ''
  AND (RESOURCE_ID, kca_seq_id) IN (
    SELECT
      RESOURCE_ID,
      MAX(kca_seq_id)
    FROM bec_raw_dl_ext.jtf_rs_resource_extns
    WHERE
      kca_operation <> 'DELETE' AND COALESCE(kca_seq_id, '') <> ''
    GROUP BY
      RESOURCE_ID
  )
  AND (
    kca_seq_date > (
      SELECT
        (
          executebegints - prune_days
        )
      FROM bec_etl_ctrl.batch_ods_info
      WHERE
        ods_table_name = 'jtf_rs_resource_extns'
    )
  );