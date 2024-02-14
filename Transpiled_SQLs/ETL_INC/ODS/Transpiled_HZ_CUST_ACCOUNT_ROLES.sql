/* Delete Records */
DELETE FROM silver_bec_ods.HZ_CUST_ACCOUNT_ROLES
WHERE
  cust_account_role_id IN (
    SELECT
      stg.cust_account_role_id
    FROM silver_bec_ods.HZ_CUST_ACCOUNT_ROLES AS ods, bronze_bec_ods_stg.HZ_CUST_ACCOUNT_ROLES AS stg
    WHERE
      ods.cust_account_role_id = stg.cust_account_role_id
      AND stg.kca_operation IN ('INSERT', 'UPDATE')
  );
/* Insert records */
INSERT INTO silver_bec_ods.HZ_CUST_ACCOUNT_ROLES (
  cust_account_role_id,
  party_id,
  cust_account_id,
  current_role_state,
  current_role_state_effective,
  cust_acct_site_id,
  begin_date,
  end_date,
  primary_flag,
  role_type,
  last_update_date,
  source_code,
  last_updated_by,
  creation_date,
  created_by,
  last_update_login,
  wh_update_date,
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
  attribute8,
  attribute9,
  attribute10,
  attribute11,
  attribute12,
  attribute13,
  attribute14,
  attribute15,
  attribute16,
  attribute17,
  attribute18,
  attribute19,
  attribute20,
  attribute21,
  attribute22,
  attribute23,
  attribute24,
  global_attribute_category,
  global_attribute1,
  global_attribute2,
  global_attribute3,
  global_attribute4,
  global_attribute5,
  global_attribute6,
  global_attribute7,
  global_attribute8,
  global_attribute9,
  global_attribute10,
  global_attribute11,
  global_attribute12,
  global_attribute13,
  global_attribute14,
  global_attribute15,
  global_attribute16,
  global_attribute17,
  global_attribute18,
  global_attribute19,
  global_attribute20,
  orig_system_reference,
  attribute25,
  status,
  object_version_number,
  created_by_module,
  application_id,
  kca_operation,
  is_deleted_flg,
  kca_seq_id,
  kca_seq_date
)
(
  SELECT
    cust_account_role_id,
    party_id,
    cust_account_id,
    current_role_state,
    current_role_state_effective,
    cust_acct_site_id,
    begin_date,
    end_date,
    primary_flag,
    role_type,
    last_update_date,
    source_code,
    last_updated_by,
    creation_date,
    created_by,
    last_update_login,
    wh_update_date,
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
    attribute8,
    attribute9,
    attribute10,
    attribute11,
    attribute12,
    attribute13,
    attribute14,
    attribute15,
    attribute16,
    attribute17,
    attribute18,
    attribute19,
    attribute20,
    attribute21,
    attribute22,
    attribute23,
    attribute24,
    global_attribute_category,
    global_attribute1,
    global_attribute2,
    global_attribute3,
    global_attribute4,
    global_attribute5,
    global_attribute6,
    global_attribute7,
    global_attribute8,
    global_attribute9,
    global_attribute10,
    global_attribute11,
    global_attribute12,
    global_attribute13,
    global_attribute14,
    global_attribute15,
    global_attribute16,
    global_attribute17,
    global_attribute18,
    global_attribute19,
    global_attribute20,
    orig_system_reference,
    attribute25,
    status,
    object_version_number,
    created_by_module,
    application_id,
    kca_operation,
    'N' AS IS_DELETED_FLG,
    CAST(NULLIF(KCA_SEQ_ID, '') AS DECIMAL(36, 0)) AS KCA_SEQ_ID,
    kca_seq_date
  FROM bronze_bec_ods_stg.HZ_CUST_ACCOUNT_ROLES
  WHERE
    kca_operation IN ('INSERT', 'UPDATE')
    AND (cust_account_role_id, kca_seq_id) IN (
      SELECT
        cust_account_role_id,
        MAX(kca_seq_id)
      FROM bronze_bec_ods_stg.HZ_CUST_ACCOUNT_ROLES
      WHERE
        kca_operation IN ('INSERT', 'UPDATE')
      GROUP BY
        cust_account_role_id
    )
);
/* Soft delete */
UPDATE silver_bec_ods.HZ_CUST_ACCOUNT_ROLES SET IS_DELETED_FLG = 'N';
UPDATE silver_bec_ods.HZ_CUST_ACCOUNT_ROLES SET IS_DELETED_FLG = 'Y'
WHERE
  (
    cust_account_role_id
  ) IN (
    SELECT
      cust_account_role_id
    FROM bec_raw_dl_ext.HZ_CUST_ACCOUNT_ROLES
    WHERE
      (cust_account_role_id, KCA_SEQ_ID) IN (
        SELECT
          cust_account_role_id,
          MAX(KCA_SEQ_ID) AS KCA_SEQ_ID
        FROM bec_raw_dl_ext.HZ_CUST_ACCOUNT_ROLES
        GROUP BY
          cust_account_role_id
      )
      AND kca_operation = 'DELETE'
  );
UPDATE bec_etl_ctrl.batch_ods_info SET last_refresh_date = CURRENT_TIMESTAMP()
WHERE
  ods_table_name = 'hz_cust_account_roles';