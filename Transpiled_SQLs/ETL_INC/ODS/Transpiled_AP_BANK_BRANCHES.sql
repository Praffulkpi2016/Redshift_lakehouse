/* Delete Records */
DELETE FROM silver_bec_ods.ap_bank_branches
WHERE
  bank_branch_id IN (
    SELECT
      stg.bank_branch_id
    FROM silver_bec_ods.ap_bank_branches AS ods, bronze_bec_ods_stg.ap_bank_branches AS stg
    WHERE
      ods.bank_branch_id = stg.bank_branch_id AND stg.kca_operation IN ('INSERT', 'UPDATE')
  );
/* Insert records */
INSERT INTO silver_bec_ods.ap_bank_branches (
  bank_branch_id,
  last_update_date,
  last_updated_by,
  bank_name,
  bank_branch_name,
  description,
  address_line1,
  address_line2,
  address_line3,
  city,
  state,
  zip,
  province,
  country,
  area_code,
  phone,
  contact_first_name,
  contact_middle_name,
  contact_last_name,
  contact_prefix,
  contact_title,
  bank_num,
  last_update_login,
  creation_date,
  created_by,
  institution_type,
  clearing_house_id,
  transmission_program_id,
  printing_program_id,
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
  program_application_id,
  program_id,
  program_update_date,
  address_style,
  bank_number,
  address_line4,
  county,
  eft_user_number,
  eft_swift_code,
  end_date,
  edi_id_number,
  bank_branch_type,
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
  bank_name_alt,
  bank_branch_name_alt,
  address_lines_alt,
  active_date,
  tp_header_id,
  ece_tp_location_code,
  payroll_bank_account_id,
  rfc_identifier,
  bank_admin_email,
  KCA_OPERATION,
  IS_DELETED_FLG,
  kca_seq_id,
  kca_seq_date
)
(
  SELECT
    bank_branch_id,
    last_update_date,
    last_updated_by,
    bank_name,
    bank_branch_name,
    description,
    address_line1,
    address_line2,
    address_line3,
    city,
    state,
    zip,
    province,
    country,
    area_code,
    phone,
    contact_first_name,
    contact_middle_name,
    contact_last_name,
    contact_prefix,
    contact_title,
    bank_num,
    last_update_login,
    creation_date,
    created_by,
    institution_type,
    clearing_house_id,
    transmission_program_id,
    printing_program_id,
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
    program_application_id,
    program_id,
    program_update_date,
    address_style,
    bank_number,
    address_line4,
    county,
    eft_user_number,
    eft_swift_code,
    end_date,
    edi_id_number,
    bank_branch_type,
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
    bank_name_alt,
    bank_branch_name_alt,
    address_lines_alt,
    active_date,
    tp_header_id,
    ece_tp_location_code,
    payroll_bank_account_id,
    rfc_identifier,
    bank_admin_email,
    KCA_OPERATION,
    'N' AS IS_DELETED_FLG,
    CAST(NULLIF(KCA_SEQ_ID, '') AS DECIMAL(36, 0)) AS KCA_SEQ_ID,
    kca_seq_date
  FROM bronze_bec_ods_stg.ap_bank_branches
  WHERE
    kca_operation IN ('INSERT', 'UPDATE')
    AND (bank_branch_id, kca_seq_id) IN (
      SELECT
        bank_branch_id,
        MAX(kca_seq_id)
      FROM bronze_bec_ods_stg.ap_bank_branches
      WHERE
        kca_operation IN ('INSERT', 'UPDATE')
      GROUP BY
        bank_branch_id
    )
);
/* Soft delete */
UPDATE silver_bec_ods.ap_bank_branches SET IS_DELETED_FLG = 'N';
UPDATE silver_bec_ods.ap_bank_branches SET IS_DELETED_FLG = 'Y'
WHERE
  (
    bank_branch_id
  ) IN (
    SELECT
      bank_branch_id
    FROM bec_raw_dl_ext.ap_bank_branches
    WHERE
      (bank_branch_id, KCA_SEQ_ID) IN (
        SELECT
          bank_branch_id,
          MAX(KCA_SEQ_ID) AS KCA_SEQ_ID
        FROM bec_raw_dl_ext.ap_bank_branches
        GROUP BY
          bank_branch_id
      )
      AND kca_operation = 'DELETE'
  );
UPDATE bec_etl_ctrl.batch_ods_info SET last_refresh_date = CURRENT_TIMESTAMP()
WHERE
  ods_table_name = 'ap_bank_branches';