/* Delete Records */
DELETE FROM silver_bec_ods.RA_CONTACTS
WHERE
  CONTACT_ID IN (
    SELECT
      stg.CONTACT_ID
    FROM silver_bec_ods.RA_CONTACTS AS ods, bronze_bec_ods_stg.RA_CONTACTS AS stg
    WHERE
      ods.CONTACT_ID = stg.CONTACT_ID AND stg.kca_operation IN ('INSERT', 'UPDATE')
  );
/* Insert records */
INSERT INTO silver_bec_ods.ra_contacts (
  contact_id,
  last_update_date,
  last_updated_by,
  creation_date,
  created_by,
  customer_id,
  status,
  orig_system_reference,
  last_name,
  last_update_login,
  title,
  first_name,
  job_title,
  mail_stop,
  address_id,
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
  request_id,
  program_application_id,
  program_id,
  program_update_date,
  contact_key,
  contact_personal_information,
  decision_maker_flag,
  job_title_code,
  managed_by,
  native_language,
  reference_use_flag,
  contact_number,
  attribute11,
  attribute25,
  other_language_1,
  other_language_2,
  `rank`,
  primary_role,
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
  do_not_mail_flag,
  suffix,
  email_address,
  mailing_address_id,
  match_group_id,
  sex_code,
  salutation,
  department_code,
  department,
  last_name_alt,
  first_name_alt,
  do_not_email_flag,
  kca_operation,
  is_deleted_flg,
  kca_seq_id,
  kca_seq_date
)
SELECT
  contact_id,
  last_update_date,
  last_updated_by,
  creation_date,
  created_by,
  customer_id,
  status,
  orig_system_reference,
  last_name,
  last_update_login,
  title,
  first_name,
  job_title,
  mail_stop,
  address_id,
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
  request_id,
  program_application_id,
  program_id,
  program_update_date,
  contact_key,
  contact_personal_information,
  decision_maker_flag,
  job_title_code,
  managed_by,
  native_language,
  reference_use_flag,
  contact_number,
  attribute11,
  attribute25,
  other_language_1,
  other_language_2,
  `rank`,
  primary_role,
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
  do_not_mail_flag,
  suffix,
  email_address,
  mailing_address_id,
  match_group_id,
  sex_code,
  salutation,
  department_code,
  department,
  last_name_alt,
  first_name_alt,
  do_not_email_flag,
  KCA_OPERATION,
  'N' AS IS_DELETED_FLG,
  CAST(NULLIF(KCA_SEQ_ID, '') AS DECIMAL(36, 0)) AS KCA_SEQ_ID,
  KCA_SEQ_DATE
FROM bronze_bec_ods_stg.ra_contacts
WHERE
  kca_operation IN ('INSERT', 'UPDATE')
  AND (CONTACT_ID, kca_seq_id) IN (
    SELECT
      CONTACT_ID,
      MAX(kca_seq_id)
    FROM bronze_bec_ods_stg.RA_CONTACTS
    WHERE
      kca_operation IN ('INSERT', 'UPDATE')
    GROUP BY
      CONTACT_ID
  );
/* Soft delete */
UPDATE silver_bec_ods.RA_CONTACTS SET IS_DELETED_FLG = 'N';
UPDATE silver_bec_ods.RA_CONTACTS SET IS_DELETED_FLG = 'Y'
WHERE
  (
    CONTACT_ID
  ) IN (
    SELECT
      CONTACT_ID
    FROM bec_raw_dl_ext.RA_CONTACTS
    WHERE
      (CONTACT_ID, KCA_SEQ_ID) IN (
        SELECT
          CONTACT_ID,
          MAX(KCA_SEQ_ID) AS KCA_SEQ_ID
        FROM bec_raw_dl_ext.RA_CONTACTS
        GROUP BY
          CONTACT_ID
      )
      AND kca_operation = 'DELETE'
  );
UPDATE bec_etl_ctrl.batch_ods_info SET last_refresh_date = CURRENT_TIMESTAMP()
WHERE
  ods_table_name = 'RA_CONTACTS';