/* Delete Records */
DELETE FROM silver_bec_ods.HZ_PARTY_SITES
WHERE
  party_site_id IN (
    SELECT
      stg.party_site_id
    FROM silver_bec_ods.HZ_PARTY_SITES AS ods, bronze_bec_ods_stg.HZ_PARTY_SITES AS stg
    WHERE
      ods.party_site_id = stg.party_site_id AND stg.kca_operation IN ('INSERT', 'UPDATE')
  );
/* Insert records */
INSERT INTO silver_bec_ods.HZ_PARTY_SITES (
  party_site_id,
  party_id,
  location_id,
  last_update_date,
  party_site_number,
  last_updated_by,
  creation_date,
  created_by,
  last_update_login,
  request_id,
  program_application_id,
  program_id,
  program_update_date,
  wh_update_date,
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
  start_date_active,
  end_date_active,
  region,
  mailstop,
  customer_key_osm,
  phone_key_osm,
  contact_key_osm,
  identifying_address_flag,
  `language`,
  status,
  party_site_name,
  addressee,
  object_version_number,
  created_by_module,
  application_id,
  actual_content_source,
  global_location_number,
  duns_number_c,
  kca_operation,
  is_deleted_flg,
  kca_seq_id,
  kca_seq_date
)
(
  SELECT
    party_site_id,
    party_id,
    location_id,
    last_update_date,
    party_site_number,
    last_updated_by,
    creation_date,
    created_by,
    last_update_login,
    request_id,
    program_application_id,
    program_id,
    program_update_date,
    wh_update_date,
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
    start_date_active,
    end_date_active,
    region,
    mailstop,
    customer_key_osm,
    phone_key_osm,
    contact_key_osm,
    identifying_address_flag,
    `language`,
    status,
    party_site_name,
    addressee,
    object_version_number,
    created_by_module,
    application_id,
    actual_content_source,
    global_location_number,
    duns_number_c,
    KCA_OPERATION,
    'N' AS IS_DELETED_FLG,
    CAST(NULLIF(KCA_SEQ_ID, '') AS DECIMAL(36, 0)) AS KCA_SEQ_ID,
    kca_seq_date
  FROM bronze_bec_ods_stg.HZ_PARTY_SITES
  WHERE
    kca_operation IN ('INSERT', 'UPDATE')
    AND (party_site_id, kca_seq_id) IN (
      SELECT
        party_site_id,
        MAX(kca_seq_id)
      FROM bronze_bec_ods_stg.HZ_PARTY_SITES
      WHERE
        kca_operation IN ('INSERT', 'UPDATE')
      GROUP BY
        party_site_id
    )
);
/* Soft delete */
UPDATE silver_bec_ods.HZ_PARTY_SITES SET IS_DELETED_FLG = 'N';
UPDATE silver_bec_ods.HZ_PARTY_SITES SET IS_DELETED_FLG = 'Y'
WHERE
  (
    party_site_id
  ) IN (
    SELECT
      party_site_id
    FROM bec_raw_dl_ext.HZ_PARTY_SITES
    WHERE
      (party_site_id, KCA_SEQ_ID) IN (
        SELECT
          party_site_id,
          MAX(KCA_SEQ_ID) AS KCA_SEQ_ID
        FROM bec_raw_dl_ext.HZ_PARTY_SITES
        GROUP BY
          party_site_id
      )
      AND kca_operation = 'DELETE'
  );
UPDATE bec_etl_ctrl.batch_ods_info SET last_refresh_date = CURRENT_TIMESTAMP()
WHERE
  ods_table_name = 'hz_party_sites';