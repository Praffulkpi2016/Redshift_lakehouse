/* Delete Records */
DELETE FROM silver_bec_ods.HR_LOCATIONS_ALL
WHERE
  (
    LOCATION_ID
  ) IN (
    SELECT
      stg.LOCATION_ID
    FROM silver_bec_ods.HR_LOCATIONS_ALL AS ods, bronze_bec_ods_stg.HR_LOCATIONS_ALL AS stg
    WHERE
      ods.LOCATION_ID = stg.LOCATION_ID AND stg.kca_operation IN ('INSERT', 'UPDATE')
  );
/* Insert records */
INSERT INTO silver_bec_ods.HR_LOCATIONS_ALL (
  location_id,
  entered_by,
  location_code,
  address_line_1,
  address_line_2,
  address_line_3,
  bill_to_site_flag,
  country,
  description,
  designated_receiver_id,
  in_organization_flag,
  inactive_date,
  inventory_organization_id,
  office_site_flag,
  postal_code,
  receiving_site_flag,
  region_1,
  region_2,
  region_3,
  ship_to_location_id,
  ship_to_site_flag,
  `style`,
  tax_name,
  telephone_number_1,
  telephone_number_2,
  telephone_number_3,
  town_or_city,
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
  last_update_date,
  last_updated_by,
  last_update_login,
  created_by,
  creation_date,
  object_version_number,
  tp_header_id,
  ece_tp_location_code,
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
  business_group_id,
  loc_information13,
  loc_information14,
  loc_information15,
  loc_information16,
  loc_information17,
  loc_information18,
  loc_information19,
  loc_information20,
  derived_locale,
  legal_address_flag,
  timezone_code,
  `TAX_NAME#1`,
  kca_operation,
  is_deleted_flg,
  kca_seq_id,
  kca_seq_date
)
(
  SELECT
    location_id,
    entered_by,
    location_code,
    address_line_1,
    address_line_2,
    address_line_3,
    bill_to_site_flag,
    country,
    description,
    designated_receiver_id,
    in_organization_flag,
    inactive_date,
    inventory_organization_id,
    office_site_flag,
    postal_code,
    receiving_site_flag,
    region_1,
    region_2,
    region_3,
    ship_to_location_id,
    ship_to_site_flag,
    `style`,
    tax_name,
    telephone_number_1,
    telephone_number_2,
    telephone_number_3,
    town_or_city,
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
    last_update_date,
    last_updated_by,
    last_update_login,
    created_by,
    creation_date,
    object_version_number,
    tp_header_id,
    ece_tp_location_code,
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
    business_group_id,
    loc_information13,
    loc_information14,
    loc_information15,
    loc_information16,
    loc_information17,
    loc_information18,
    loc_information19,
    loc_information20,
    derived_locale,
    legal_address_flag,
    timezone_code,
    `TAX_NAME#1`,
    KCA_OPERATION,
    'N' AS IS_DELETED_FLG,
    CAST(NULLIF(KCA_SEQ_ID, '') AS DECIMAL(36, 0)) AS KCA_SEQ_ID,
    kca_seq_date
  FROM bronze_bec_ods_stg.HR_LOCATIONS_ALL
  WHERE
    kca_operation IN ('INSERT', 'UPDATE')
    AND (LOCATION_ID, kca_seq_id) IN (
      SELECT
        LOCATION_ID,
        MAX(kca_seq_id)
      FROM bronze_bec_ods_stg.HR_LOCATIONS_ALL
      WHERE
        kca_operation IN ('INSERT', 'UPDATE')
      GROUP BY
        LOCATION_ID
    )
);
/* Soft delete */
UPDATE silver_bec_ods.HR_LOCATIONS_ALL SET IS_DELETED_FLG = 'N';
UPDATE silver_bec_ods.HR_LOCATIONS_ALL SET IS_DELETED_FLG = 'Y'
WHERE
  (
    LOCATION_ID
  ) IN (
    SELECT
      LOCATION_ID
    FROM bec_raw_dl_ext.HR_LOCATIONS_ALL
    WHERE
      (LOCATION_ID, KCA_SEQ_ID) IN (
        SELECT
          LOCATION_ID,
          MAX(KCA_SEQ_ID) AS KCA_SEQ_ID
        FROM bec_raw_dl_ext.HR_LOCATIONS_ALL
        GROUP BY
          LOCATION_ID
      )
      AND kca_operation = 'DELETE'
  );
UPDATE bec_etl_ctrl.batch_ods_info SET last_refresh_date = CURRENT_TIMESTAMP()
WHERE
  ods_table_name = 'hr_locations_all';