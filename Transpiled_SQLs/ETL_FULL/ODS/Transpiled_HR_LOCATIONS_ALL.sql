DROP TABLE IF EXISTS silver_bec_ods.HR_LOCATIONS_ALL;
CREATE TABLE IF NOT EXISTS silver_bec_ods.HR_LOCATIONS_ALL (
  location_id DECIMAL(15, 0),
  entered_by DECIMAL(15, 0),
  location_code STRING,
  address_line_1 STRING,
  address_line_2 STRING,
  address_line_3 STRING,
  bill_to_site_flag STRING,
  country STRING,
  description STRING,
  designated_receiver_id DECIMAL(15, 0),
  in_organization_flag STRING,
  inactive_date TIMESTAMP,
  inventory_organization_id DECIMAL(15, 0),
  office_site_flag STRING,
  postal_code STRING,
  receiving_site_flag STRING,
  region_1 STRING,
  region_2 STRING,
  region_3 STRING,
  ship_to_location_id DECIMAL(15, 0),
  ship_to_site_flag STRING,
  style STRING,
  tax_name STRING,
  telephone_number_1 STRING,
  telephone_number_2 STRING,
  telephone_number_3 STRING,
  town_or_city STRING,
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
  attribute16 STRING,
  attribute17 STRING,
  attribute18 STRING,
  attribute19 STRING,
  attribute20 STRING,
  last_update_date TIMESTAMP,
  last_updated_by DECIMAL(15, 0),
  last_update_login DECIMAL(15, 0),
  created_by DECIMAL(15, 0),
  creation_date TIMESTAMP,
  object_version_number DECIMAL(9, 0),
  tp_header_id DECIMAL(15, 0),
  ece_tp_location_code STRING,
  global_attribute_category STRING,
  global_attribute1 STRING,
  global_attribute2 STRING,
  global_attribute3 STRING,
  global_attribute4 STRING,
  global_attribute5 STRING,
  global_attribute6 STRING,
  global_attribute7 STRING,
  global_attribute8 STRING,
  global_attribute9 STRING,
  global_attribute10 STRING,
  global_attribute11 STRING,
  global_attribute12 STRING,
  global_attribute13 STRING,
  global_attribute14 STRING,
  global_attribute15 STRING,
  global_attribute16 STRING,
  global_attribute17 STRING,
  global_attribute18 STRING,
  global_attribute19 STRING,
  global_attribute20 STRING,
  business_group_id DECIMAL(15, 0),
  loc_information13 STRING,
  loc_information14 STRING,
  loc_information15 STRING,
  loc_information16 STRING,
  loc_information17 STRING,
  loc_information18 STRING,
  loc_information19 STRING,
  loc_information20 STRING,
  derived_locale STRING,
  legal_address_flag STRING,
  timezone_code STRING,
  `TAX_NAME#1` STRING,
  kca_operation STRING,
  is_deleted_flg STRING,
  kca_seq_id DECIMAL(36, 0),
  kca_seq_date TIMESTAMP
);
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
  IS_DELETED_FLG,
  kca_seq_id,
  kca_seq_date
)
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
  kca_operation,
  'N' AS IS_DELETED_FLG,
  CAST(NULLIF(KCA_SEQ_ID, '') AS DECIMAL(36, 0)) AS KCA_SEQ_ID,
  kca_seq_date
FROM bronze_bec_ods_stg.HR_LOCATIONS_ALL;
UPDATE bec_etl_ctrl.batch_ods_info SET load_type = 'I', last_refresh_date = CURRENT_TIMESTAMP()
WHERE
  ods_table_name = 'hr_locations_all';