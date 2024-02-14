DROP TABLE IF EXISTS silver_bec_ods.HZ_LOCATIONS;
CREATE TABLE IF NOT EXISTS silver_bec_ods.HZ_LOCATIONS (
  location_id DECIMAL(15, 0),
  last_update_date TIMESTAMP,
  last_updated_by DECIMAL(15, 0),
  creation_date TIMESTAMP,
  created_by DECIMAL(15, 0),
  last_update_login DECIMAL(15, 0),
  request_id DECIMAL(15, 0),
  program_application_id DECIMAL(15, 0),
  program_id DECIMAL(15, 0),
  program_update_date TIMESTAMP,
  wh_update_date TIMESTAMP,
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
  orig_system_reference STRING,
  country STRING,
  address1 STRING,
  address2 STRING,
  address3 STRING,
  address4 STRING,
  city STRING,
  postal_code STRING,
  state STRING,
  province STRING,
  county STRING,
  address_key STRING,
  address_style STRING,
  validated_flag STRING,
  address_lines_phonetic STRING,
  apartment_flag STRING,
  po_box_number STRING,
  house_number STRING,
  street_suffix STRING,
  apartment_number STRING,
  secondary_suffix_element STRING,
  street STRING,
  rural_route_type STRING,
  rural_route_number STRING,
  street_number STRING,
  building STRING,
  floor STRING,
  suite STRING,
  room STRING,
  postal_plus4_code STRING,
  time_zone STRING,
  overseas_address_flag STRING,
  post_office STRING,
  `position` STRING,
  delivery_point_code STRING,
  location_directions STRING,
  address_effective_date TIMESTAMP,
  address_expiration_date TIMESTAMP,
  address_error_code STRING,
  clli_code STRING,
  dodaac STRING,
  trailing_directory_code STRING,
  `language` STRING,
  life_cycle_status STRING,
  short_description STRING,
  description STRING,
  content_source_type STRING,
  loc_hierarchy_id DECIMAL(15, 0),
  sales_tax_geocode STRING,
  sales_tax_inside_city_limits STRING,
  fa_location_id DECIMAL(15, 0),
  object_version_number DECIMAL(15, 0),
  created_by_module STRING,
  application_id DECIMAL(15, 0),
  timezone_id DECIMAL(15, 0),
  geometry_status_code STRING,
  actual_content_source STRING,
  validation_status_code STRING,
  date_validated TIMESTAMP,
  do_not_validate_flag STRING,
  GEOMETRY_SOURCE STRING,
  GEOMETRY_ACCURACY DECIMAL(28, 10),
  KCA_OPERATION STRING,
  IS_DELETED_FLG STRING,
  kca_seq_id DECIMAL(36, 0),
  kca_seq_date TIMESTAMP
);
INSERT INTO silver_bec_ods.HZ_LOCATIONS (
  location_id,
  last_update_date,
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
  country,
  address1,
  address2,
  address3,
  address4,
  city,
  postal_code,
  state,
  province,
  county,
  address_key,
  address_style,
  validated_flag,
  address_lines_phonetic,
  apartment_flag,
  po_box_number,
  house_number,
  street_suffix,
  apartment_number,
  secondary_suffix_element,
  street,
  rural_route_type,
  rural_route_number,
  street_number,
  building,
  floor,
  suite,
  room,
  postal_plus4_code,
  time_zone,
  overseas_address_flag,
  post_office,
  `position`,
  delivery_point_code,
  location_directions,
  address_effective_date,
  address_expiration_date,
  address_error_code,
  clli_code,
  dodaac,
  trailing_directory_code,
  `language`,
  life_cycle_status,
  short_description,
  description,
  content_source_type,
  loc_hierarchy_id,
  sales_tax_geocode,
  sales_tax_inside_city_limits,
  fa_location_id,
  object_version_number,
  created_by_module,
  application_id,
  timezone_id,
  geometry_status_code,
  actual_content_source,
  validation_status_code,
  date_validated,
  do_not_validate_flag,
  GEOMETRY_SOURCE,
  GEOMETRY_ACCURACY,
  KCA_OPERATION,
  IS_DELETED_FLG,
  kca_seq_id,
  kca_seq_date
)
SELECT
  location_id,
  last_update_date,
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
  country,
  address1,
  address2,
  address3,
  address4,
  city,
  postal_code,
  state,
  province,
  county,
  address_key,
  address_style,
  validated_flag,
  address_lines_phonetic,
  apartment_flag,
  po_box_number,
  house_number,
  street_suffix,
  apartment_number,
  secondary_suffix_element,
  street,
  rural_route_type,
  rural_route_number,
  street_number,
  building,
  floor,
  suite,
  room,
  postal_plus4_code,
  time_zone,
  overseas_address_flag,
  post_office,
  `position`,
  delivery_point_code,
  location_directions,
  address_effective_date,
  address_expiration_date,
  address_error_code,
  clli_code,
  dodaac,
  trailing_directory_code,
  `language`,
  life_cycle_status,
  short_description,
  description,
  content_source_type,
  loc_hierarchy_id,
  sales_tax_geocode,
  sales_tax_inside_city_limits,
  fa_location_id,
  object_version_number,
  created_by_module,
  application_id,
  timezone_id,
  geometry_status_code,
  actual_content_source,
  validation_status_code,
  date_validated,
  do_not_validate_flag,
  GEOMETRY_SOURCE,
  GEOMETRY_ACCURACY,
  KCA_OPERATION,
  'N' AS IS_DELETED_FLG,
  CAST(NULLIF(KCA_SEQ_ID, '') AS DECIMAL(36, 0)) AS KCA_SEQ_ID,
  kca_seq_date
FROM bronze_bec_ods_stg.HZ_LOCATIONS;
UPDATE bec_etl_ctrl.batch_ods_info SET load_type = 'I', last_refresh_date = CURRENT_TIMESTAMP()
WHERE
  ods_table_name = 'hz_locations';