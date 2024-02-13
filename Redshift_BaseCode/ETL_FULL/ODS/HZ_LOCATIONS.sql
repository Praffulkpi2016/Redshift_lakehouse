/*
# Copyright(c) 2022 KPI Partners, Inc. All Rights Reserved.
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
# WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
#
# author: KPI Partners, Inc.
# version: 2022.06
# description: This script represents full load approach for ODS.
# File Version: KPI v1.0
*/

begin;

DROP TABLE if exists bec_ods.HZ_LOCATIONS;

CREATE TABLE IF NOT EXISTS bec_ods.HZ_LOCATIONS
(
	location_id NUMERIC(15,0)   ENCODE az64
	,last_update_date TIMESTAMP WITHOUT TIME ZONE   ENCODE az64
	,last_updated_by NUMERIC(15,0)   ENCODE az64
	,creation_date TIMESTAMP WITHOUT TIME ZONE   ENCODE az64
	,created_by NUMERIC(15,0)   ENCODE az64
	,last_update_login NUMERIC(15,0)   ENCODE az64
	,request_id NUMERIC(15,0)   ENCODE az64
	,program_application_id NUMERIC(15,0)   ENCODE az64
	,program_id NUMERIC(15,0)   ENCODE az64
	,program_update_date TIMESTAMP WITHOUT TIME ZONE   ENCODE az64
	,wh_update_date TIMESTAMP WITHOUT TIME ZONE   ENCODE az64
	,attribute_category VARCHAR(30)   ENCODE lzo
	,attribute1 VARCHAR(150)   ENCODE lzo
	,attribute2 VARCHAR(150)   ENCODE lzo
	,attribute3 VARCHAR(150)   ENCODE lzo
	,attribute4 VARCHAR(150)   ENCODE lzo
	,attribute5 VARCHAR(150)   ENCODE lzo
	,attribute6 VARCHAR(150)   ENCODE lzo
	,attribute7 VARCHAR(150)   ENCODE lzo
	,attribute8 VARCHAR(150)   ENCODE lzo
	,attribute9 VARCHAR(150)   ENCODE lzo
	,attribute10 VARCHAR(150)   ENCODE lzo
	,attribute11 VARCHAR(150)   ENCODE lzo
	,attribute12 VARCHAR(150)   ENCODE lzo
	,attribute13 VARCHAR(150)   ENCODE lzo
	,attribute14 VARCHAR(150)   ENCODE lzo
	,attribute15 VARCHAR(150)   ENCODE lzo
	,attribute16 VARCHAR(150)   ENCODE lzo
	,attribute17 VARCHAR(150)   ENCODE lzo
	,attribute18 VARCHAR(150)   ENCODE lzo
	,attribute19 VARCHAR(150)   ENCODE lzo
	,attribute20 VARCHAR(150)   ENCODE lzo
	,global_attribute_category VARCHAR(30)   ENCODE lzo
	,global_attribute1 VARCHAR(150)   ENCODE lzo
	,global_attribute2 VARCHAR(150)   ENCODE lzo
	,global_attribute3 VARCHAR(150)   ENCODE lzo
	,global_attribute4 VARCHAR(150)   ENCODE lzo
	,global_attribute5 VARCHAR(150)   ENCODE lzo
	,global_attribute6 VARCHAR(150)   ENCODE lzo
	,global_attribute7 VARCHAR(150)   ENCODE lzo
	,global_attribute8 VARCHAR(150)   ENCODE lzo
	,global_attribute9 VARCHAR(150)   ENCODE lzo
	,global_attribute10 VARCHAR(150)   ENCODE lzo
	,global_attribute11 VARCHAR(150)   ENCODE lzo
	,global_attribute12 VARCHAR(150)   ENCODE lzo
	,global_attribute13 VARCHAR(150)   ENCODE lzo
	,global_attribute14 VARCHAR(150)   ENCODE lzo
	,global_attribute15 VARCHAR(150)   ENCODE lzo
	,global_attribute16 VARCHAR(150)   ENCODE lzo
	,global_attribute17 VARCHAR(150)   ENCODE lzo
	,global_attribute18 VARCHAR(150)   ENCODE lzo
	,global_attribute19 VARCHAR(150)   ENCODE lzo
	,global_attribute20 VARCHAR(150)   ENCODE lzo
	,orig_system_reference VARCHAR(240)   ENCODE lzo
	,country VARCHAR(60)   ENCODE lzo
	,address1 VARCHAR(240)   ENCODE lzo
	,address2 VARCHAR(240)   ENCODE lzo
	,address3 VARCHAR(240)   ENCODE lzo
	,address4 VARCHAR(240)   ENCODE lzo
	,city VARCHAR(60)   ENCODE lzo
	,postal_code VARCHAR(60)   ENCODE lzo
	,state VARCHAR(60)   ENCODE lzo
	,province VARCHAR(60)   ENCODE lzo
	,county VARCHAR(60)   ENCODE lzo
	,address_key VARCHAR(500)   ENCODE lzo
	,address_style VARCHAR(30)   ENCODE lzo
	,validated_flag VARCHAR(1)   ENCODE lzo
	,address_lines_phonetic VARCHAR(560)   ENCODE lzo
	,apartment_flag VARCHAR(1)   ENCODE lzo
	,po_box_number VARCHAR(50)   ENCODE lzo
	,house_number VARCHAR(50)   ENCODE lzo
	,street_suffix VARCHAR(50)   ENCODE lzo
	,apartment_number VARCHAR(50)   ENCODE lzo
	,secondary_suffix_element VARCHAR(240)   ENCODE lzo
	,street VARCHAR(50)   ENCODE lzo
	,rural_route_type VARCHAR(50)   ENCODE lzo
	,rural_route_number VARCHAR(50)   ENCODE lzo
	,street_number VARCHAR(50)   ENCODE lzo
	,building VARCHAR(50)   ENCODE lzo
	,floor VARCHAR(50)   ENCODE lzo
	,suite VARCHAR(50)   ENCODE lzo
	,room VARCHAR(50)   ENCODE lzo
	,postal_plus4_code VARCHAR(10)   ENCODE lzo
	,time_zone VARCHAR(50)   ENCODE lzo
	,overseas_address_flag VARCHAR(1)   ENCODE lzo
	,post_office VARCHAR(50)   ENCODE lzo
	,"position" VARCHAR(50)   ENCODE lzo
	,delivery_point_code VARCHAR(50)   ENCODE lzo
	,location_directions VARCHAR(640)   ENCODE lzo
	,address_effective_date TIMESTAMP WITHOUT TIME ZONE   ENCODE az64
	,address_expiration_date TIMESTAMP WITHOUT TIME ZONE   ENCODE az64
	,address_error_code VARCHAR(50)   ENCODE lzo
	,clli_code VARCHAR(60)   ENCODE lzo
	,dodaac VARCHAR(6)   ENCODE lzo
	,trailing_directory_code VARCHAR(60)   ENCODE lzo
	,"language" VARCHAR(4)   ENCODE lzo
	,life_cycle_status VARCHAR(30)   ENCODE lzo
	,short_description VARCHAR(240)   ENCODE lzo
	,description VARCHAR(2000)   ENCODE lzo
	,content_source_type VARCHAR(30)   ENCODE lzo
	,loc_hierarchy_id NUMERIC(15,0)   ENCODE az64
	,sales_tax_geocode VARCHAR(30)   ENCODE lzo
	,sales_tax_inside_city_limits VARCHAR(30)   ENCODE lzo
	,fa_location_id NUMERIC(15,0)   ENCODE az64
	,object_version_number NUMERIC(15,0)   ENCODE az64
	,created_by_module VARCHAR(150)   ENCODE lzo
	,application_id NUMERIC(15,0)   ENCODE az64
	,timezone_id NUMERIC(15,0)   ENCODE az64
	,geometry_status_code VARCHAR(30)   ENCODE lzo
	,actual_content_source VARCHAR(30)   ENCODE lzo
	,validation_status_code VARCHAR(30)   ENCODE lzo
	,date_validated TIMESTAMP WITHOUT TIME ZONE   ENCODE az64
	,do_not_validate_flag VARCHAR(1)   ENCODE lzo
	,GEOMETRY_SOURCE VARCHAR(30) ENCODE lzo
    ,GEOMETRY_ACCURACY NUMERIC(28,10)   ENCODE az64
	,KCA_OPERATION VARCHAR(10)   ENCODE lzo
    ,IS_DELETED_FLG VARCHAR(2) ENCODE lzo
	,kca_seq_id NUMERIC(36,0)   ENCODE az64
	,kca_seq_date TIMESTAMP WITHOUT TIME ZONE   ENCODE az64
)
DISTSTYLE
auto;

INSERT INTO bec_ods.HZ_LOCATIONS (
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
	"position",
	delivery_point_code,
	location_directions,
	address_effective_date,
	address_expiration_date,
	address_error_code,
	clli_code,
	dodaac,
	trailing_directory_code,
	"language",
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
	"position",
	delivery_point_code,
	location_directions,
	address_effective_date,
	address_expiration_date,
	address_error_code,
	clli_code,
	dodaac,
	trailing_directory_code,
	"language",
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
		'N' as IS_DELETED_FLG,
		cast(NULLIF(KCA_SEQ_ID,'') as numeric(36,0)) as KCA_SEQ_ID,
	kca_seq_date
    FROM
        bec_ods_stg.HZ_LOCATIONS;

end;		

UPDATE bec_etl_ctrl.batch_ods_info
SET
    load_type = 'I',
    last_refresh_date = getdate()
WHERE
    ods_table_name = 'hz_locations';
	
commit;