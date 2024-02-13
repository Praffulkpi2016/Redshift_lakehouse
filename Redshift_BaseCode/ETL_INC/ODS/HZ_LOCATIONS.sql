/*
# Copyright(c) 2022 KPI Partners, Inc. All Rights Reserved.
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
# WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
#
# author: KPI Partners, Inc.
# version: 2022.06
# description: This script represents Incremental load approach for ODS.
# File Version: KPI v1.0
*/
begin;

-- Delete Records

delete from bec_ods.HZ_LOCATIONS
where LOCATION_ID in (
select stg.LOCATION_ID
from bec_ods.HZ_LOCATIONS ods, bec_ods_stg.HZ_LOCATIONS stg
where ods.LOCATION_ID = stg.LOCATION_ID
and stg.kca_operation IN ('INSERT','UPDATE')
);

commit;

-- Insert records

insert into	bec_ods.HZ_LOCATIONS
       (  location_id,
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
    GEOMETRY_ACCURACY,
    GEOMETRY_SOURCE,
	kca_operation,
	is_deleted_flg,
	kca_seq_id,
	kca_seq_date)	
(
	select
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
    GEOMETRY_ACCURACY,
    GEOMETRY_SOURCE,
        KCA_OPERATION,
       'N' AS IS_DELETED_FLG,
	    cast(NULLIF(KCA_SEQ_ID,'') as numeric(36,0)) as KCA_SEQ_ID,
	kca_seq_date
	from bec_ods_stg.HZ_LOCATIONS
	where kca_operation IN ('INSERT','UPDATE') 
	and (LOCATION_ID,kca_seq_id) in 
	(select LOCATION_ID, max(kca_seq_id) from bec_ods_stg.HZ_LOCATIONS 
     where kca_operation IN ('INSERT','UPDATE')
     group by LOCATION_ID)
);

commit;
 
-- Soft delete
update bec_ods.HZ_LOCATIONS set IS_DELETED_FLG = 'N';
commit;
update bec_ods.HZ_LOCATIONS set IS_DELETED_FLG = 'Y'
where (LOCATION_ID)  in
(
select LOCATION_ID from bec_raw_dl_ext.HZ_LOCATIONS
where (LOCATION_ID,KCA_SEQ_ID)
in 
(
select LOCATION_ID,max(KCA_SEQ_ID) as KCA_SEQ_ID 
from bec_raw_dl_ext.HZ_LOCATIONS
group by LOCATION_ID
) 
and kca_operation= 'DELETE'
);
commit;

end;

update bec_etl_ctrl.batch_ods_info
set	last_refresh_date = getdate()
where ods_table_name = 'hz_locations';

commit;