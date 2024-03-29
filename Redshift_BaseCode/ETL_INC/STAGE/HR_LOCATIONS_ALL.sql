/*
# Copyright(c) 2022 KPI Partners, Inc. All Rights Reserved.
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
# WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
#
# author: KPI Partners, Inc.
# version: 2022.06
# description: This script represents Incremental load approach for stage.
# File Version: KPI v1.0
*/
begin;

truncate table bec_ods_stg.HR_LOCATIONS_ALL;

insert into	bec_ods_stg.HR_LOCATIONS_ALL
   (location_id,
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
	"style",
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
	"TAX_NAME#1",
	kca_operation,
	kca_seq_id,
	kca_seq_date)
(
	select
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
	"style",
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
	"TAX_NAME#1",
	kca_operation,
	kca_seq_id,
	kca_seq_date
	from bec_raw_dl_ext.HR_LOCATIONS_ALL
	where kca_operation != 'DELETE'  and nvl(kca_seq_id,'')!= '' 
	and (LOCATION_ID,kca_seq_id) in 
	(select LOCATION_ID,max(kca_seq_id) from bec_raw_dl_ext.HR_LOCATIONS_ALL 
     where kca_operation != 'DELETE'  and nvl(kca_seq_id,'')!= ''
     group by LOCATION_ID)
        and	( kca_seq_date > (
		select
			(executebegints-prune_days)
		from
			bec_etl_ctrl.batch_ods_info
		where
			ods_table_name = 'hr_locations_all')
		 
            )
);
end;