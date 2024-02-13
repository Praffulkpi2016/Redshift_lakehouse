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

delete from bec_ods.HR_LOCATIONS_ALL
where (LOCATION_ID) in (
select stg.LOCATION_ID
from bec_ods.HR_LOCATIONS_ALL ods, bec_ods_stg.HR_LOCATIONS_ALL stg
where ods.LOCATION_ID = stg.LOCATION_ID

and stg.kca_operation IN ('INSERT','UPDATE')
);

commit;

-- Insert records

insert into	bec_ods.HR_LOCATIONS_ALL
       (	location_id,
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
	is_deleted_flg,
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
        KCA_OPERATION,
       'N' AS IS_DELETED_FLG,
	    cast(NULLIF(KCA_SEQ_ID,'') as numeric(36,0)) as KCA_SEQ_ID,
	kca_seq_date
	from bec_ods_stg.HR_LOCATIONS_ALL
	where kca_operation IN ('INSERT','UPDATE') 
	and (LOCATION_ID,kca_seq_id) in 
	(select LOCATION_ID,max(kca_seq_id) from bec_ods_stg.HR_LOCATIONS_ALL 
     where kca_operation IN ('INSERT','UPDATE')
     group by LOCATION_ID)
);

commit;
 
-- Soft delete
update bec_ods.HR_LOCATIONS_ALL set IS_DELETED_FLG = 'N';
commit;
update bec_ods.HR_LOCATIONS_ALL set IS_DELETED_FLG = 'Y'
where (LOCATION_ID)  in
(
select LOCATION_ID from bec_raw_dl_ext.HR_LOCATIONS_ALL
where (LOCATION_ID,KCA_SEQ_ID)
in 
(
select LOCATION_ID,max(KCA_SEQ_ID) as KCA_SEQ_ID 
from bec_raw_dl_ext.HR_LOCATIONS_ALL
group by LOCATION_ID
) 
and kca_operation= 'DELETE'
);
commit;

end;

update bec_etl_ctrl.batch_ods_info
set	last_refresh_date = getdate()
where ods_table_name = 'hr_locations_all';

commit;