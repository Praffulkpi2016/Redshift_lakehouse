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

truncate table bec_ods_stg.HZ_PARTY_SITES;

insert into	bec_ods_stg.HZ_PARTY_SITES
   (party_site_id,
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
	"language",
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
	kca_seq_id,
	kca_seq_date)
(
	select
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
	"language",
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
	kca_seq_id,
	kca_seq_date
	from bec_raw_dl_ext.HZ_PARTY_SITES
	where kca_operation != 'DELETE'  and nvl(kca_seq_id,'')!= '' 
	and (party_site_id,kca_seq_id) in 
	(select party_site_id,max(kca_seq_id) from bec_raw_dl_ext.HZ_PARTY_SITES 
     where kca_operation != 'DELETE'  and nvl(kca_seq_id,'')!= ''
     group by party_site_id)
        and	( kca_seq_date > (
		select
			(executebegints-prune_days)
		from
			bec_etl_ctrl.batch_ods_info
		where
			ods_table_name = 'hz_party_sites')
		  
            )
);
end;