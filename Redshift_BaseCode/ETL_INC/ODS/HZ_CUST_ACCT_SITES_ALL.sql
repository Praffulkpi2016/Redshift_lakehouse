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

delete from bec_ods.HZ_CUST_ACCT_SITES_ALL
where (CUST_ACCT_SITE_ID) in (
select stg.CUST_ACCT_SITE_ID from bec_ods.HZ_CUST_ACCT_SITES_ALL ods, bec_ods_stg.HZ_CUST_ACCT_SITES_ALL stg
where ods.CUST_ACCT_SITE_ID = stg.CUST_ACCT_SITE_ID  and stg.kca_operation IN ('INSERT','UPDATE')
);

commit;

-- Insert records

insert
	into
	bec_ods.hz_cust_acct_sites_all
(cust_acct_site_id,
	cust_account_id,
	party_site_id,
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
	status,
	org_id,
	bill_to_flag,
	market_flag,
	ship_to_flag,
	customer_category_code,
	"language",
	key_account_flag,
	tp_header_id,
	ece_tp_location_code,
	service_territory_id,
	primary_specialist_id,
	secondary_specialist_id,
	territory_id,
	address_text,
	territory,
	translated_customer_name,
	object_version_number,
	created_by_module,
	application_id,
	kca_operation,
	IS_DELETED_FLG,
	kca_seq_id,
	kca_seq_date)
	SELECT
	cust_acct_site_id,
	cust_account_id,
	party_site_id,
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
	status,
	org_id,
	bill_to_flag,
	market_flag,
	ship_to_flag,
	customer_category_code,
	"language",
	key_account_flag,
	tp_header_id,
	ece_tp_location_code,
	service_territory_id,
	primary_specialist_id,
	secondary_specialist_id,
	territory_id,
	address_text,
	territory,
	translated_customer_name,
	object_version_number,
	created_by_module,
	application_id,
		KCA_OPERATION,
		'N' as IS_DELETED_FLG,
		cast(NULLIF(KCA_SEQ_ID,'') as numeric(36,0)) as KCA_SEQ_ID,
	kca_seq_date
    FROM
        bec_ods_stg.HZ_CUST_ACCT_SITES_ALL
	where kca_operation IN ('INSERT','UPDATE') 
	and (CUST_ACCT_SITE_ID,kca_seq_id) in 
	(select CUST_ACCT_SITE_ID,max(kca_seq_id) from bec_ods_stg.HZ_CUST_ACCT_SITES_ALL 
     where kca_operation IN ('INSERT','UPDATE')
     group by CUST_ACCT_SITE_ID);

commit;
 

-- Soft delete
update bec_ods.HZ_CUST_ACCT_SITES_ALL set IS_DELETED_FLG = 'N';
commit;
update bec_ods.HZ_CUST_ACCT_SITES_ALL set IS_DELETED_FLG = 'Y'
where (CUST_ACCT_SITE_ID)  in
(
select CUST_ACCT_SITE_ID from bec_raw_dl_ext.HZ_CUST_ACCT_SITES_ALL
where (CUST_ACCT_SITE_ID,KCA_SEQ_ID)
in 
(
select CUST_ACCT_SITE_ID,max(KCA_SEQ_ID) as KCA_SEQ_ID 
from bec_raw_dl_ext.HZ_CUST_ACCT_SITES_ALL
group by CUST_ACCT_SITE_ID
) 
and kca_operation= 'DELETE'
);
commit;

end;

update bec_etl_ctrl.batch_ods_info
set	last_refresh_date = getdate()
where ods_table_name = 'hz_cust_acct_sites_all';

commit;