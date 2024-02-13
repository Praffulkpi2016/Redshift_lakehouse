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

delete from bec_ods.HZ_CUST_ACCOUNT_ROLES
where cust_account_role_id in (
select stg.cust_account_role_id from bec_ods.HZ_CUST_ACCOUNT_ROLES ods, bec_ods_stg.HZ_CUST_ACCOUNT_ROLES stg
where ods.cust_account_role_id = stg.cust_account_role_id and stg.kca_operation IN ('INSERT','UPDATE')
);

commit;

-- Insert records

insert into	bec_ods.HZ_CUST_ACCOUNT_ROLES
       (
    cust_account_role_id,
	party_id,
	cust_account_id,
	current_role_state,
	current_role_state_effective,
	cust_acct_site_id,
	begin_date,
	end_date,
	primary_flag,
	role_type,
	last_update_date,
	source_code,
	last_updated_by,
	creation_date,
	created_by,
	last_update_login,
	wh_update_date,
	request_id,
	program_application_id,
	program_id,
	program_update_date,
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
	attribute21,
	attribute22,
	attribute23,
	attribute24,
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
	attribute25,
	status,
	object_version_number,
	created_by_module,
	application_id,
	kca_operation,
	is_deleted_flg,
	kca_seq_id,
	kca_seq_date
)
(
	SELECT
	cust_account_role_id,
	party_id,
	cust_account_id,
	current_role_state,
	current_role_state_effective,
	cust_acct_site_id,
	begin_date,
	end_date,
	primary_flag,
	role_type,
	last_update_date,
	source_code,
	last_updated_by,
	creation_date,
	created_by,
	last_update_login,
	wh_update_date,
	request_id,
	program_application_id,
	program_id,
	program_update_date,
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
	attribute21,
	attribute22,
	attribute23,
	attribute24,
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
	attribute25,
	status,
	object_version_number,
	created_by_module,
	application_id,
	kca_operation,
       'N' AS IS_DELETED_FLG,
	    cast(NULLIF(KCA_SEQ_ID,'') as numeric(36,0)) as KCA_SEQ_ID,
	kca_seq_date
	from bec_ods_stg.HZ_CUST_ACCOUNT_ROLES
	where kca_operation IN ('INSERT','UPDATE') 
	and (cust_account_role_id,kca_seq_id) in 
	(select cust_account_role_id,max(kca_seq_id) from bec_ods_stg.HZ_CUST_ACCOUNT_ROLES 
     where kca_operation IN ('INSERT','UPDATE')
     group by cust_account_role_id)
);

commit;
 
-- Soft delete
update bec_ods.HZ_CUST_ACCOUNT_ROLES set IS_DELETED_FLG = 'N';
commit;
update bec_ods.HZ_CUST_ACCOUNT_ROLES set IS_DELETED_FLG = 'Y'
where (cust_account_role_id)  in
(
select cust_account_role_id from bec_raw_dl_ext.HZ_CUST_ACCOUNT_ROLES
where (cust_account_role_id,KCA_SEQ_ID)
in 
(
select cust_account_role_id,max(KCA_SEQ_ID) as KCA_SEQ_ID 
from bec_raw_dl_ext.HZ_CUST_ACCOUNT_ROLES
group by cust_account_role_id
) 
and kca_operation= 'DELETE'
);
commit;

end;

update bec_etl_ctrl.batch_ods_info
set	last_refresh_date = getdate()
where ods_table_name = 'hz_cust_account_roles';

commit;