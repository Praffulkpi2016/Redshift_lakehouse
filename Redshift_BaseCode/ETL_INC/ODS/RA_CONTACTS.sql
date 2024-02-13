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

delete from bec_ods.RA_CONTACTS
where CONTACT_ID in (
select stg.CONTACT_ID from bec_ods.RA_CONTACTS ods, bec_ods_stg.RA_CONTACTS stg
where ods.CONTACT_ID = stg.CONTACT_ID and stg.kca_operation IN ('INSERT','UPDATE')
);

commit;

-- Insert records

insert
	into
	bec_ods.ra_contacts
(	contact_id,
	last_update_date,
	last_updated_by,
	creation_date,
	created_by,
	customer_id,
	status,
	orig_system_reference,
	last_name,
	last_update_login,
	title,
	first_name,
	job_title,
	mail_stop,
	address_id,
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
	request_id,
	program_application_id,
	program_id,
	program_update_date,
	contact_key,
	contact_personal_information,
	decision_maker_flag,
	job_title_code,
	managed_by,
	native_language,
	reference_use_flag,
	contact_number,
	attribute11,
	attribute25,
	other_language_1,
	other_language_2,
	"rank",
	primary_role,
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
	do_not_mail_flag,
	suffix,
	email_address,
	mailing_address_id,
	match_group_id,
	sex_code,
	salutation,
	department_code,
	department,
	last_name_alt,
	first_name_alt,
	do_not_email_flag,
	kca_operation,
	is_deleted_flg,
	kca_seq_id
	,kca_seq_date)
	SELECT
	contact_id,
	last_update_date,
	last_updated_by,
	creation_date,
	created_by,
	customer_id,
	status,
	orig_system_reference,
	last_name,
	last_update_login,
	title,
	first_name,
	job_title,
	mail_stop,
	address_id,
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
	request_id,
	program_application_id,
	program_id,
	program_update_date,
	contact_key,
	contact_personal_information,
	decision_maker_flag,
	job_title_code,
	managed_by,
	native_language,
	reference_use_flag,
	contact_number,
	attribute11,
	attribute25,
	other_language_1,
	other_language_2,
	"rank",
	primary_role,
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
	do_not_mail_flag,
	suffix,
	email_address,
	mailing_address_id,
	match_group_id,
	sex_code,
	salutation,
	department_code,
	department,
	last_name_alt,
	first_name_alt,
	do_not_email_flag,
	KCA_OPERATION,
	'N' as IS_DELETED_FLG,
	cast(NULLIF(KCA_SEQ_ID,'') as numeric(36,0)) as KCA_SEQ_ID,
	KCA_SEQ_DATE
    FROM
        bec_ods_stg.ra_contacts
	where kca_operation in ('INSERT','UPDATE') 
	and (CONTACT_ID,kca_seq_id) in 
	(select CONTACT_ID,max(kca_seq_id) from bec_ods_stg.RA_CONTACTS 
     where kca_operation in ('INSERT','UPDATE')
     group by CONTACT_ID);

commit;



-- Soft delete
update bec_ods.RA_CONTACTS set IS_DELETED_FLG = 'N';
commit;
update bec_ods.RA_CONTACTS set IS_DELETED_FLG = 'Y'
where (CONTACT_ID)  in
(
select CONTACT_ID from bec_raw_dl_ext.RA_CONTACTS
where (CONTACT_ID,KCA_SEQ_ID)
in 
(
select CONTACT_ID,max(KCA_SEQ_ID) as KCA_SEQ_ID 
from bec_raw_dl_ext.RA_CONTACTS
group by CONTACT_ID
) 
and kca_operation= 'DELETE'
);
commit;

end;

update bec_etl_ctrl.batch_ods_info
set	last_refresh_date = getdate()
where ods_table_name = 'RA_CONTACTS';

commit;