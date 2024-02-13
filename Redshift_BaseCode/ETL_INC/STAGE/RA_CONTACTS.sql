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

truncate table bec_ods_stg.ra_contacts;

insert
	into
	bec_ods_stg.ra_contacts
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
	--is_deleted_flg,
	kca_seq_id
	,KCA_SEQ_DATE)
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
	--'N' as IS_DELETED_FLG,
	KCA_SEQ_ID
	,KCA_SEQ_DATE
    FROM
        bec_raw_dl_ext.ra_contacts
	where kca_operation != 'DELETE'  and nvl(kca_seq_id,'') != '' 
	and (CONTACT_ID,kca_seq_id) in 
	(select CONTACT_ID,max(kca_seq_id) from bec_raw_dl_ext.ra_contacts 
     where kca_operation != 'DELETE'  and nvl(kca_seq_id,'') != ''
     group by CONTACT_ID)
        and	(KCA_SEQ_DATE > (
		select
			(executebegints-prune_days)
		from
			bec_etl_ctrl.batch_ods_info
		where
			ods_table_name = 'ra_contacts')

            )
			;
end;