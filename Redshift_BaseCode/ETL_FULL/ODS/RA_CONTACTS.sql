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

DROP TABLE if exists bec_ods.ra_contacts;

CREATE TABLE IF NOT EXISTS bec_ods.ra_contacts
(	contact_id NUMERIC(15,0)   ENCODE az64
	,last_update_date TIMESTAMP WITHOUT TIME ZONE   ENCODE az64
	,last_updated_by NUMERIC(15,0)   ENCODE az64
	,creation_date TIMESTAMP WITHOUT TIME ZONE   ENCODE az64
	,created_by NUMERIC(15,0)   ENCODE az64
	,customer_id NUMERIC(15,0)   ENCODE az64
	,status VARCHAR(1)   ENCODE lzo
	,orig_system_reference VARCHAR(240)   ENCODE lzo
	,last_name VARCHAR(50)   ENCODE lzo
	,last_update_login NUMERIC(15,0)   ENCODE az64
	,title VARCHAR(30)   ENCODE lzo
	,first_name VARCHAR(40)   ENCODE lzo
	,job_title VARCHAR(50)   ENCODE lzo
	,mail_stop VARCHAR(60)   ENCODE lzo
	,address_id NUMERIC(15,0)   ENCODE az64
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
	,request_id NUMERIC(15,0)   ENCODE az64
	,program_application_id NUMERIC(15,0)   ENCODE az64
	,program_id NUMERIC(15,0)   ENCODE az64
	,program_update_date TIMESTAMP WITHOUT TIME ZONE   ENCODE az64
	,contact_key VARCHAR(50)   ENCODE lzo
	,contact_personal_information VARCHAR(240)   ENCODE lzo
	,decision_maker_flag VARCHAR(1)   ENCODE lzo
	,job_title_code VARCHAR(30)   ENCODE lzo
	,managed_by NUMERIC(15,0)   ENCODE az64
	,native_language VARCHAR(30)   ENCODE lzo
	,reference_use_flag VARCHAR(1)   ENCODE lzo
	,contact_number VARCHAR(30)   ENCODE lzo
	,attribute11 VARCHAR(150)   ENCODE lzo
	,attribute25 VARCHAR(150)   ENCODE lzo
	,other_language_1 VARCHAR(30)   ENCODE lzo
	,other_language_2 VARCHAR(30)   ENCODE lzo
	,rank VARCHAR(30)   ENCODE lzo
	,primary_role VARCHAR(30)   ENCODE lzo
	,attribute12 VARCHAR(150)   ENCODE lzo
	,attribute13 VARCHAR(150)   ENCODE lzo
	,attribute14 VARCHAR(150)   ENCODE lzo
	,attribute15 VARCHAR(150)   ENCODE lzo
	,attribute16 VARCHAR(150)   ENCODE lzo
	,attribute17 VARCHAR(150)   ENCODE lzo
	,attribute18 VARCHAR(150)   ENCODE lzo
	,attribute19 VARCHAR(150)   ENCODE lzo
	,attribute20 VARCHAR(150)   ENCODE lzo
	,attribute21 VARCHAR(150)   ENCODE lzo
	,attribute22 VARCHAR(150)   ENCODE lzo
	,attribute23 VARCHAR(150)   ENCODE lzo
	,attribute24 VARCHAR(150)   ENCODE lzo
	,do_not_mail_flag VARCHAR(1)   ENCODE lzo
	,suffix VARCHAR(60)   ENCODE lzo
	,email_address VARCHAR(240)   ENCODE lzo
	,mailing_address_id NUMERIC(15,0)   ENCODE az64
	,match_group_id NUMERIC(15,0)   ENCODE az64
	,sex_code VARCHAR(30)   ENCODE lzo
	,salutation VARCHAR(60)   ENCODE lzo
	,department_code VARCHAR(30)   ENCODE lzo
	,department VARCHAR(40)   ENCODE lzo
	,last_name_alt VARCHAR(50)   ENCODE lzo
	,first_name_alt VARCHAR(40)   ENCODE lzo
	,do_not_email_flag VARCHAR(1)   ENCODE lzo
	,kca_operation VARCHAR(10)   ENCODE lzo
	,is_deleted_flg VARCHAR(2)   ENCODE lzo
	,kca_seq_id NUMERIC(36,0)   ENCODE az64
		,kca_seq_date TIMESTAMP WITHOUT TIME ZONE   ENCODE az64
)
DISTSTYLE AUTO
;
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
	cast(NULLIF(KCA_SEQ_ID,'') as numeric(36,0)) as KCA_SEQ_ID
	,kca_seq_date
    FROM
        bec_ods_stg.ra_contacts;

end;		

UPDATE bec_etl_ctrl.batch_ods_info
SET
    load_type = 'I',
    last_refresh_date = getdate()
WHERE
    ods_table_name = 'ra_contacts';
	
commit;
	
