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

DROP TABLE if exists bec_ods.jtf_rs_resource_extns;

CREATE TABLE IF NOT EXISTS bec_ods.jtf_rs_resource_extns
(
	resource_id NUMERIC(15,0)   ENCODE az64
	,created_by NUMERIC(15,0)   ENCODE az64
	,creation_date TIMESTAMP WITHOUT TIME ZONE   ENCODE az64
	,last_updated_by NUMERIC(15,0)   ENCODE az64
	,last_update_date TIMESTAMP WITHOUT TIME ZONE   ENCODE az64
	,last_update_login NUMERIC(15,0)   ENCODE az64
	,category VARCHAR(30)   ENCODE lzo
	,resource_number VARCHAR(30)   ENCODE lzo
	,source_id NUMERIC(15,0)   ENCODE az64
	,address_id NUMERIC(15,0)   ENCODE az64
	,contact_id NUMERIC(15,0)   ENCODE az64
	,managing_employee_id NUMERIC(15,0)   ENCODE az64
	,start_date_active TIMESTAMP WITHOUT TIME ZONE   ENCODE az64
	,end_date_active TIMESTAMP WITHOUT TIME ZONE   ENCODE az64
	,time_zone NUMERIC(15,0)   ENCODE az64
	,cost_per_hr NUMERIC(28,10)   ENCODE az64
	,primary_language VARCHAR(30)   ENCODE lzo
	,secondary_language VARCHAR(30)   ENCODE lzo
	,ies_agent_login VARCHAR(240)   ENCODE lzo
	,server_group_id NUMERIC(15,0)   ENCODE az64
	,assigned_to_group_id NUMERIC(15,0)   ENCODE az64
	,cost_center VARCHAR(30)   ENCODE lzo
	,charge_to_cost_center VARCHAR(30)   ENCODE lzo
	,compensation_currency_code VARCHAR(15)   ENCODE lzo
	,commissionable_flag VARCHAR(1)   ENCODE lzo
	,hold_reason_code VARCHAR(30)   ENCODE lzo
	,hold_payment VARCHAR(1)   ENCODE lzo
	,comp_service_team_id NUMERIC(15,0)   ENCODE az64
	,transaction_number NUMERIC(15,0)   ENCODE az64
	,object_version_number NUMERIC(15,0)   ENCODE az64
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
	,attribute_category VARCHAR(30)   ENCODE lzo
	,user_id NUMERIC(15,0)   ENCODE az64
	,support_site_id NUMERIC(15,0)   ENCODE az64
	,security_group_id NUMERIC(15,0)   ENCODE az64
	,support_site VARCHAR(30)   ENCODE lzo
	,source_name VARCHAR(360)   ENCODE lzo
	,source_number VARCHAR(30)   ENCODE lzo
	,source_job_title VARCHAR(240)   ENCODE lzo
	,source_email VARCHAR(2000)   ENCODE lzo
	,source_phone VARCHAR(2000)   ENCODE lzo
	,source_org_id NUMERIC(15,0)   ENCODE az64
	,source_org_name VARCHAR(360)   ENCODE lzo
	,source_address1 VARCHAR(240)   ENCODE lzo
	,source_address2 VARCHAR(240)   ENCODE lzo
	,source_address3 VARCHAR(240)   ENCODE lzo
	,source_address4 VARCHAR(240)   ENCODE lzo
	,source_city VARCHAR(60)   ENCODE lzo
	,source_postal_code VARCHAR(60)   ENCODE lzo
	,source_state VARCHAR(150)   ENCODE lzo
	,source_province VARCHAR(150)   ENCODE lzo
	,source_county VARCHAR(150)   ENCODE lzo
	,source_country VARCHAR(60)   ENCODE lzo
	,source_mgr_id NUMERIC(15,0)   ENCODE az64
	,source_mgr_name VARCHAR(360)   ENCODE lzo
	,source_business_grp_id NUMERIC(15,0)   ENCODE az64
	,source_business_grp_name VARCHAR(360)   ENCODE lzo
	,source_first_name VARCHAR(360)   ENCODE lzo
	,source_middle_name VARCHAR(360)   ENCODE lzo
	,source_last_name VARCHAR(360)   ENCODE lzo
	,source_category VARCHAR(360)   ENCODE lzo
	,source_status VARCHAR(360)   ENCODE lzo 
	,user_name VARCHAR(100)   ENCODE lzo
	,source_office VARCHAR(45)   ENCODE lzo
	,source_location VARCHAR(45)   ENCODE lzo
	,source_mailstop VARCHAR(45)   ENCODE lzo
	,source_mobile_phone VARCHAR(60)   ENCODE lzo
	,source_pager VARCHAR(60)   ENCODE lzo 
	,source_job_id NUMERIC(15,0)   ENCODE az64
	,person_party_id NUMERIC(15,0)   ENCODE az64
	,FS_SETUP_COMPLETE VARCHAR(1)  ENCODE lzo
	,KCA_OPERATION VARCHAR(10)   ENCODE lzo
    ,IS_DELETED_FLG VARCHAR(2) ENCODE lzo
	,kca_seq_id NUMERIC(36,0)   ENCODE az64
	,kca_seq_date TIMESTAMP WITHOUT TIME ZONE   ENCODE az64
)
DISTSTYLE AUTO
;

insert
	into
	bec_ods.jtf_rs_resource_extns
(resource_id,
	created_by,
	creation_date,
	last_updated_by,
	last_update_date,
	last_update_login,
	category,
	resource_number,
	source_id,
	address_id,
	contact_id,
	managing_employee_id,
	start_date_active,
	end_date_active,
	time_zone,
	cost_per_hr,
	primary_language,
	secondary_language,
	ies_agent_login,
	server_group_id,
	assigned_to_group_id,
	cost_center,
	charge_to_cost_center,
	compensation_currency_code,
	commissionable_flag,
	hold_reason_code,
	hold_payment,
	comp_service_team_id,
	transaction_number,
	object_version_number,
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
	attribute_category,
	user_id,
	support_site_id,
	security_group_id,
	support_site,
	source_name,
	source_number,
	source_job_title,
	source_email,
	source_phone,
	source_org_id,
	source_org_name,
	source_address1,
	source_address2,
	source_address3,
	source_address4,
	source_city,
	source_postal_code,
	source_state,
	source_province,
	source_county,
	source_country,
	source_mgr_id,
	source_mgr_name,
	source_business_grp_id,
	source_business_grp_name,
	source_first_name,
	source_middle_name,
	source_last_name,
	source_category,
	source_status, 
	user_name,
	source_office,
	source_location,
	source_mailstop,
	source_mobile_phone,
	source_pager, 
	source_job_id,
	person_party_id,
	FS_SETUP_COMPLETE,
	kca_operation,
	is_deleted_flg,
	kca_seq_id,
	kca_seq_date)
	SELECT
	resource_id,
	created_by,
	creation_date,
	last_updated_by,
	last_update_date,
	last_update_login,
	category,
	resource_number,
	source_id,
	address_id,
	contact_id,
	managing_employee_id,
	start_date_active,
	end_date_active,
	time_zone,
	cost_per_hr,
	primary_language,
	secondary_language,
	ies_agent_login,
	server_group_id,
	assigned_to_group_id,
	cost_center,
	charge_to_cost_center,
	compensation_currency_code,
	commissionable_flag,
	hold_reason_code,
	hold_payment,
	comp_service_team_id,
	transaction_number,
	object_version_number,
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
	attribute_category,
	user_id,
	support_site_id,
	security_group_id,
	support_site,
	source_name,
	source_number,
	source_job_title,
	source_email,
	source_phone,
	source_org_id,
	source_org_name,
	source_address1,
	source_address2,
	source_address3,
	source_address4,
	source_city,
	source_postal_code,
	source_state,
	source_province,
	source_county,
	source_country,
	source_mgr_id,
	source_mgr_name,
	source_business_grp_id,
	source_business_grp_name,
	source_first_name,
	source_middle_name,
	source_last_name,
	source_category,
	source_status, 
	user_name,
	source_office,
	source_location,
	source_mailstop,
	source_mobile_phone,
	source_pager, 
	source_job_id,
	person_party_id,
	FS_SETUP_COMPLETE,
	KCA_OPERATION,
	'N' as IS_DELETED_FLG,
	cast(NULLIF(KCA_SEQ_ID,'') as numeric(36,0)) as KCA_SEQ_ID,
	kca_seq_date	
    FROM
        bec_ods_stg.jtf_rs_resource_extns;

end;		

UPDATE bec_etl_ctrl.batch_ods_info
SET
    load_type = 'I',
    last_refresh_date = getdate()
WHERE
    ods_table_name = 'jtf_rs_resource_extns';
	
commit;
	
