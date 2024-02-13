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

DROP TABLE if exists bec_ods.hz_cust_acct_sites_all;

CREATE TABLE  IF NOT EXISTS bec_ods.hz_cust_acct_sites_all
(
	cust_acct_site_id NUMERIC(15,0)   ENCODE az64
	,cust_account_id NUMERIC(15,0)   ENCODE az64
	,party_site_id NUMERIC(15,0)   ENCODE az64
	,last_update_date TIMESTAMP WITHOUT TIME ZONE   ENCODE az64
	,last_updated_by NUMERIC(15,0)   ENCODE az64
	,creation_date TIMESTAMP WITHOUT TIME ZONE   ENCODE az64
	,created_by NUMERIC(15,0)   ENCODE az64
	,last_update_login NUMERIC(15,0)   ENCODE az64
	,request_id NUMERIC(15,0)   ENCODE az64
	,program_application_id NUMERIC(15,0)   ENCODE az64
	,program_id NUMERIC(15,0)   ENCODE az64
	,program_update_date TIMESTAMP WITHOUT TIME ZONE   ENCODE az64
	,wh_update_date TIMESTAMP WITHOUT TIME ZONE   ENCODE az64
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
	,attribute11 VARCHAR(150)   ENCODE lzo
	,attribute12 VARCHAR(150)   ENCODE lzo
	,attribute13 VARCHAR(150)   ENCODE lzo
	,attribute14 VARCHAR(150)   ENCODE lzo
	,attribute15 VARCHAR(150)   ENCODE lzo
	,attribute16 VARCHAR(150)   ENCODE lzo
	,attribute17 VARCHAR(150)   ENCODE lzo
	,attribute18 VARCHAR(150)   ENCODE lzo
	,attribute19 VARCHAR(150)   ENCODE lzo
	,attribute20 VARCHAR(150)   ENCODE lzo
	,global_attribute_category VARCHAR(30)   ENCODE lzo
	,global_attribute1 VARCHAR(150)   ENCODE lzo
	,global_attribute2 VARCHAR(150)   ENCODE lzo
	,global_attribute3 VARCHAR(150)   ENCODE lzo
	,global_attribute4 VARCHAR(150)   ENCODE lzo
	,global_attribute5 VARCHAR(150)   ENCODE lzo
	,global_attribute6 VARCHAR(150)   ENCODE lzo
	,global_attribute7 VARCHAR(150)   ENCODE lzo
	,global_attribute8 VARCHAR(150)   ENCODE lzo
	,global_attribute9 VARCHAR(150)   ENCODE lzo
	,global_attribute10 VARCHAR(150)   ENCODE lzo
	,global_attribute11 VARCHAR(150)   ENCODE lzo
	,global_attribute12 VARCHAR(150)   ENCODE lzo
	,global_attribute13 VARCHAR(150)   ENCODE lzo
	,global_attribute14 VARCHAR(150)   ENCODE lzo
	,global_attribute15 VARCHAR(150)   ENCODE lzo
	,global_attribute16 VARCHAR(150)   ENCODE lzo
	,global_attribute17 VARCHAR(150)   ENCODE lzo
	,global_attribute18 VARCHAR(150)   ENCODE lzo
	,global_attribute19 VARCHAR(150)   ENCODE lzo
	,global_attribute20 VARCHAR(150)   ENCODE lzo
	,orig_system_reference VARCHAR(240)   ENCODE lzo
	,status VARCHAR(1)   ENCODE lzo
	,org_id NUMERIC(15,0)   ENCODE az64
	,bill_to_flag VARCHAR(1)   ENCODE lzo
	,market_flag VARCHAR(1)   ENCODE lzo
	,ship_to_flag VARCHAR(1)   ENCODE lzo
	,customer_category_code VARCHAR(30)   ENCODE lzo
	,"language" VARCHAR(4)   ENCODE lzo
	,key_account_flag VARCHAR(1)   ENCODE lzo
	,tp_header_id NUMERIC(15,0)   ENCODE az64
	,ece_tp_location_code VARCHAR(40)   ENCODE lzo
	,service_territory_id NUMERIC(15,0)   ENCODE az64
	,primary_specialist_id NUMERIC(15,0)   ENCODE az64
	,secondary_specialist_id NUMERIC(15,0)   ENCODE az64
	,territory_id NUMERIC(15,0)   ENCODE az64
	,address_text VARCHAR(3000)   ENCODE lzo
	,territory VARCHAR(30)   ENCODE lzo
	,translated_customer_name VARCHAR(50)   ENCODE lzo
	,object_version_number NUMERIC(15,0)   ENCODE az64
	,created_by_module VARCHAR(150)   ENCODE lzo
	,application_id NUMERIC(15,0)   ENCODE az64
	,KCA_OPERATION VARCHAR(10)   ENCODE lzo
    ,IS_DELETED_FLG VARCHAR(2) ENCODE lzo
	,kca_seq_id NUMERIC(36,0)   ENCODE az64
	,kca_seq_date TIMESTAMP WITHOUT TIME ZONE   ENCODE az64
)
DISTSTYLE AUTO
;
	
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
        bec_ods_stg.HZ_CUST_ACCT_SITES_ALL;

end;		

UPDATE bec_etl_ctrl.batch_ods_info
SET
    load_type = 'I',
    last_refresh_date = getdate()
WHERE
    ods_table_name = 'hz_cust_acct_sites_all';
	
commit;
	
