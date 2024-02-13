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

DROP TABLE if exists bec_ods.MSC_COMPANY_SITES  ;

CREATE TABLE IF NOT EXISTS bec_ods.MSC_COMPANY_SITES  
(

  company_site_id NUMERIC(15,0)   ENCODE az64
	,company_id NUMERIC(15,0)   ENCODE az64
	,company_site_name VARCHAR(40)   ENCODE lzo
	,sr_instance_id NUMERIC(15,0)   ENCODE az64
	,deleted_flag NUMERIC(15,0)   ENCODE az64
	,longitude NUMERIC(28,10)   ENCODE az64
	,latitude NUMERIC(28,10)   ENCODE az64
	,refresh_number NUMERIC(15,0)   ENCODE az64
	,disable_date TIMESTAMP WITHOUT TIME ZONE   ENCODE az64
	,planning_enabled VARCHAR(3)   ENCODE lzo
	,creation_date TIMESTAMP WITHOUT TIME ZONE   ENCODE az64
	,created_by NUMERIC(15,0)   ENCODE az64
	,last_update_date TIMESTAMP WITHOUT TIME ZONE   ENCODE az64
	,last_updated_by NUMERIC(15,0)   ENCODE az64
	,last_update_login NUMERIC(15,0)   ENCODE az64
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
	,request_id NUMERIC(15,0)   ENCODE az64
	,program_id NUMERIC(15,0)   ENCODE az64
	,program_update_date TIMESTAMP WITHOUT TIME ZONE   ENCODE az64
	,"location" VARCHAR(60)   ENCODE lzo
	,address1 VARCHAR(240)   ENCODE lzo
	,address2 VARCHAR(240)   ENCODE lzo
	,address3 VARCHAR(240)   ENCODE lzo
	,address4 VARCHAR(240)   ENCODE lzo
	,country VARCHAR(60)   ENCODE lzo
	,state VARCHAR(150)   ENCODE lzo
	,city VARCHAR(60)   ENCODE lzo
	,county VARCHAR(150)   ENCODE lzo
	,province VARCHAR(150)   ENCODE lzo
	,postal_code VARCHAR(60)   ENCODE lzo
	,KCA_OPERATION VARCHAR(10)   ENCODE lzo
    ,IS_DELETED_FLG VARCHAR(2) ENCODE lzo
	,kca_seq_id NUMERIC(36,0)   ENCODE az64
	,kca_seq_date TIMESTAMP WITHOUT TIME ZONE   ENCODE az64
)
DISTSTYLE
auto;

INSERT INTO bec_ods.MSC_COMPANY_SITES   (
   company_site_id,
	company_id,
	company_site_name,
	sr_instance_id,
	deleted_flag,
	longitude,
	latitude,
	refresh_number,
	disable_date,
	planning_enabled,
	creation_date,
	created_by,
	last_update_date,
	last_updated_by,
	last_update_login,
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
	request_id,
	program_id,
	program_update_date,
	"location",
	address1,
	address2,
	address3,
	address4,
	country,
	state,
	city,
	county,
	province,
	postal_code,
	kca_operation,
	IS_DELETED_FLG,
	kca_seq_id,
	kca_seq_date)
	
    SELECT
    company_site_id,
	company_id,
	company_site_name,
	sr_instance_id,
	deleted_flag,
	longitude,
	latitude,
	refresh_number,
	disable_date,
	planning_enabled,
	creation_date,
	created_by,
	last_update_date,
	last_updated_by,
	last_update_login,
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
	request_id,
	program_id,
	program_update_date,
	"location",
	address1,
	address2,
	address3,
	address4,
	country,
	state,
	city,
	county,
	province,
	postal_code,
		KCA_OPERATION,
		'N' as IS_DELETED_FLG,
		cast(NULLIF(KCA_SEQ_ID,'') as numeric(36,0)) as KCA_SEQ_ID,
	kca_seq_date
    FROM
        bec_ods_stg.MSC_COMPANY_SITES  ;

end;		

UPDATE bec_etl_ctrl.batch_ods_info
SET
    load_type = 'I',
    last_refresh_date = getdate()
WHERE
    ods_table_name = 'msc_company_sites';
	
commit;