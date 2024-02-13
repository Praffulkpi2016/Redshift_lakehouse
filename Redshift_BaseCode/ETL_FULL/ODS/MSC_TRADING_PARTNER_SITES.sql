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

DROP TABLE if exists bec_ods.MSC_TRADING_PARTNER_SITES  ;

CREATE TABLE IF NOT EXISTS bec_ods.MSC_TRADING_PARTNER_SITES  
(

   partner_site_id NUMERIC(15,0)   ENCODE az64
	,sr_tp_site_id NUMERIC(15,0)   ENCODE az64
	,sr_instance_id NUMERIC(15,0)   ENCODE az64
	,partner_id NUMERIC(15,0)   ENCODE az64
	,partner_address VARCHAR(1600)   ENCODE lzo
	,tp_site_code VARCHAR(30)   ENCODE lzo
	,sr_tp_id NUMERIC(15,0)   ENCODE az64
	,"location" VARCHAR(60)   ENCODE lzo
	,partner_type NUMERIC(15,0)   ENCODE az64
	,deleted_flag NUMERIC(15,0)   ENCODE az64
	,longitude NUMERIC(10,7)   ENCODE az64
	,latitude NUMERIC(10,7)   ENCODE az64
	,refresh_number NUMERIC(15,0)   ENCODE az64
	,last_update_date TIMESTAMP WITHOUT TIME ZONE   ENCODE az64
	,last_updated_by NUMERIC(15,0)   ENCODE az64
	,creation_date TIMESTAMP WITHOUT TIME ZONE   ENCODE az64
	,created_by NUMERIC(15,0)   ENCODE az64
	,last_update_login NUMERIC(15,0)   ENCODE az64
	,request_id NUMERIC(15,0)   ENCODE az64
	,program_application_id NUMERIC(15,0)   ENCODE az64
	,program_id NUMERIC(15,0)   ENCODE az64
	,program_update_date TIMESTAMP WITHOUT TIME ZONE   ENCODE az64
	,attribute_category VARCHAR(30)   ENCODE lzo
	,attribute1 VARCHAR(150)   ENCODE lzo
	,attribute2 VARCHAR(150)   ENCODE lzo
	,attribute3 VARCHAR(150)   ENCODE lzo
	,attribute4 VARCHAR(150)   ENCODE lzo
	,attribute5 VARCHAR(150)   ENCODE lzo
	,attribute6 VARCHAR(150)   ENCODE lzo
	,attribute7 VARCHAR(150)   ENCODE lzo
	,attribute9 VARCHAR(150)   ENCODE lzo
	,attribute10 VARCHAR(150)   ENCODE lzo
	,attribute11 VARCHAR(150)   ENCODE lzo
	,attribute12 VARCHAR(150)   ENCODE lzo
	,attribute13 VARCHAR(150)   ENCODE lzo
	,attribute14 VARCHAR(150)   ENCODE lzo
	,attribute15 VARCHAR(150)   ENCODE lzo
	,operating_unit_name VARCHAR(240)   ENCODE lzo
	,country VARCHAR(60)   ENCODE lzo
	,state VARCHAR(150)   ENCODE lzo
	,city VARCHAR(60)   ENCODE lzo
	,postal_code VARCHAR(60)   ENCODE lzo
	,shipping_control VARCHAR(14)   ENCODE lzo
	,KCA_OPERATION VARCHAR(10)   ENCODE lzo
    ,IS_DELETED_FLG VARCHAR(2) ENCODE lzo
	,kca_seq_id NUMERIC(36,0)   ENCODE az64
	,kca_seq_date TIMESTAMP WITHOUT TIME ZONE   ENCODE az64
)
DISTSTYLE
auto;

INSERT INTO bec_ods.MSC_TRADING_PARTNER_SITES   (
   partner_site_id,
	sr_tp_site_id,
	sr_instance_id,
	partner_id,
	partner_address,
	tp_site_code,
	sr_tp_id,
	"location",
	partner_type,
	deleted_flag,
	longitude,
	latitude,
	refresh_number,
	last_update_date,
	last_updated_by,
	creation_date,
	created_by,
	last_update_login,
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
	attribute9,
	attribute10,
	attribute11,
	attribute12,
	attribute13,
	attribute14,
	attribute15,
	operating_unit_name,
	country,
	state,
	city,
	postal_code,
	shipping_control,
	kca_operation,
	IS_DELETED_FLG,
	kca_seq_id,
	kca_seq_date)
    SELECT
    partner_site_id,
	sr_tp_site_id,
	sr_instance_id,
	partner_id,
	partner_address,
	tp_site_code,
	sr_tp_id,
	"location",
	partner_type,
	deleted_flag,
	longitude,
	latitude,
	refresh_number,
	last_update_date,
	last_updated_by,
	creation_date,
	created_by,
	last_update_login,
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
	attribute9,
	attribute10,
	attribute11,
	attribute12,
	attribute13,
	attribute14,
	attribute15,
	operating_unit_name,
	country,
	state,
	city,
	postal_code,
	shipping_control,
		KCA_OPERATION,
		'N' as IS_DELETED_FLG,
		cast(NULLIF(KCA_SEQ_ID,'') as numeric(36,0)) as KCA_SEQ_ID,
	kca_seq_date
    FROM
        bec_ods_stg.MSC_TRADING_PARTNER_SITES  ;

end;		

UPDATE bec_etl_ctrl.batch_ods_info
SET
    load_type = 'I',
    last_refresh_date = getdate()
WHERE
    ods_table_name = 'msc_trading_partner_sites';
	
commit;