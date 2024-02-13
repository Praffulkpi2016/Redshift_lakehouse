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

DROP TABLE if exists bec_ods.GL_CURRENCIES;

CREATE TABLE IF NOT EXISTS bec_ods.GL_CURRENCIES		
(CURRENCY_CODE	VARCHAR(15)   ENCODE lzo
,LAST_UPDATE_DATE	TIMESTAMP WITHOUT TIME ZONE   ENCODE az64
,LAST_UPDATED_BY	NUMERIC(15,0)   ENCODE az64
,CREATION_DATE	TIMESTAMP WITHOUT TIME ZONE   ENCODE az64
,CREATED_BY	NUMERIC(15,0)   ENCODE az64
,LAST_UPDATE_LOGIN	NUMERIC(15,0)   ENCODE az64
,ENABLED_FLAG	VARCHAR(1)   ENCODE lzo
,CURRENCY_FLAG	VARCHAR(1)   ENCODE lzo
,DESCRIPTION	VARCHAR(240)   ENCODE lzo
,ISSUING_TERRITORY_CODE	VARCHAR(2)   ENCODE lzo
,PRECISION	NUMERIC(1,0)   ENCODE az64
,EXTENDED_PRECISION	NUMERIC(2,0)   ENCODE az64
,SYMBOL	VARCHAR(12)   ENCODE lzo
,START_DATE_ACTIVE	TIMESTAMP WITHOUT TIME ZONE   ENCODE az64
,END_DATE_ACTIVE	TIMESTAMP WITHOUT TIME ZONE   ENCODE az64
,MINIMUM_ACCOUNTABLE_UNIT	NUMERIC(15,0)   ENCODE az64
,CONTEXT	VARCHAR(80)   ENCODE lzo
,ISO_FLAG	VARCHAR(1)   ENCODE lzo
,ATTRIBUTE1	VARCHAR(150)   ENCODE lzo
,ATTRIBUTE2	VARCHAR(150)   ENCODE lzo
,ATTRIBUTE3	VARCHAR(150)   ENCODE lzo
,ATTRIBUTE4	VARCHAR(150)   ENCODE lzo
,ATTRIBUTE5	VARCHAR(150)   ENCODE lzo
,ATTRIBUTE6	VARCHAR(150)   ENCODE lzo
,ATTRIBUTE7	VARCHAR(150)   ENCODE lzo
,ATTRIBUTE8	VARCHAR(150)   ENCODE lzo
,ATTRIBUTE9	VARCHAR(150)   ENCODE lzo
,ATTRIBUTE10	VARCHAR(150)   ENCODE lzo
,ATTRIBUTE11	VARCHAR(150)   ENCODE lzo
,ATTRIBUTE12	VARCHAR(150)   ENCODE lzo
,ATTRIBUTE13	VARCHAR(150)   ENCODE lzo
,ATTRIBUTE14	VARCHAR(150)   ENCODE lzo
,ATTRIBUTE15	VARCHAR(150)   ENCODE lzo
,GLOBAL_ATTRIBUTE_CATEGORY	VARCHAR(150)   ENCODE lzo
,GLOBAL_ATTRIBUTE1	VARCHAR(150)   ENCODE lzo
,GLOBAL_ATTRIBUTE2	VARCHAR(150)   ENCODE lzo
,GLOBAL_ATTRIBUTE3	VARCHAR(150)   ENCODE lzo
,GLOBAL_ATTRIBUTE4	VARCHAR(150)   ENCODE lzo
,GLOBAL_ATTRIBUTE5	VARCHAR(150)   ENCODE lzo
,GLOBAL_ATTRIBUTE6	VARCHAR(150)   ENCODE lzo
,GLOBAL_ATTRIBUTE7	VARCHAR(150)   ENCODE lzo
,GLOBAL_ATTRIBUTE8	VARCHAR(150)   ENCODE lzo
,GLOBAL_ATTRIBUTE9	VARCHAR(150)   ENCODE lzo
,GLOBAL_ATTRIBUTE10	VARCHAR(150)   ENCODE lzo
,GLOBAL_ATTRIBUTE11	VARCHAR(150)   ENCODE lzo
,GLOBAL_ATTRIBUTE12	VARCHAR(150)   ENCODE lzo
,GLOBAL_ATTRIBUTE13	VARCHAR(150)   ENCODE lzo
,GLOBAL_ATTRIBUTE14	VARCHAR(150)   ENCODE lzo
,GLOBAL_ATTRIBUTE15	VARCHAR(150)   ENCODE lzo
,GLOBAL_ATTRIBUTE16	VARCHAR(150)   ENCODE lzo
,GLOBAL_ATTRIBUTE17	VARCHAR(150)   ENCODE lzo
,GLOBAL_ATTRIBUTE18	VARCHAR(150)   ENCODE lzo
,GLOBAL_ATTRIBUTE19	VARCHAR(150)   ENCODE lzo
,GLOBAL_ATTRIBUTE20	VARCHAR(150)   ENCODE lzo
,DERIVE_EFFECTIVE	TIMESTAMP WITHOUT TIME ZONE   ENCODE az64
,DERIVE_TYPE	VARCHAR(8)   ENCODE lzo
,DERIVE_FACTOR	NUMERIC(15,0)   ENCODE az64
,KCA_OPERATION VARCHAR(10)   ENCODE lzo
,IS_DELETED_FLG VARCHAR(2) ENCODE lzo
,kca_seq_id NUMERIC(36,0)   ENCODE az64
,kca_seq_date TIMESTAMP WITHOUT TIME ZONE   ENCODE az64  ) 
DISTSTYLE AUTO
;	

insert into bec_ods.GL_CURRENCIES
(
	currency_code,
	last_update_date,
	last_updated_by,
	creation_date,
	created_by,
	last_update_login,
	enabled_flag,
	currency_flag,
	description,
	issuing_territory_code,
	"precision",
	extended_precision,
	symbol,
	start_date_active,
	end_date_active,
	minimum_accountable_unit,
	context,
	iso_flag,
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
	derive_effective,
	derive_type,
	derive_factor
	,KCA_OPERATION
	,IS_DELETED_FLG	
	,kca_seq_id,
	kca_seq_date
) 
select
	currency_code,
	last_update_date,
	last_updated_by,
	creation_date,
	created_by,
	last_update_login,
	enabled_flag,
	currency_flag,
	description,
	issuing_territory_code,
	"precision",
	extended_precision,
	symbol,
	start_date_active,
	end_date_active,
	minimum_accountable_unit,
	context,
	iso_flag,
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
	derive_effective,
	derive_type,
	derive_factor
	,KCA_OPERATION
	,'N' as IS_DELETED_FLG
	,cast(NULLIF(KCA_SEQ_ID,'') as numeric(36,0)) as KCA_SEQ_ID
		,kca_seq_date
from
	bec_ods_stg.GL_CURRENCIES;

end;	
 
 update
	bec_etl_ctrl.batch_ods_info
set load_type = 'I',
	last_refresh_date = getdate()
where
	ods_table_name = 'gl_currencies'; 
	
commit;