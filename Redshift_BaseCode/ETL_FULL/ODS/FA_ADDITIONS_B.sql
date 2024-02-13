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

DROP TABLE if exists bec_ods.FA_ADDITIONS_B;

CREATE TABLE IF NOT EXISTS bec_ods.FA_ADDITIONS_B
(
	ASSET_ID NUMERIC(15,0) ENCODE az64 
	,ASSET_NUMBER VARCHAR(15) ENCODE lzo 
	,ASSET_KEY_CCID NUMERIC(15,0) ENCODE az64 
	,CURRENT_UNITS NUMERIC(15,0) ENCODE az64 
	,ASSET_TYPE VARCHAR(11) ENCODE lzo 
	,TAG_NUMBER VARCHAR(15) ENCODE lzo 
	,ASSET_CATEGORY_ID NUMERIC(15,0) ENCODE az64 
	,PARENT_ASSET_ID NUMERIC(15,0) ENCODE az64 
	,MANUFACTURER_NAME VARCHAR(360) ENCODE lzo 
	,SERIAL_NUMBER VARCHAR(35) ENCODE lzo 
	,MODEL_NUMBER VARCHAR(40) ENCODE lzo 
	,PROPERTY_TYPE_CODE VARCHAR(10) ENCODE lzo 
	,PROPERTY_1245_1250_CODE VARCHAR(4) ENCODE lzo 
	,IN_USE_FLAG VARCHAR(3) ENCODE lzo 
	,OWNED_LEASED VARCHAR(15) ENCODE lzo 
	,NEW_USED VARCHAR(4) ENCODE lzo 
	,UNIT_ADJUSTMENT_FLAG VARCHAR(3) ENCODE lzo 
	,ADD_COST_JE_FLAG VARCHAR(3) ENCODE lzo 
	,ATTRIBUTE1 VARCHAR(150) ENCODE lzo 
	,ATTRIBUTE2 VARCHAR(150) ENCODE lzo 
	,ATTRIBUTE3 VARCHAR(150) ENCODE lzo 
	,ATTRIBUTE4 VARCHAR(150) ENCODE lzo 
	,ATTRIBUTE5 VARCHAR(150) ENCODE lzo 
	,ATTRIBUTE6 VARCHAR(150) ENCODE lzo 
	,ATTRIBUTE7 VARCHAR(150) ENCODE lzo 
	,ATTRIBUTE8 VARCHAR(150) ENCODE lzo 
	,ATTRIBUTE9 VARCHAR(150) ENCODE lzo 
	,ATTRIBUTE10 VARCHAR(150) ENCODE lzo 
	,ATTRIBUTE11 VARCHAR(150) ENCODE lzo 
	,ATTRIBUTE12 VARCHAR(150) ENCODE lzo 
	,ATTRIBUTE13 VARCHAR(150) ENCODE lzo 
	,ATTRIBUTE14 VARCHAR(150) ENCODE lzo 
	,ATTRIBUTE15 VARCHAR(150) ENCODE lzo 
	,ATTRIBUTE16 VARCHAR(150) ENCODE lzo 
	,ATTRIBUTE17 VARCHAR(150) ENCODE lzo 
	,ATTRIBUTE18 VARCHAR(150) ENCODE lzo 
	,ATTRIBUTE19 VARCHAR(150) ENCODE lzo 
	,ATTRIBUTE20 VARCHAR(150) ENCODE lzo 
	,ATTRIBUTE21 VARCHAR(150) ENCODE lzo 
	,ATTRIBUTE22 VARCHAR(150) ENCODE lzo 
	,ATTRIBUTE23 VARCHAR(150) ENCODE lzo 
	,ATTRIBUTE24 VARCHAR(150) ENCODE lzo 
	,ATTRIBUTE25 VARCHAR(150) ENCODE lzo 
	,ATTRIBUTE26 VARCHAR(150) ENCODE lzo 
	,ATTRIBUTE27 VARCHAR(150) ENCODE lzo 
	,ATTRIBUTE28 VARCHAR(150) ENCODE lzo 
	,ATTRIBUTE29 VARCHAR(150) ENCODE lzo 
	,ATTRIBUTE30 VARCHAR(150) ENCODE lzo 
	,ATTRIBUTE_CATEGORY_CODE VARCHAR(210) ENCODE lzo 
	,CONTEXT VARCHAR(210) ENCODE lzo 
	,LEASE_ID NUMERIC(15,0) ENCODE az64 
	,INVENTORIAL VARCHAR(3) ENCODE lzo 
	,LAST_UPDATE_DATE TIMESTAMP WITHOUT TIME ZONE ENCODE az64 
	,LAST_UPDATED_BY NUMERIC(15,0) ENCODE az64 
	,CREATED_BY NUMERIC(15,0) ENCODE az64 
	,CREATION_DATE TIMESTAMP WITHOUT TIME ZONE ENCODE az64 
	,LAST_UPDATE_LOGIN NUMERIC(15,0) ENCODE az64 
	,GLOBAL_ATTRIBUTE1 VARCHAR(150) ENCODE lzo 
	,GLOBAL_ATTRIBUTE2 VARCHAR(150) ENCODE lzo 
	,GLOBAL_ATTRIBUTE3 VARCHAR(150) ENCODE lzo 
	,GLOBAL_ATTRIBUTE4 VARCHAR(150) ENCODE lzo 
	,GLOBAL_ATTRIBUTE5 VARCHAR(150) ENCODE lzo 
	,GLOBAL_ATTRIBUTE6 VARCHAR(150) ENCODE lzo 
	,GLOBAL_ATTRIBUTE7 VARCHAR(150) ENCODE lzo 
	,GLOBAL_ATTRIBUTE8 VARCHAR(150) ENCODE lzo 
	,GLOBAL_ATTRIBUTE9 VARCHAR(150) ENCODE lzo 
	,GLOBAL_ATTRIBUTE10 VARCHAR(150) ENCODE lzo 
	,GLOBAL_ATTRIBUTE11 VARCHAR(150) ENCODE lzo 
	,GLOBAL_ATTRIBUTE12 VARCHAR(150) ENCODE lzo 
	,GLOBAL_ATTRIBUTE13 VARCHAR(150) ENCODE lzo 
	,GLOBAL_ATTRIBUTE14 VARCHAR(150) ENCODE lzo 
	,GLOBAL_ATTRIBUTE15 VARCHAR(150) ENCODE lzo 
	,GLOBAL_ATTRIBUTE16 VARCHAR(150) ENCODE lzo 
	,GLOBAL_ATTRIBUTE17 VARCHAR(150) ENCODE lzo 
	,GLOBAL_ATTRIBUTE18 VARCHAR(150) ENCODE lzo 
	,GLOBAL_ATTRIBUTE19 VARCHAR(150) ENCODE lzo 
	,GLOBAL_ATTRIBUTE20 VARCHAR(150) ENCODE lzo 
	,GLOBAL_ATTRIBUTE_CATEGORY VARCHAR(30) ENCODE lzo 
	,COMMITMENT VARCHAR(30) ENCODE lzo 
	,INVESTMENT_LAW VARCHAR(30) ENCODE lzo 
	,KCA_OPERATION VARCHAR(10)   ENCODE lzo
    ,IS_DELETED_FLG VARCHAR(2) ENCODE lzo
	,kca_seq_id NUMERIC(36,0)   ENCODE az64
	,kca_seq_date TIMESTAMP WITHOUT TIME ZONE ENCODE az64
)
DISTSTYLE
auto;

INSERT INTO bec_ods.FA_ADDITIONS_B (
    ASSET_ID, 
	ASSET_NUMBER, 
	ASSET_KEY_CCID, 
	CURRENT_UNITS, 
	ASSET_TYPE, 
	TAG_NUMBER, 
	ASSET_CATEGORY_ID, 
	PARENT_ASSET_ID, 
	MANUFACTURER_NAME, 
	SERIAL_NUMBER, 
	MODEL_NUMBER, 
	PROPERTY_TYPE_CODE, 
	PROPERTY_1245_1250_CODE, 
	IN_USE_FLAG, 
	OWNED_LEASED, 
	NEW_USED, 
	UNIT_ADJUSTMENT_FLAG, 
	ADD_COST_JE_FLAG, 
	ATTRIBUTE1, 
	ATTRIBUTE2, 
	ATTRIBUTE3, 
	ATTRIBUTE4, 
	ATTRIBUTE5, 
	ATTRIBUTE6, 
	ATTRIBUTE7, 
	ATTRIBUTE8, 
	ATTRIBUTE9, 
	ATTRIBUTE10, 
	ATTRIBUTE11, 
	ATTRIBUTE12, 
	ATTRIBUTE13, 
	ATTRIBUTE14, 
	ATTRIBUTE15, 
	ATTRIBUTE16, 
	ATTRIBUTE17, 
	ATTRIBUTE18, 
	ATTRIBUTE19, 
	ATTRIBUTE20, 
	ATTRIBUTE21, 
	ATTRIBUTE22, 
	ATTRIBUTE23, 
	ATTRIBUTE24, 
	ATTRIBUTE25, 
	ATTRIBUTE26, 
	ATTRIBUTE27, 
	ATTRIBUTE28, 
	ATTRIBUTE29, 
	ATTRIBUTE30, 
	ATTRIBUTE_CATEGORY_CODE, 
	CONTEXT, 
	LEASE_ID, 
	INVENTORIAL, 
	LAST_UPDATE_DATE, 
	LAST_UPDATED_BY, 
	CREATED_BY, 
	CREATION_DATE, 
	LAST_UPDATE_LOGIN, 
	GLOBAL_ATTRIBUTE1, 
	GLOBAL_ATTRIBUTE2, 
	GLOBAL_ATTRIBUTE3, 
	GLOBAL_ATTRIBUTE4, 
	GLOBAL_ATTRIBUTE5, 
	GLOBAL_ATTRIBUTE6, 
	GLOBAL_ATTRIBUTE7, 
	GLOBAL_ATTRIBUTE8, 
	GLOBAL_ATTRIBUTE9, 
	GLOBAL_ATTRIBUTE10, 
	GLOBAL_ATTRIBUTE11, 
	GLOBAL_ATTRIBUTE12, 
	GLOBAL_ATTRIBUTE13, 
	GLOBAL_ATTRIBUTE14, 
	GLOBAL_ATTRIBUTE15, 
	GLOBAL_ATTRIBUTE16, 
	GLOBAL_ATTRIBUTE17, 
	GLOBAL_ATTRIBUTE18, 
	GLOBAL_ATTRIBUTE19, 
	GLOBAL_ATTRIBUTE20, 
	GLOBAL_ATTRIBUTE_CATEGORY, 
	COMMITMENT, 
	INVESTMENT_LAW, 
	KCA_OPERATION,
	IS_DELETED_FLG,
	kca_seq_id,
	kca_seq_date
)
    SELECT
        ASSET_ID, 
		ASSET_NUMBER, 
		ASSET_KEY_CCID, 
		CURRENT_UNITS, 
		ASSET_TYPE, 
		TAG_NUMBER, 
		ASSET_CATEGORY_ID, 
		PARENT_ASSET_ID, 
		MANUFACTURER_NAME, 
		SERIAL_NUMBER, 
		MODEL_NUMBER, 
		PROPERTY_TYPE_CODE, 
		PROPERTY_1245_1250_CODE, 
		IN_USE_FLAG, 
		OWNED_LEASED, 
		NEW_USED, 
		UNIT_ADJUSTMENT_FLAG, 
		ADD_COST_JE_FLAG, 
		ATTRIBUTE1, 
		ATTRIBUTE2, 
		ATTRIBUTE3, 
		ATTRIBUTE4, 
		ATTRIBUTE5, 
		ATTRIBUTE6, 
		ATTRIBUTE7, 
		ATTRIBUTE8, 
		ATTRIBUTE9, 
		ATTRIBUTE10, 
		ATTRIBUTE11, 
		ATTRIBUTE12, 
		ATTRIBUTE13, 
		ATTRIBUTE14, 
		ATTRIBUTE15, 
		ATTRIBUTE16, 
		ATTRIBUTE17, 
		ATTRIBUTE18, 
		ATTRIBUTE19, 
		ATTRIBUTE20, 
		ATTRIBUTE21, 
		ATTRIBUTE22, 
		ATTRIBUTE23, 
		ATTRIBUTE24, 
		ATTRIBUTE25, 
		ATTRIBUTE26, 
		ATTRIBUTE27, 
		ATTRIBUTE28, 
		ATTRIBUTE29, 
		ATTRIBUTE30, 
		ATTRIBUTE_CATEGORY_CODE, 
		CONTEXT, 
		LEASE_ID, 
		INVENTORIAL, 
		LAST_UPDATE_DATE, 
		LAST_UPDATED_BY, 
		CREATED_BY, 
		CREATION_DATE, 
		LAST_UPDATE_LOGIN, 
		GLOBAL_ATTRIBUTE1, 
		GLOBAL_ATTRIBUTE2, 
		GLOBAL_ATTRIBUTE3, 
		GLOBAL_ATTRIBUTE4, 
		GLOBAL_ATTRIBUTE5, 
		GLOBAL_ATTRIBUTE6, 
		GLOBAL_ATTRIBUTE7, 
		GLOBAL_ATTRIBUTE8, 
		GLOBAL_ATTRIBUTE9, 
		GLOBAL_ATTRIBUTE10, 
		GLOBAL_ATTRIBUTE11, 
		GLOBAL_ATTRIBUTE12, 
		GLOBAL_ATTRIBUTE13, 
		GLOBAL_ATTRIBUTE14, 
		GLOBAL_ATTRIBUTE15, 
		GLOBAL_ATTRIBUTE16, 
		GLOBAL_ATTRIBUTE17, 
		GLOBAL_ATTRIBUTE18, 
		GLOBAL_ATTRIBUTE19, 
		GLOBAL_ATTRIBUTE20, 
		GLOBAL_ATTRIBUTE_CATEGORY, 
		COMMITMENT, 
		INVESTMENT_LAW, 
		KCA_OPERATION,
		'N' as IS_DELETED_FLG,
		cast(NULLIF(KCA_SEQ_ID,'') as numeric(36,0)) as KCA_SEQ_ID,
		kca_seq_date
    FROM
        bec_ods_stg.FA_ADDITIONS_B;

end;		

UPDATE bec_etl_ctrl.batch_ods_info
SET
    load_type = 'I',
    last_refresh_date = getdate()
WHERE
    ods_table_name = 'fa_additions_b';
	
commit;