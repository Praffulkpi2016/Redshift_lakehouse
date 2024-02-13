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

DROP TABLE if exists bec_ods.FA_CATEGORIES_B;

CREATE TABLE IF NOT EXISTS bec_ods.FA_CATEGORIES_B
(
	CATEGORY_ID NUMERIC(15,0) ENCODE az64 
	,SUMMARY_FLAG VARCHAR(1) ENCODE lzo 
	,ENABLED_FLAG VARCHAR(1) ENCODE lzo 
	,OWNED_LEASED VARCHAR(6) ENCODE lzo 
	,PRODUCTION_CAPACITY NUMERIC(15,0) ENCODE az64 
	,LAST_UPDATE_DATE TIMESTAMP WITHOUT TIME ZONE ENCODE az64 
	,LAST_UPDATED_BY NUMERIC(15,0) ENCODE az64 
	,CATEGORY_TYPE VARCHAR(30) ENCODE lzo 
	,CAPITALIZE_FLAG VARCHAR(3) ENCODE lzo 
	,SEGMENT1 VARCHAR(30) ENCODE lzo 
	,SEGMENT2 VARCHAR(30) ENCODE lzo 
	,SEGMENT3 VARCHAR(30) ENCODE lzo 
	,SEGMENT4 VARCHAR(30) ENCODE lzo 
	,SEGMENT5 VARCHAR(30) ENCODE lzo 
	,SEGMENT6 VARCHAR(30) ENCODE lzo 
	,SEGMENT7 VARCHAR(30) ENCODE lzo 
	,START_DATE_ACTIVE TIMESTAMP WITHOUT TIME ZONE ENCODE az64 
	,END_DATE_ACTIVE TIMESTAMP WITHOUT TIME ZONE ENCODE az64 
	,PROPERTY_TYPE_CODE VARCHAR(10) ENCODE lzo 
	,PROPERTY_1245_1250_CODE VARCHAR(4) ENCODE lzo 
	,DATE_INEFFECTIVE TIMESTAMP WITHOUT TIME ZONE ENCODE az64 
	,INVENTORIAL VARCHAR(3) ENCODE lzo 
	,CREATED_BY NUMERIC(15,0) ENCODE az64 
	,CREATION_DATE TIMESTAMP WITHOUT TIME ZONE ENCODE az64 
	,LAST_UPDATE_LOGIN NUMERIC(15,0) ENCODE az64 
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
	,ATTRIBUTE_CATEGORY_CODE VARCHAR(30) ENCODE lzo 
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
	,KCA_OPERATION VARCHAR(10)   ENCODE lzo
    ,IS_DELETED_FLG VARCHAR(2) ENCODE lzo
	,kca_seq_id NUMERIC(36,0)   ENCODE az64
	,kca_seq_date TIMESTAMP WITHOUT TIME ZONE ENCODE az64
)
DISTSTYLE
auto;

INSERT INTO bec_ods.FA_CATEGORIES_B (
    CATEGORY_ID, 
	SUMMARY_FLAG, 
	ENABLED_FLAG, 
	OWNED_LEASED, 
	PRODUCTION_CAPACITY, 
	LAST_UPDATE_DATE, 
	LAST_UPDATED_BY, 
	CATEGORY_TYPE, 
	CAPITALIZE_FLAG, 
	SEGMENT1, 
	SEGMENT2, 
	SEGMENT3, 
	SEGMENT4, 
	SEGMENT5, 
	SEGMENT6, 
	SEGMENT7, 
	START_DATE_ACTIVE, 
	END_DATE_ACTIVE, 
	PROPERTY_TYPE_CODE, 
	PROPERTY_1245_1250_CODE, 
	DATE_INEFFECTIVE, 
	INVENTORIAL, 
	CREATED_BY, 
	CREATION_DATE, 
	LAST_UPDATE_LOGIN, 
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
	ATTRIBUTE_CATEGORY_CODE, 
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
	KCA_OPERATION,
	IS_DELETED_FLG,
	kca_seq_id,
	kca_seq_date
)
    SELECT
        CATEGORY_ID, 
		SUMMARY_FLAG, 
		ENABLED_FLAG, 
		OWNED_LEASED, 
		PRODUCTION_CAPACITY, 
		LAST_UPDATE_DATE, 
		LAST_UPDATED_BY, 
		CATEGORY_TYPE, 
		CAPITALIZE_FLAG, 
		SEGMENT1, 
		SEGMENT2, 
		SEGMENT3, 
		SEGMENT4, 
		SEGMENT5, 
		SEGMENT6, 
		SEGMENT7, 
		START_DATE_ACTIVE, 
		END_DATE_ACTIVE, 
		PROPERTY_TYPE_CODE, 
		PROPERTY_1245_1250_CODE, 
		DATE_INEFFECTIVE, 
		INVENTORIAL, 
		CREATED_BY, 
		CREATION_DATE, 
		LAST_UPDATE_LOGIN, 
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
		ATTRIBUTE_CATEGORY_CODE, 
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
		KCA_OPERATION,
		'N' as IS_DELETED_FLG,
		cast(NULLIF(KCA_SEQ_ID,'') as numeric(36,0)) as KCA_SEQ_ID,
		kca_seq_date
    FROM
        bec_ods_stg.FA_CATEGORIES_B;

end;		

UPDATE bec_etl_ctrl.batch_ods_info
SET
    load_type = 'I',
    last_refresh_date = getdate()
WHERE
    ods_table_name = 'fa_categories_b';
	
commit;