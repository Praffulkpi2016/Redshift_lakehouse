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

DROP TABLE if exists bec_ods.chv_bucket_patterns;

CREATE TABLE IF NOT EXISTS bec_ods.chv_bucket_patterns
(
	bucket_pattern_id NUMERIC(15,0)   ENCODE az64
	,bucket_pattern_name VARCHAR(25)   ENCODE lzo
	,number_daily_buckets NUMERIC(15,0)   ENCODE az64
	,number_weekly_buckets NUMERIC(15,0)   ENCODE az64
	,number_monthly_buckets NUMERIC(15,0)   ENCODE az64
	,number_quarterly_buckets NUMERIC(15,0)   ENCODE az64
	,last_update_date TIMESTAMP WITHOUT TIME ZONE   ENCODE az64
	,last_updated_by NUMERIC(15,0)   ENCODE az64
	,creation_date TIMESTAMP WITHOUT TIME ZONE   ENCODE az64
	,created_by NUMERIC(15,0)   ENCODE az64
	,week_start_day VARCHAR(25)   ENCODE lzo
	,description VARCHAR(80)   ENCODE lzo
	,inactive_date TIMESTAMP WITHOUT TIME ZONE   ENCODE az64
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
	,last_update_login NUMERIC(15,0)   ENCODE az64
	,request_id NUMERIC(15,0)   ENCODE az64
	,program_application_id NUMERIC(15,0)   ENCODE az64
	,program_id NUMERIC(15,0)   ENCODE az64
	,program_update_date TIMESTAMP WITHOUT TIME ZONE   ENCODE az64
	,KCA_OPERATION VARCHAR(10)   ENCODE lzo
    ,IS_DELETED_FLG VARCHAR(2) ENCODE lzo
	,kca_seq_id NUMERIC(36,0)   ENCODE az64
	,kca_seq_date TIMESTAMP WITHOUT TIME ZONE ENCODE az64
)
DISTSTYLE
auto;

INSERT INTO bec_ods.chv_bucket_patterns (
	BUCKET_PATTERN_ID
,      BUCKET_PATTERN_NAME
,      NUMBER_DAILY_BUCKETS
,      NUMBER_WEEKLY_BUCKETS
,      NUMBER_MONTHLY_BUCKETS
,      NUMBER_QUARTERLY_BUCKETS
,      LAST_UPDATE_DATE
,      LAST_UPDATED_BY
,      CREATION_DATE
,      CREATED_BY
,      WEEK_START_DAY
,      DESCRIPTION
,      INACTIVE_DATE
,      ATTRIBUTE_CATEGORY
,      ATTRIBUTE1
,      ATTRIBUTE2
,      ATTRIBUTE3
,      ATTRIBUTE4
,      ATTRIBUTE5
,      ATTRIBUTE6
,      ATTRIBUTE7
,      ATTRIBUTE8
,      ATTRIBUTE9
,      ATTRIBUTE10
,      ATTRIBUTE11
,      ATTRIBUTE12
,      ATTRIBUTE13
,      ATTRIBUTE14
,      ATTRIBUTE15
,      LAST_UPDATE_LOGIN
,      REQUEST_ID
,      PROGRAM_APPLICATION_ID
,      PROGRAM_ID
,      PROGRAM_UPDATE_DATE
,	KCA_OPERATION,
	IS_DELETED_FLG,
	kca_seq_id,
	kca_seq_date
)
    SELECT
		BUCKET_PATTERN_ID
,      BUCKET_PATTERN_NAME
,      NUMBER_DAILY_BUCKETS
,      NUMBER_WEEKLY_BUCKETS
,      NUMBER_MONTHLY_BUCKETS
,      NUMBER_QUARTERLY_BUCKETS
,      LAST_UPDATE_DATE
,      LAST_UPDATED_BY
,      CREATION_DATE
,      CREATED_BY
,      WEEK_START_DAY
,      DESCRIPTION
,      INACTIVE_DATE
,      ATTRIBUTE_CATEGORY
,      ATTRIBUTE1
,      ATTRIBUTE2
,      ATTRIBUTE3
,      ATTRIBUTE4
,      ATTRIBUTE5
,      ATTRIBUTE6
,      ATTRIBUTE7
,      ATTRIBUTE8
,      ATTRIBUTE9
,      ATTRIBUTE10
,      ATTRIBUTE11
,      ATTRIBUTE12
,      ATTRIBUTE13
,      ATTRIBUTE14
,      ATTRIBUTE15
,      LAST_UPDATE_LOGIN
,      REQUEST_ID
,      PROGRAM_APPLICATION_ID
,      PROGRAM_ID
,      PROGRAM_UPDATE_DATE
		,KCA_OPERATION,
		'N' as IS_DELETED_FLG,
		cast(NULLIF(KCA_SEQ_ID,'') as numeric(36,0)) as KCA_SEQ_ID,
		kca_seq_date
    FROM
        bec_ods_stg.chv_bucket_patterns;

end;		

UPDATE bec_etl_ctrl.batch_ods_info
SET
    load_type = 'I',
    last_refresh_date = getdate()
WHERE
    ods_table_name = 'chv_bucket_patterns';
	
commit;