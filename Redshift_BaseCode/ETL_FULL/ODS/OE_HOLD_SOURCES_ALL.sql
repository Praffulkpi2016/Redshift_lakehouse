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

DROP TABLE if exists bec_ods.OE_HOLD_SOURCES_ALL;

CREATE TABLE IF NOT EXISTS bec_ods.OE_HOLD_SOURCES_ALL
(
	 
	 hold_source_id NUMERIC(15,0)   ENCODE az64
	,last_update_date TIMESTAMP WITHOUT TIME ZONE   ENCODE az64
	,last_updated_by NUMERIC(15,0)   ENCODE az64
	,last_update_login NUMERIC(15,0)   ENCODE az64
	,creation_date TIMESTAMP WITHOUT TIME ZONE   ENCODE az64
	,created_by NUMERIC(15,0)   ENCODE az64
	,program_application_id NUMERIC(15,0)   ENCODE az64
	,program_id NUMERIC(15,0)   ENCODE az64
	,program_update_date TIMESTAMP WITHOUT TIME ZONE   ENCODE az64
	,request_id NUMERIC(15,0)   ENCODE az64
	,hold_id NUMERIC(15,0)   ENCODE az64
	,hold_entity_code VARCHAR(30)   ENCODE lzo
	,hold_entity_id VARCHAR(250)   ENCODE lzo
	,hold_entity_code2 VARCHAR(30)   ENCODE lzo
	,hold_entity_id2 VARCHAR(250)   ENCODE lzo
	,hold_until_date TIMESTAMP WITHOUT TIME ZONE   ENCODE az64
	,released_flag VARCHAR(1)   ENCODE lzo
	,hold_comment VARCHAR(2000)   ENCODE lzo
	,org_id NUMERIC(15,0)   ENCODE az64
	,context VARCHAR(30)   ENCODE lzo
	,attribute1 VARCHAR(240)   ENCODE lzo
	,attribute2 VARCHAR(240)   ENCODE lzo
	,attribute3 VARCHAR(240)   ENCODE lzo
	,attribute4 VARCHAR(240)   ENCODE lzo
	,attribute5 VARCHAR(240)   ENCODE lzo
	,attribute6 VARCHAR(240)   ENCODE lzo
	,attribute7 VARCHAR(240)   ENCODE lzo
	,attribute8 VARCHAR(240)   ENCODE lzo
	,attribute9 VARCHAR(240)   ENCODE lzo
	,attribute10 VARCHAR(240)   ENCODE lzo
	,attribute11 VARCHAR(240)   ENCODE lzo
	,attribute12 VARCHAR(240)   ENCODE lzo
	,attribute13 VARCHAR(240)   ENCODE lzo
	,attribute14 VARCHAR(240)   ENCODE lzo
	,attribute15 VARCHAR(240)   ENCODE lzo
	,hold_release_id NUMERIC(15,0)   ENCODE az64,
	 KCA_OPERATION VARCHAR(10)   ENCODE lzo,
     IS_DELETED_FLG VARCHAR(2) ENCODE lzo, 
	 kca_seq_id NUMERIC(36,0)   ENCODE az64
	 ,kca_seq_date TIMESTAMP WITHOUT TIME ZONE   ENCODE az64
)
DISTSTYLE
auto;

INSERT INTO bec_ods.OE_HOLD_SOURCES_ALL (
	
	HOLD_SOURCE_ID
    ,LAST_UPDATE_DATE
    ,LAST_UPDATED_BY
    ,LAST_UPDATE_LOGIN
    ,CREATION_DATE
    ,CREATED_BY
    ,PROGRAM_APPLICATION_ID
    ,PROGRAM_ID
    ,PROGRAM_UPDATE_DATE
    ,REQUEST_ID
    ,HOLD_ID
    ,HOLD_ENTITY_CODE
    ,HOLD_ENTITY_ID
    ,HOLD_ENTITY_CODE2
    ,HOLD_ENTITY_ID2
    ,HOLD_UNTIL_DATE
    ,RELEASED_FLAG
    ,HOLD_COMMENT
    ,ORG_ID
    ,CONTEXT
    ,ATTRIBUTE1
    ,ATTRIBUTE2
    ,ATTRIBUTE3
    ,ATTRIBUTE4
    ,ATTRIBUTE5
    ,ATTRIBUTE6
    ,ATTRIBUTE7
    ,ATTRIBUTE8
    ,ATTRIBUTE9
    ,ATTRIBUTE10
    ,ATTRIBUTE11
    ,ATTRIBUTE12
    ,ATTRIBUTE13
    ,ATTRIBUTE14
    ,ATTRIBUTE15
    ,HOLD_RELEASE_ID, 
	KCA_OPERATION,
	IS_DELETED_FLG,
	kca_seq_id
	,kca_seq_date
)
    SELECT
		HOLD_SOURCE_ID
       ,LAST_UPDATE_DATE
       ,LAST_UPDATED_BY
       ,LAST_UPDATE_LOGIN
       ,CREATION_DATE
       ,CREATED_BY
       ,PROGRAM_APPLICATION_ID
       ,PROGRAM_ID
       ,PROGRAM_UPDATE_DATE
       ,REQUEST_ID
       ,HOLD_ID
       ,HOLD_ENTITY_CODE
       ,HOLD_ENTITY_ID
       ,HOLD_ENTITY_CODE2
       ,HOLD_ENTITY_ID2
       ,HOLD_UNTIL_DATE
       ,RELEASED_FLAG
       ,HOLD_COMMENT
       ,ORG_ID
       ,CONTEXT
       ,ATTRIBUTE1
       ,ATTRIBUTE2
       ,ATTRIBUTE3
       ,ATTRIBUTE4
       ,ATTRIBUTE5
       ,ATTRIBUTE6
       ,ATTRIBUTE7
       ,ATTRIBUTE8
       ,ATTRIBUTE9
       ,ATTRIBUTE10
       ,ATTRIBUTE11
       ,ATTRIBUTE12
       ,ATTRIBUTE13
       ,ATTRIBUTE14
       ,ATTRIBUTE15
       ,HOLD_RELEASE_ID, 
		KCA_OPERATION,
		'N' as IS_DELETED_FLG,
		cast(NULLIF(KCA_SEQ_ID,'') as numeric(36,0)) as KCA_SEQ_ID
		,kca_seq_date
    FROM
        bec_ods_stg.OE_HOLD_SOURCES_ALL;

end;		

UPDATE bec_etl_ctrl.batch_ods_info
SET
    load_type = 'I',
    last_refresh_date = getdate()
WHERE
    ods_table_name = 'oe_hold_sources_all';
	
commit;