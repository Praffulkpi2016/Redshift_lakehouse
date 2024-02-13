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

DROP TABLE if exists bec_ods.oe_hold_definitions;

CREATE TABLE IF NOT EXISTS bec_ods.oe_hold_definitions
(
	HOLD_ID NUMERIC(15,0) ENCODE az64 
	,CREATION_DATE TIMESTAMP WITHOUT TIME ZONE ENCODE az64  
	,CREATED_BY NUMERIC(15,0) ENCODE az64 
	,LAST_UPDATE_DATE TIMESTAMP WITHOUT TIME ZONE ENCODE az64  
	,LAST_UPDATED_BY NUMERIC(15,0) ENCODE az64 
	,LAST_UPDATE_LOGIN NUMERIC(15,0) ENCODE az64 	
	,NAME VARCHAR(240) ENCODE lzo 
	,TYPE_CODE VARCHAR(30) ENCODE lzo 
	,DESCRIPTION VARCHAR(2000) ENCODE lzo 
	,START_DATE_ACTIVE TIMESTAMP WITHOUT TIME ZONE ENCODE az64 
	,END_DATE_ACTIVE TIMESTAMP WITHOUT TIME ZONE ENCODE az64  
	,ITEM_TYPE VARCHAR(8) ENCODE lzo 
	,ACTIVITY_NAME VARCHAR(30) ENCODE lzo 
	,CONTEXT VARCHAR(30) ENCODE lzo 
	,ATTRIBUTE1 VARCHAR(240) ENCODE lzo 
	,ATTRIBUTE2 VARCHAR(240) ENCODE lzo 
	,ATTRIBUTE3 VARCHAR(240) ENCODE lzo 
	,ATTRIBUTE4 VARCHAR(240) ENCODE lzo 
	,ATTRIBUTE5 VARCHAR(240) ENCODE lzo 
	,ATTRIBUTE6 VARCHAR(240) ENCODE lzo 
	,ATTRIBUTE7 VARCHAR(240) ENCODE lzo 
	,ATTRIBUTE8 VARCHAR(240) ENCODE lzo 
	,ATTRIBUTE9 VARCHAR(240) ENCODE lzo 
	,ATTRIBUTE10 VARCHAR(240) ENCODE lzo 
	,ATTRIBUTE11 VARCHAR(240) ENCODE lzo 
	,ATTRIBUTE12 VARCHAR(240) ENCODE lzo 
	,ATTRIBUTE13 VARCHAR(240) ENCODE lzo 
	,ATTRIBUTE14 VARCHAR(240) ENCODE lzo 
	,ATTRIBUTE15 VARCHAR(240) ENCODE lzo	
	,HOLD_INCLUDED_ITEMS_FLAG VARCHAR(1) ENCODE lzo 
	,APPLY_TO_ORDER_AND_LINE_FLAG VARCHAR(1) ENCODE lzo  
	,PROGRESS_WF_ON_RELEASE_FLAG VARCHAR(1) ENCODE lzo  
	,ZD_EDITION_NAME VARCHAR(30) ENCODE lzo  
	,ZD_SYNC VARCHAR(30) ENCODE lzo 
	,KCA_OPERATION VARCHAR(10)   ENCODE lzo
    ,IS_DELETED_FLG VARCHAR(2) ENCODE lzo
	,kca_seq_id NUMERIC(36,0)   ENCODE az64
		,kca_seq_date TIMESTAMP WITHOUT TIME ZONE   ENCODE az64
)
DISTSTYLE
auto;

INSERT INTO bec_ods.oe_hold_definitions (
	HOLD_ID,
	CREATION_DATE,
	CREATED_BY,
	LAST_UPDATE_DATE,
	LAST_UPDATED_BY,
	LAST_UPDATE_LOGIN,
	NAME,
	TYPE_CODE,
	DESCRIPTION,
	START_DATE_ACTIVE,
	END_DATE_ACTIVE,
	ITEM_TYPE,
	ACTIVITY_NAME,
	CONTEXT,
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
	HOLD_INCLUDED_ITEMS_FLAG,
	APPLY_TO_ORDER_AND_LINE_FLAG,
	PROGRESS_WF_ON_RELEASE_FLAG,
	ZD_EDITION_NAME,
	ZD_SYNC, 
	KCA_OPERATION,
	IS_DELETED_FLG,
	kca_seq_id
	,kca_seq_date
)
    SELECT
		HOLD_ID,
		CREATION_DATE,
		CREATED_BY,
		LAST_UPDATE_DATE,
		LAST_UPDATED_BY,
		LAST_UPDATE_LOGIN,
		NAME,
		TYPE_CODE,
		DESCRIPTION,
		START_DATE_ACTIVE,
		END_DATE_ACTIVE,
		ITEM_TYPE,
		ACTIVITY_NAME,
		CONTEXT,
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
		HOLD_INCLUDED_ITEMS_FLAG,
		APPLY_TO_ORDER_AND_LINE_FLAG,
		PROGRESS_WF_ON_RELEASE_FLAG,
		ZD_EDITION_NAME,
		ZD_SYNC, 
		KCA_OPERATION,
		'N' as IS_DELETED_FLG,
		cast(NULLIF(KCA_SEQ_ID,'') as numeric(36,0)) as KCA_SEQ_ID
	,kca_seq_date		
    FROM
        bec_ods_stg.oe_hold_definitions;

end;		

UPDATE bec_etl_ctrl.batch_ods_info
SET
    load_type = 'I',
    last_refresh_date = getdate()
WHERE
    ods_table_name = 'oe_hold_definitions';
	
commit;