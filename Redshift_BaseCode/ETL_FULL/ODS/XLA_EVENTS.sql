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

DROP TABLE if exists bec_ods.XLA_EVENTS;

CREATE TABLE IF NOT EXISTS bec_ods.XLA_EVENTS
(
EVENT_ID	NUMERIC(15,0)   ENCODE az64
,APPLICATION_ID	NUMERIC(15,0)   ENCODE az64
,EVENT_TYPE_CODE	VARCHAR(30)
,EVENT_DATE TIMESTAMP WITHOUT TIME ZONE   ENCODE az64
,ENTITY_ID	NUMERIC(15,0)   ENCODE az64
,EVENT_STATUS_CODE	VARCHAR(1)  ENCODE lzo
,PROCESS_STATUS_CODE	VARCHAR(1)  ENCODE lzo
,REFERENCE_NUM_1	NUMERIC(15,0)   ENCODE az64
,REFERENCE_NUM_2	NUMERIC(15,0)   ENCODE az64
,REFERENCE_NUM_3	NUMERIC(15,0)   ENCODE az64
,REFERENCE_NUM_4	NUMERIC(15,0)   ENCODE az64
,REFERENCE_CHAR_1	VARCHAR(240)  ENCODE lzo
,REFERENCE_CHAR_2	VARCHAR(240)  ENCODE lzo
,REFERENCE_CHAR_3	VARCHAR(240)  ENCODE lzo
,REFERENCE_CHAR_4	VARCHAR(240)  ENCODE lzo
,REFERENCE_DATE_1 TIMESTAMP WITHOUT TIME ZONE   ENCODE az64
,REFERENCE_DATE_2 TIMESTAMP WITHOUT TIME ZONE   ENCODE az64
,REFERENCE_DATE_3 TIMESTAMP WITHOUT TIME ZONE   ENCODE az64
,REFERENCE_DATE_4 TIMESTAMP WITHOUT TIME ZONE   ENCODE az64
,EVENT_NUMBER	NUMERIC(38,0)   ENCODE az64
,ON_HOLD_FLAG	VARCHAR(1)  ENCODE lzo
,CREATION_DATE TIMESTAMP WITHOUT TIME ZONE   ENCODE az64
,CREATED_BY	NUMERIC(15,0)   ENCODE az64
,LAST_UPDATE_DATE TIMESTAMP WITHOUT TIME ZONE   ENCODE az64
,LAST_UPDATED_BY	NUMERIC(15,0)   ENCODE az64
,LAST_UPDATE_LOGIN	NUMERIC(15,0)   ENCODE az64
,PROGRAM_UPDATE_DATE TIMESTAMP WITHOUT TIME ZONE   ENCODE az64
,PROGRAM_APPLICATION_ID	NUMERIC(15,0)   ENCODE az64
,PROGRAM_ID	NUMERIC(15,0)   ENCODE az64
,REQUEST_ID	NUMERIC(15,0)   ENCODE az64
,UPG_BATCH_ID	NUMERIC(15,0)   ENCODE az64
,UPG_SOURCE_APPLICATION_ID	NUMERIC(15,0)   ENCODE az64
,UPG_VALID_FLAG	VARCHAR(1)  ENCODE lzo
,TRANSACTION_DATE TIMESTAMP WITHOUT TIME ZONE   ENCODE az64
,BUDGETARY_CONTROL_FLAG	VARCHAR(1)  ENCODE lzo
,MERGE_EVENT_SET_ID	NUMERIC(15,0)   ENCODE az64
,KCA_OPERATION VARCHAR(10)   ENCODE lzo
,IS_DELETED_FLG VARCHAR(2) ENCODE lzo
,KCA_SEQ_ID NUMERIC(36,0)   ENCODE az64
,kca_seq_date TIMESTAMP WITHOUT TIME ZONE   ENCODE az64
)
DISTSTYLE
auto;

INSERT INTO bec_ods.XLA_EVENTS
(
EVENT_ID
,APPLICATION_ID
,EVENT_TYPE_CODE
,EVENT_DATE
,ENTITY_ID
,EVENT_STATUS_CODE
,PROCESS_STATUS_CODE
,REFERENCE_NUM_1
,REFERENCE_NUM_2
,REFERENCE_NUM_3
,REFERENCE_NUM_4
,REFERENCE_CHAR_1
,REFERENCE_CHAR_2
,REFERENCE_CHAR_3
,REFERENCE_CHAR_4
,REFERENCE_DATE_1
,REFERENCE_DATE_2
,REFERENCE_DATE_3
,REFERENCE_DATE_4
,EVENT_NUMBER
,ON_HOLD_FLAG
,CREATION_DATE
,CREATED_BY
,LAST_UPDATE_DATE
,LAST_UPDATED_BY
,LAST_UPDATE_LOGIN
,PROGRAM_UPDATE_DATE
,PROGRAM_APPLICATION_ID
,PROGRAM_ID
,REQUEST_ID
,UPG_BATCH_ID
,UPG_SOURCE_APPLICATION_ID
,UPG_VALID_FLAG
,TRANSACTION_DATE
,BUDGETARY_CONTROL_FLAG
,MERGE_EVENT_SET_ID
,KCA_OPERATION
,IS_DELETED_FLG
,KCA_SEQ_ID
,kca_seq_date
)
SELECT
EVENT_ID
,APPLICATION_ID
,EVENT_TYPE_CODE
,EVENT_DATE
,ENTITY_ID
,EVENT_STATUS_CODE
,PROCESS_STATUS_CODE
,REFERENCE_NUM_1
,REFERENCE_NUM_2
,REFERENCE_NUM_3
,REFERENCE_NUM_4
,REFERENCE_CHAR_1
,REFERENCE_CHAR_2
,REFERENCE_CHAR_3
,REFERENCE_CHAR_4
,REFERENCE_DATE_1
,REFERENCE_DATE_2
,REFERENCE_DATE_3
,REFERENCE_DATE_4
,EVENT_NUMBER
,ON_HOLD_FLAG
,CREATION_DATE
,CREATED_BY
,LAST_UPDATE_DATE
,LAST_UPDATED_BY
,LAST_UPDATE_LOGIN
,PROGRAM_UPDATE_DATE
,PROGRAM_APPLICATION_ID
,PROGRAM_ID
,REQUEST_ID
,UPG_BATCH_ID
,UPG_SOURCE_APPLICATION_ID
,UPG_VALID_FLAG
,TRANSACTION_DATE
,BUDGETARY_CONTROL_FLAG
,MERGE_EVENT_SET_ID
,KCA_OPERATION
,'N' as IS_DELETED_FLG
,cast(NULLIF(KCA_SEQ_ID,'') as numeric(36,0)) as KCA_SEQ_ID
,kca_seq_date
    FROM
        bec_ods_stg.XLA_EVENTS;

end;		

UPDATE bec_etl_ctrl.batch_ods_info
SET
    load_type = 'I',
    last_refresh_date = getdate()
WHERE
    ods_table_name = 'xla_events'; 
	
COMMIT;