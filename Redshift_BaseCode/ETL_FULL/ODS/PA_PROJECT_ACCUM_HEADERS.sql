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

DROP TABLE if exists bec_ods.PA_PROJECT_ACCUM_HEADERS;

CREATE TABLE IF NOT EXISTS bec_ods.PA_PROJECT_ACCUM_HEADERS
(

     project_accum_id NUMERIC(15,0)   ENCODE az64
	,project_id NUMERIC(15,0)   ENCODE az64
	,task_id NUMERIC(15,0)   ENCODE az64
	,accum_period VARCHAR(20)   ENCODE lzo
	,resource_id NUMERIC(15,0)   ENCODE az64
	,resource_list_assignment_id NUMERIC(15,0)   ENCODE az64
	,resource_list_id NUMERIC(15,0)   ENCODE az64
	,resource_list_member_id NUMERIC(15,0)   ENCODE az64
	,last_updated_by NUMERIC(15,0)   ENCODE az64
	,last_update_date TIMESTAMP WITHOUT TIME ZONE   ENCODE az64
	,creation_date TIMESTAMP WITHOUT TIME ZONE   ENCODE az64
	,created_by NUMERIC(15,0)   ENCODE az64
	,last_update_login NUMERIC(15,0)   ENCODE az64
	,request_id NUMERIC(15,0)   ENCODE az64
	,program_application_id NUMERIC(15,0)   ENCODE az64
	,program_id NUMERIC(15,0)   ENCODE az64
	,program_update_date TIMESTAMP WITHOUT TIME ZONE   ENCODE az64
	,tasks_restructured_flag VARCHAR(1)   ENCODE lzo
	,sum_exception_code VARCHAR(80)   ENCODE lzo
	,KCA_OPERATION VARCHAR(10)   ENCODE lzo
    ,IS_DELETED_FLG VARCHAR(2) ENCODE lzo
	,kca_seq_id NUMERIC(36,0)   ENCODE az64
	,kca_seq_date TIMESTAMP WITHOUT TIME ZONE ENCODE az64
)
DISTSTYLE
auto;

INSERT INTO bec_ods.PA_PROJECT_ACCUM_HEADERS (
 PROJECT_ACCUM_ID
,      PROJECT_ID
,      TASK_ID
,      ACCUM_PERIOD
,      RESOURCE_ID
,      RESOURCE_LIST_ASSIGNMENT_ID
,      RESOURCE_LIST_ID
,      RESOURCE_LIST_MEMBER_ID
,      LAST_UPDATED_BY
,      LAST_UPDATE_DATE
,      CREATION_DATE
,      CREATED_BY
,      LAST_UPDATE_LOGIN
,      REQUEST_ID
,      PROGRAM_APPLICATION_ID
,      PROGRAM_ID
,      PROGRAM_UPDATE_DATE
,      TASKS_RESTRUCTURED_FLAG
,      SUM_EXCEPTION_CODE
	,kca_operation,
	IS_DELETED_FLG,
	kca_seq_id
	,kca_seq_date
)
    SELECT
  PROJECT_ACCUM_ID
,      PROJECT_ID
,      TASK_ID
,      ACCUM_PERIOD
,      RESOURCE_ID
,      RESOURCE_LIST_ASSIGNMENT_ID
,      RESOURCE_LIST_ID
,      RESOURCE_LIST_MEMBER_ID
,      LAST_UPDATED_BY
,      LAST_UPDATE_DATE
,      CREATION_DATE
,      CREATED_BY
,      LAST_UPDATE_LOGIN
,      REQUEST_ID
,      PROGRAM_APPLICATION_ID
,      PROGRAM_ID
,      PROGRAM_UPDATE_DATE
,      TASKS_RESTRUCTURED_FLAG
,      SUM_EXCEPTION_CODE
   , KCA_OPERATION,
    'N' as IS_DELETED_FLG,
    cast(NULLIF(KCA_SEQ_ID,'') as numeric(36,0)) as KCA_SEQ_ID
	,kca_seq_date
    FROM
        bec_ods_stg.PA_PROJECT_ACCUM_HEADERS;

end;		

UPDATE bec_etl_ctrl.batch_ods_info
SET
    load_type = 'I',
    last_refresh_date = getdate()
WHERE
    ods_table_name = 'pa_project_accum_headers';
	
COMMIT;