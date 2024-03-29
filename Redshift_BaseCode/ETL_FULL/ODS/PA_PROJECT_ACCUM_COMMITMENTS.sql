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

DROP TABLE if exists bec_ods.pa_project_accum_commitments;

CREATE TABLE IF NOT EXISTS bec_ods.pa_project_accum_commitments
(
	PROJECT_ACCUM_ID NUMERIC(15,0) ENCODE az64 
	,CMT_RAW_COST_ITD NUMERIC(28,10) ENCODE az64 
	,CMT_RAW_COST_YTD NUMERIC(28,10) ENCODE az64 
	,CMT_RAW_COST_PP NUMERIC(28,10) ENCODE az64 
	,CMT_RAW_COST_PTD NUMERIC(28,10) ENCODE az64 
	,CMT_BURDENED_COST_ITD NUMERIC(28,10) ENCODE az64 
	,CMT_BURDENED_COST_YTD NUMERIC(28,10) ENCODE az64 
	,CMT_BURDENED_COST_PP NUMERIC(28,10) ENCODE az64 
	,CMT_BURDENED_COST_PTD NUMERIC(28,10) ENCODE az64 
	,CMT_QUANTITY_ITD NUMERIC(28,10) ENCODE az64 
	,CMT_QUANTITY_YTD NUMERIC(28,10) ENCODE az64 
	,CMT_QUANTITY_PP NUMERIC(28,10) ENCODE az64 
	,CMT_QUANTITY_PTD NUMERIC(28,10) ENCODE az64
	,CMT_UNIT_OF_MEASURE VARCHAR(30) ENCODE lzo    
	,LAST_UPDATED_BY NUMERIC(15,0) ENCODE az64 
	,LAST_UPDATE_DATE TIMESTAMP WITHOUT TIME ZONE ENCODE az64	
	,CREATION_DATE TIMESTAMP WITHOUT TIME ZONE ENCODE az64 		
	,CREATED_BY NUMERIC(15,0) ENCODE az64 
	,LAST_UPDATE_LOGIN NUMERIC(15,0) ENCODE az64 	
	,REQUEST_ID NUMERIC(15,0) ENCODE az64 	
	,PROGRAM_APPLICATION_ID NUMERIC(15,0) ENCODE az64 
	,PROGRAM_ID NUMERIC(15,0) ENCODE az64 
	,PROGRAM_UPDATE_DATE TIMESTAMP WITHOUT TIME ZONE ENCODE az64  
	,KCA_OPERATION VARCHAR(10)   ENCODE lzo
    ,IS_DELETED_FLG VARCHAR(2) ENCODE lzo
	,kca_seq_id NUMERIC(36,0)   ENCODE az64
	,kca_seq_date TIMESTAMP WITHOUT TIME ZONE ENCODE az64
)
DISTSTYLE
auto;

INSERT INTO bec_ods.pa_project_accum_commitments (
	PROJECT_ACCUM_ID,
	CMT_RAW_COST_ITD,
	CMT_RAW_COST_YTD,
	CMT_RAW_COST_PP,
	CMT_RAW_COST_PTD,
	CMT_BURDENED_COST_ITD,
	CMT_BURDENED_COST_YTD,
	CMT_BURDENED_COST_PP,
	CMT_BURDENED_COST_PTD,
	CMT_QUANTITY_ITD,
	CMT_QUANTITY_YTD,
	CMT_QUANTITY_PP,
	CMT_QUANTITY_PTD,
	CMT_UNIT_OF_MEASURE,
	LAST_UPDATED_BY,
	LAST_UPDATE_DATE,
	CREATION_DATE,
	CREATED_BY,
	LAST_UPDATE_LOGIN,
	REQUEST_ID,
	PROGRAM_APPLICATION_ID,
	PROGRAM_ID,
	PROGRAM_UPDATE_DATE,
	KCA_OPERATION,
	IS_DELETED_FLG,
	kca_seq_id
	,kca_seq_date
)
    SELECT
		PROJECT_ACCUM_ID,
		CMT_RAW_COST_ITD,
		CMT_RAW_COST_YTD,
		CMT_RAW_COST_PP,
		CMT_RAW_COST_PTD,
		CMT_BURDENED_COST_ITD,
		CMT_BURDENED_COST_YTD,
		CMT_BURDENED_COST_PP,
		CMT_BURDENED_COST_PTD,
		CMT_QUANTITY_ITD,
		CMT_QUANTITY_YTD,
		CMT_QUANTITY_PP,
		CMT_QUANTITY_PTD,
		CMT_UNIT_OF_MEASURE,
		LAST_UPDATED_BY,
		LAST_UPDATE_DATE,
		CREATION_DATE,
		CREATED_BY,
		LAST_UPDATE_LOGIN,
		REQUEST_ID,
		PROGRAM_APPLICATION_ID,
		PROGRAM_ID,
		PROGRAM_UPDATE_DATE,
		KCA_OPERATION,
		'N' as IS_DELETED_FLG,
		cast(NULLIF(KCA_SEQ_ID,'') as numeric(36,0)) as KCA_SEQ_ID
		,kca_seq_date
    FROM
        bec_ods_stg.pa_project_accum_commitments;

end;		

UPDATE bec_etl_ctrl.batch_ods_info
SET
    load_type = 'I',
    last_refresh_date = getdate()
WHERE
    ods_table_name = 'pa_project_accum_commitments';
	
commit;