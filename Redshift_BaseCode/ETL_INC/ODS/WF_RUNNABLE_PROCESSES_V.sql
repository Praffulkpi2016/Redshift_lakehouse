/*
# Copyright(c) 2022 KPI Partners, Inc. All Rights Reserved.
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
# WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
#
# author: KPI Partners, Inc.
# version: 2022.06
# description: This script represents incremental load approach for ODS.
# File Version: KPI v1.0
*/


begin;

TRUNCATE TABLE bec_ods.wf_runnable_processes_v;

 

INSERT INTO bec_ods.wf_runnable_processes_v (
	ITEM_TYPE, 
	PROCESS_NAME, 
	DISPLAY_NAME, 
	kca_operation,
	is_deleted_flg,
	kca_seq_id
	,kca_seq_date
)
    SELECT
	ITEM_TYPE, 
	PROCESS_NAME, 
	DISPLAY_NAME, 
	kca_operation,
	'N' as IS_DELETED_FLG,
	cast(NULLIF(KCA_SEQ_ID,'') as numeric(36,0)) as KCA_SEQ_ID,
	KCA_SEQ_DATE
    FROM
        bec_ods_stg.wf_runnable_processes_v;

end;


UPDATE bec_etl_ctrl.batch_ods_info
SET
    load_type = 'I',
    last_refresh_date = getdate()
WHERE
    ods_table_name = 'wf_runnable_processes_v';
	
commit;