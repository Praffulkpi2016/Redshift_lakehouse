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

DROP TABLE if exists bec_ods.wf_runnable_processes_v;

CREATE TABLE IF NOT EXISTS bec_ods.wf_runnable_processes_v
(
	ITEM_TYPE VARCHAR(8)   ENCODE lzo
	,PROCESS_NAME VARCHAR(30)   ENCODE lzo
	,DISPLAY_NAME VARCHAR(80)   ENCODE lzo	
	,kca_operation VARCHAR(10)   ENCODE lzo
    ,is_deleted_flg VARCHAR(2) ENCODE lzo
	,kca_seq_id NUMERIC(36,0)   ENCODE az64
	,kca_seq_date TIMESTAMP WITHOUT TIME ZONE   ENCODE az64	
)
DISTSTYLE
auto;

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
	cast(NULLIF(KCA_SEQ_ID,'') as numeric(36,0)) as KCA_SEQ_ID
	,kca_seq_date
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