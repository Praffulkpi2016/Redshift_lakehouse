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

DROP TABLE if exists bec_ods.WF_ACTIVITIES;

CREATE TABLE IF NOT EXISTS bec_ods.WF_ACTIVITIES
(
	item_type VARCHAR(8)   ENCODE lzo
	,"name" VARCHAR(30)   ENCODE lzo
	,version NUMERIC(15,0)   ENCODE az64
	,"type" VARCHAR(8)   ENCODE lzo
	,rerun VARCHAR(8)   ENCODE lzo
	,expand_role VARCHAR(1)   ENCODE lzo
	,protect_level NUMERIC(15,0)   ENCODE az64
	,custom_level NUMERIC(15,0)   ENCODE az64
	,begin_date TIMESTAMP WITHOUT TIME ZONE   ENCODE az64
	,end_date TIMESTAMP WITHOUT TIME ZONE   ENCODE az64
	,"function" VARCHAR(240)   ENCODE lzo
	,result_type VARCHAR(30)   ENCODE lzo
	,cost NUMERIC(28,10)   ENCODE az64
	,read_role VARCHAR(320)   ENCODE lzo
	,write_role VARCHAR(320)   ENCODE lzo
	,execute_role VARCHAR(320)   ENCODE lzo
	,icon_name VARCHAR(30)   ENCODE lzo
	,message VARCHAR(30)   ENCODE lzo
	,error_process VARCHAR(30)   ENCODE lzo
	,error_item_type VARCHAR(8)   ENCODE lzo
	,runnable_flag VARCHAR(1)   ENCODE lzo
	,function_type VARCHAR(30)   ENCODE lzo
	,event_name VARCHAR(240)   ENCODE lzo
	,direction VARCHAR(30) ENCODE lzo
	,security_group_id VARCHAR(32)   ENCODE lzo
	,zd_edition_name VARCHAR(30)   ENCODE lzo
	,zd_sync VARCHAR(30)   ENCODE lzo
	,KCA_OPERATION VARCHAR(10)   ENCODE lzo
    ,IS_DELETED_FLG VARCHAR(2) ENCODE lzo
	,kca_seq_id NUMERIC(36,0)   ENCODE az64
		,kca_seq_date TIMESTAMP WITHOUT TIME ZONE   ENCODE az64
)
DISTSTYLE
auto;

INSERT INTO bec_ods.WF_ACTIVITIES (
   item_type,
	"name",
	version,
	"type",
	rerun,
	expand_role,
	protect_level,
	custom_level,
	begin_date,
	end_date,
	"function",
	result_type,
	cost,
	read_role,
	write_role,
	execute_role,
	icon_name,
	message,
	error_process,
	error_item_type,
	runnable_flag,
	function_type,
	event_name,
	direction,
	security_group_id,
	zd_edition_name,
	zd_sync,
	kca_operation,
	IS_DELETED_FLG,
	kca_seq_id
	,kca_seq_date
)
    SELECT
   item_type,
	"name",
	version,
	"type",
	rerun,
	expand_role,
	protect_level,
	custom_level,
	begin_date,
	end_date,
	"function",
	result_type,
	cost,
	read_role,
	write_role,
	execute_role,
	icon_name,
	message,
	error_process,
	error_item_type,
	runnable_flag,
	function_type,
	event_name,
	direction,
	security_group_id,
	zd_edition_name,
	zd_sync,
    KCA_OPERATION,
    'N' as IS_DELETED_FLG,
    cast(NULLIF(KCA_SEQ_ID,'') as numeric(36,0)) as KCA_SEQ_ID
	,kca_seq_date
    FROM
        bec_ods_stg.WF_ACTIVITIES;

end;		

UPDATE bec_etl_ctrl.batch_ods_info
SET
    load_type = 'I',
    last_refresh_date = getdate()
WHERE
    ods_table_name = 'wf_activities';
	
COMMIT;