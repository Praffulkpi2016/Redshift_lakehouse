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

DROP TABLE if exists bec_ods.GL_LEDGER_CONFIG_DETAILS;

CREATE TABLE IF NOT EXISTS bec_ods.GL_LEDGER_CONFIG_DETAILS
(    configuration_id NUMERIC(15,0)   ENCODE az64
	,object_type_code VARCHAR(30)   ENCODE lzo
	,object_id NUMERIC(15,0)   ENCODE az64
	,object_name VARCHAR(240)   ENCODE lzo
	,setup_step_code VARCHAR(30)   ENCODE lzo
	,next_action_code VARCHAR(30)   ENCODE lzo
	,status_code VARCHAR(30)   ENCODE lzo
	,created_by NUMERIC(15,0)   ENCODE az64
	,last_update_login NUMERIC(15,0)   ENCODE az64
	,last_update_date TIMESTAMP WITHOUT TIME ZONE   ENCODE az64
	,last_updated_by NUMERIC(15,0)   ENCODE az64
	,creation_date TIMESTAMP WITHOUT TIME ZONE   ENCODE az64
	,kca_operation VARCHAR(10)   ENCODE lzo
    ,is_deleted_flg VARCHAR(2) ENCODE lzo
	,kca_seq_id NUMERIC(36,0)   ENCODE az64
	,kca_seq_date TIMESTAMP WITHOUT TIME ZONE   ENCODE az64
)
DISTSTYLE AUTO
;


insert into bec_ods.GL_LEDGER_CONFIG_DETAILS
(
	configuration_id,
	object_type_code,
	object_id,
	object_name,
	setup_step_code,
	next_action_code,
	status_code,
	created_by,
	last_update_login,
	last_update_date,
	last_updated_by,
	creation_date,
	kca_operation,
	IS_DELETED_FLG,
	kca_seq_id,
	kca_seq_date)
SELECT
	configuration_id,
	object_type_code,
	object_id,
	object_name,
	setup_step_code,
	next_action_code,
	status_code,
	created_by,
	last_update_login,
	last_update_date,
	last_updated_by,
	creation_date,
	kca_operation,
	'N' as IS_DELETED_FLG,
	cast(NULLIF(KCA_SEQ_ID,'') as numeric(36,0)) as KCA_SEQ_ID,
	kca_seq_date
FROM bec_ods_stg.GL_LEDGER_CONFIG_DETAILS;

end;

update
	bec_etl_ctrl.batch_ods_info
set load_type = 'I',
	last_refresh_date = getdate()
where
	ods_table_name = 'gl_ledger_config_details'; 
commit;