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

DROP TABLE if exists bec_ods.PO_ACTION_HISTORY ;

CREATE TABLE IF NOT EXISTS bec_ods.PO_ACTION_HISTORY 
(
    object_id NUMERIC(15,0)   ENCODE az64
	,object_type_code VARCHAR(25)   ENCODE lzo
	,object_sub_type_code VARCHAR(25)   ENCODE lzo
	,sequence_num NUMERIC(15,0)   ENCODE az64
	,last_update_date TIMESTAMP WITHOUT TIME ZONE   ENCODE az64
	,last_updated_by NUMERIC(15,0)   ENCODE az64
	,creation_date TIMESTAMP WITHOUT TIME ZONE   ENCODE az64
	,created_by NUMERIC(15,0)   ENCODE az64
	,action_code VARCHAR(25)   ENCODE lzo
	,action_date TIMESTAMP WITHOUT TIME ZONE   ENCODE az64
	,employee_id NUMERIC(9,0)   ENCODE az64
	,approval_path_id NUMERIC(15,0)   ENCODE az64
	,note VARCHAR(4000)   ENCODE lzo
	,object_revision_num NUMERIC(15,0)   ENCODE az64
	,offline_code VARCHAR(25)   ENCODE lzo
	,last_update_login NUMERIC(15,0)   ENCODE az64
	,request_id NUMERIC(15,0)   ENCODE az64
	,program_application_id NUMERIC(15,0)   ENCODE az64
	,program_id NUMERIC(15,0)   ENCODE az64
	,program_update_date TIMESTAMP WITHOUT TIME ZONE   ENCODE az64
	,program_date TIMESTAMP WITHOUT TIME ZONE   ENCODE az64
	,approval_group_id NUMERIC(15,0)   ENCODE az64
	,kca_operation VARCHAR(10)   ENCODE lzo
    ,is_deleted_flg VARCHAR(2) ENCODE lzo
	,kca_seq_id NUMERIC(36,0)   ENCODE az64
		,kca_seq_date TIMESTAMP WITHOUT TIME ZONE   ENCODE az64
)
DISTSTYLE
auto;

INSERT INTO bec_ods.PO_ACTION_HISTORY  (
			object_id,
			object_type_code,
			object_sub_type_code,
			sequence_num,
			last_update_date,
			last_updated_by,
			creation_date,
			created_by,
			action_code,
			action_date,
			employee_id,
			approval_path_id,
			note,
			object_revision_num,
			offline_code,
			last_update_login,
			request_id,
			program_application_id,
			program_id,
			program_update_date,
			program_date,
			approval_group_id,
			kca_operation,
			is_deleted_flg,
			kca_seq_id
		,kca_seq_date	
)
    SELECT
			object_id,
			object_type_code,
			object_sub_type_code,
			sequence_num,
			last_update_date,
			last_updated_by,
			creation_date,
			created_by,
			action_code,
			action_date,
			employee_id,
			approval_path_id,
			note,
			object_revision_num,
			offline_code,
			last_update_login,
			request_id,
			program_application_id,
			program_id,
			program_update_date,
			program_date,
			approval_group_id,
			kca_operation,
			'N' as IS_DELETED_FLG,
			cast(NULLIF(KCA_SEQ_ID,'') as numeric(36,0)) as KCA_SEQ_ID
		,kca_seq_date 		
    FROM
        bec_ods_stg.PO_ACTION_HISTORY ;

end;		

UPDATE bec_etl_ctrl.batch_ods_info
SET
    load_type = 'I',
    last_refresh_date = getdate()
WHERE
    ods_table_name = 'po_action_history ';
	
commit;