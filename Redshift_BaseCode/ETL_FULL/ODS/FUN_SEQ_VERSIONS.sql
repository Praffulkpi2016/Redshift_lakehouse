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

DROP TABLE if exists bec_ods.FUN_SEQ_VERSIONS;

CREATE TABLE IF NOT EXISTS bec_ods.FUN_SEQ_VERSIONS
(
	seq_version_id NUMERIC(15,0)   ENCODE az64
	,seq_header_id NUMERIC(15,0)   ENCODE az64
	,version_name VARCHAR(120)   ENCODE lzo
	,header_name VARCHAR(120)   ENCODE lzo
	,initial_value NUMERIC(15,0)   ENCODE az64
	,start_date TIMESTAMP WITHOUT TIME ZONE   ENCODE az64
	,end_date TIMESTAMP WITHOUT TIME ZONE   ENCODE az64
	,current_value NUMERIC(15,0)   ENCODE az64
	,use_status_code VARCHAR(30)   ENCODE lzo
	,db_sequence_name VARCHAR(30)   ENCODE lzo
	,object_version_number NUMERIC(15,0)   ENCODE az64
	,last_update_date TIMESTAMP WITHOUT TIME ZONE   ENCODE az64
	,last_updated_by NUMERIC(15,0)   ENCODE az64
	,creation_date TIMESTAMP WITHOUT TIME ZONE   ENCODE az64
	,created_by NUMERIC(15,0)   ENCODE az64
	,last_update_login NUMERIC(15,0)   ENCODE az64
	,zd_edition_name VARCHAR(30)   ENCODE lzo
	,zd_sync VARCHAR(30)   ENCODE lzo
	,kca_operation VARCHAR(10)   ENCODE lzo
    ,is_deleted_flg VARCHAR(2) ENCODE lzo
	,kca_seq_id NUMERIC(36,0)   ENCODE az64
,kca_seq_date TIMESTAMP WITHOUT TIME ZONE   ENCODE az64  ) 
DISTSTYLE
auto;

INSERT INTO bec_ods.FUN_SEQ_VERSIONS (
	seq_version_id,
	seq_header_id,
	version_name,
	header_name,
	initial_value,
	start_date,
	end_date,
	current_value,
	use_status_code,
	db_sequence_name,
	object_version_number,
	last_update_date,
	last_updated_by,
	creation_date,
	created_by,
	last_update_login,
	zd_edition_name,
	zd_sync,
	kca_operation,
	is_deleted_flg,
	kca_seq_id,
	kca_seq_date
)
SELECT
	seq_version_id,
	seq_header_id,
	version_name,
	header_name,
	initial_value,
	start_date,
	end_date,
	current_value,
	use_status_code,
	db_sequence_name,
	object_version_number,
	last_update_date,
	last_updated_by,
	creation_date,
	created_by,
	last_update_login,
	zd_edition_name,
	zd_sync,
	kca_operation,
	'N' as IS_DELETED_FLG,
	cast(NULLIF(KCA_SEQ_ID,'') as numeric(36,0)) as KCA_SEQ_ID
		,kca_seq_date
    FROM
        bec_ods_stg.FUN_SEQ_VERSIONS;
end;		

UPDATE bec_etl_ctrl.batch_ods_info
SET
    load_type = 'I',
    last_refresh_date = getdate()
WHERE
    ods_table_name = 'fun_seq_versions';