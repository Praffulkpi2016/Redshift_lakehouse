/*
	# Copyright(c) 2022 KPI Partners, Inc. All Rights Reserved.
	#
	# Unless required by applicable law or agreed to in writing, software
	# distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
	# WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
	#
	# author: KPI Partners, Inc.
	# version: 2022.06
	# description: This script represents full incremental approach for ODS.
	# File Version: KPI v1.0
*/
begin;
	drop table if exists bec_ods.GL_ACCESS_SET_LEDGERS;
	
	CREATE TABLE IF NOT EXISTS bec_ods.gl_access_set_ledgers
	(
		access_set_id NUMERIC(15,0) ENCODE az64,
		ledger_id NUMERIC(15,0) ENCODE az64,
		access_privilege_code VARCHAR(1) ENCODE lzo,
		last_update_date TIMESTAMP WITHOUT TIME ZONE ENCODE az64,
		last_updated_by NUMERIC(15,0) ENCODE az64,
		creation_date TIMESTAMP WITHOUT TIME ZONE ENCODE az64,
		created_by NUMERIC(15,0) ENCODE az64,
		last_update_login NUMERIC(15,0) ENCODE az64,
		start_date TIMESTAMP WITHOUT TIME ZONE ENCODE az64,
		end_date TIMESTAMP WITHOUT TIME ZONE ENCODE az64,
		kca_operation VARCHAR(10)   ENCODE lzo,
		is_deleted_flg VARCHAR(2) ENCODE lzo,
		kca_seq_id NUMERIC(36,0)   ENCODE az64,
		kca_seq_date TIMESTAMP WITHOUT TIME ZONE ENCODE az64
	)
	DISTSTYLE AUTO
	;
	
	insert into bec_ods.gl_access_set_ledgers
	(
		access_set_id,
		ledger_id,
		access_privilege_code,
		last_update_date,
		last_updated_by,
		creation_date,
		created_by,
		last_update_login,
		start_date,
		end_date,
		kca_operation,
		is_deleted_flg,
		kca_seq_id,
		kca_seq_date
	)
	(
		select 
		access_set_id,
		ledger_id,
		access_privilege_code,
		last_update_date,
		last_updated_by,
		creation_date,
		created_by,
		last_update_login,
		start_date,
		end_date,
		kca_operation,
		'N' as IS_DELETED_FLG,
		cast(NULLIF(KCA_SEQ_ID,'') as numeric(36,0)) as KCA_SEQ_ID,
		kca_seq_date
		from bec_ods_stg.gl_access_set_ledgers
	);
	
end;

update bec_etl_ctrl.batch_ods_info 
set load_type = 'I', 
last_refresh_date = getdate() 
where ods_table_name='gl_access_set_ledgers'; 

commit;