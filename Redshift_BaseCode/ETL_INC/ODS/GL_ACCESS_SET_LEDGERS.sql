/*
	# Copyright(c) 2022 KPI Partners, Inc. All Rights Reserved.
	#
	# Unless required by applicable law or agreed to in writing, software
	# distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
	# WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
	#
	# author: KPI Partners, Inc.
	# version: 2022.06
	# description: This script represents incremental approach for ODS.
	# File Version: KPI v1.0
*/
begin;
	truncate table bec_ods.GL_ACCESS_SET_LEDGERS;
	
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