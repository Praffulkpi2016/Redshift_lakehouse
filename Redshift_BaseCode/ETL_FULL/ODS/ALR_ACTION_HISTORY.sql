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
	drop table if exists bec_ods.ALR_ACTION_HISTORY;
	
	CREATE TABLE IF NOT EXISTS bec_ods.alr_action_history
	(
		APPLICATION_ID NUMERIC(15,0) ENCODE az64, 
		ACTION_HISTORY_ID NUMERIC(15,0) ENCODE az64, 
		ALERT_ID NUMERIC(15,0) ENCODE az64, 
		ACTION_SET_ID NUMERIC(15,0) ENCODE az64, 
		CHECK_ID NUMERIC(15,0) ENCODE az64, 
		ACTION_ID NUMERIC(15,0) ENCODE az64, 
		ACTION_SET_MEMBER_ID NUMERIC(15,0) ENCODE az64, 
		LAST_UPDATE_DATE TIMESTAMP WITHOUT TIME ZONE ENCODE az64, 
		ACTION_LEVEL NUMERIC(15,0) ENCODE az64, 
		COLUMN_WRAP_FLAG VARCHAR(1) ENCODE lzo, 
		MAXIMUM_SUMMARY_MESSAGE_WIDTH NUMERIC(28,10) ENCODE az64, 
		ACTION_EXECUTED_FLAG VARCHAR(1) ENCODE lzo, 
		SUCCESS_FLAG VARCHAR(1) ENCODE lzo, 
		VERSION_NUMBER NUMERIC(15,0) ENCODE az64, 
		MESSAGE_HANDLE NUMERIC(15,0) ENCODE az64, 
		NODE_HANDLE NUMERIC(15,0) ENCODE az64, 
		SECURITY_GROUP_ID NUMERIC(15,0) ENCODE az64,
		kca_operation VARCHAR(10)   ENCODE lzo,
		is_deleted_flg VARCHAR(2) ENCODE lzo,
		kca_seq_id NUMERIC(36,0)   ENCODE az64,
		kca_seq_date TIMESTAMP WITHOUT TIME ZONE ENCODE az64
	)
	DISTSTYLE AUTO
	;
	
	insert into bec_ods.alr_action_history
	(
		APPLICATION_ID, 
		ACTION_HISTORY_ID, 
		ALERT_ID, 
		ACTION_SET_ID, 
		CHECK_ID, 
		ACTION_ID, 
		ACTION_SET_MEMBER_ID, 
		LAST_UPDATE_DATE, 
		ACTION_LEVEL, 
		COLUMN_WRAP_FLAG, 
		MAXIMUM_SUMMARY_MESSAGE_WIDTH, 
		ACTION_EXECUTED_FLAG, 
		SUCCESS_FLAG, 
		VERSION_NUMBER, 
		MESSAGE_HANDLE, 
		NODE_HANDLE, 
		SECURITY_GROUP_ID, 
		kca_operation,
		is_deleted_flg,
		kca_seq_id,
		kca_seq_date
	)
	(
		select 
		APPLICATION_ID, 
		ACTION_HISTORY_ID, 
		ALERT_ID, 
		ACTION_SET_ID, 
		CHECK_ID, 
		ACTION_ID, 
		ACTION_SET_MEMBER_ID, 
		LAST_UPDATE_DATE, 
		ACTION_LEVEL, 
		COLUMN_WRAP_FLAG, 
		MAXIMUM_SUMMARY_MESSAGE_WIDTH, 
		ACTION_EXECUTED_FLAG, 
		SUCCESS_FLAG, 
		VERSION_NUMBER, 
		MESSAGE_HANDLE, 
		NODE_HANDLE, 
		SECURITY_GROUP_ID, 
		kca_operation,
		'N' as IS_DELETED_FLG,
		cast(NULLIF(KCA_SEQ_ID,'') as numeric(36,0)) as KCA_SEQ_ID,
		kca_seq_date
		from bec_ods_stg.alr_action_history
	);
	
end;

update bec_etl_ctrl.batch_ods_info 
set load_type = 'I', 
last_refresh_date = getdate() 
where ods_table_name='alr_action_history'; 

commit;