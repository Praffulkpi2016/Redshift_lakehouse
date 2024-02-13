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
	drop table if exists bec_ods.ALR_OUTPUT_HISTORY;
	
	CREATE TABLE IF NOT EXISTS bec_ods.alr_output_history
	(
		APPLICATION_ID NUMERIC(15,0) ENCODE az64, 
		NAME VARCHAR(30) ENCODE lzo, 
		CHECK_ID NUMERIC(15,0) ENCODE az64, 
		ROW_NUMBER NUMERIC(15,0) ENCODE az64, 
		DATA_TYPE VARCHAR(1) ENCODE lzo, 
		VALUE VARCHAR(240) ENCODE lzo, 
		SECURITY_GROUP_ID NUMERIC(15,0) ENCODE az64, 
		kca_operation VARCHAR(10)   ENCODE lzo,
		is_deleted_flg VARCHAR(2) ENCODE lzo,
		kca_seq_id NUMERIC(36,0)   ENCODE az64,
		kca_seq_date TIMESTAMP WITHOUT TIME ZONE ENCODE az64
	)
	DISTSTYLE AUTO
	;
	
	insert into bec_ods.alr_output_history
	(
		APPLICATION_ID, 
		NAME, 
		CHECK_ID, 
		ROW_NUMBER, 
		DATA_TYPE, 
		VALUE, 
		SECURITY_GROUP_ID,
		kca_operation,
		is_deleted_flg,
		kca_seq_id,
		kca_seq_date
	)
	(
		select 
		APPLICATION_ID, 
		NAME, 
		CHECK_ID, 
		ROW_NUMBER, 
		DATA_TYPE, 
		VALUE, 
		SECURITY_GROUP_ID,
		kca_operation,
		'N' as IS_DELETED_FLG,
		cast(NULLIF(KCA_SEQ_ID,'') as numeric(36,0)) as KCA_SEQ_ID,
		kca_seq_date
		from bec_ods_stg.alr_output_history
	);
	
end;

update bec_etl_ctrl.batch_ods_info 
set load_type = 'I', 
last_refresh_date = getdate() 
where ods_table_name='alr_output_history'; 

commit;