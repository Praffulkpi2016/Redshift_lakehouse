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
	drop table if exists bec_ods.OKS_LINE_DETAILS_V;
	
	CREATE TABLE IF NOT EXISTS bec_ods.OKS_LINE_DETAILS_V
	(
		contract_id NUMERIC(15,0)   ENCODE az64
		,line_id VARCHAR(50)   ENCODE lzo
		,line_reference VARCHAR(300)   ENCODE lzo
		,service_line_number VARCHAR(150)   ENCODE lzo
		,service_name VARCHAR(40)   ENCODE lzo
		,object1_id1 VARCHAR(40)   ENCODE lzo
		,object1_id2 VARCHAR(200)   ENCODE lzo
		,service_start_date TIMESTAMP WITHOUT TIME ZONE   ENCODE az64
		,service_end_date TIMESTAMP WITHOUT TIME ZONE   ENCODE az64
		,customer_account_name VARCHAR(360)   ENCODE lzo
		,line_bto_address VARCHAR(4000)   ENCODE lzo
		,line_sto_address VARCHAR(4000)   ENCODE lzo
		,kca_operation VARCHAR(10)   ENCODE lzo
		,is_deleted_flg VARCHAR(2) ENCODE lzo
		,kca_seq_id NUMERIC(36,0)   ENCODE az64
		,kca_seq_date TIMESTAMP WITHOUT TIME ZONE ENCODE az64
	)
	DISTSTYLE AUTO
	;
	
	insert into bec_ods.OKS_LINE_DETAILS_V
	(
		contract_id,
		line_id,
		line_reference,
		service_line_number,
		service_name,
		object1_id1,
		object1_id2,
		service_start_date,
		service_end_date,
		customer_account_name,
		line_bto_address,
		line_sto_address,
		kca_operation,
		is_deleted_flg,
		kca_seq_id,
		kca_seq_date
	)
	(
		select 
		contract_id,
		line_id,
		line_reference,
		service_line_number,
		service_name,
		object1_id1,
		object1_id2,
		service_start_date,
		service_end_date,
		customer_account_name,
		line_bto_address,
		line_sto_address 
		,kca_operation,
		'N' as IS_DELETED_FLG,
		cast(NULLIF(KCA_SEQ_ID,'') as numeric(36,0)) as KCA_SEQ_ID,
		kca_seq_date
		from bec_ods_stg.OKS_LINE_DETAILS_V
	);
	
end;

update bec_etl_ctrl.batch_ods_info set load_type = 'I', 
last_refresh_date = getdate() where ods_table_name='oks_line_details_v'; 

commit;