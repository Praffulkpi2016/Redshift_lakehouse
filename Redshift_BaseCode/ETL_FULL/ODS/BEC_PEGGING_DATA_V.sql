/*
	# Copyright(c) 2022 KPI Partners, Inc. All Rights Reserved.
	#
	# Unless required by applicable law or agreed to in writing, software
	# distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
	# WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
	#
	# author: KPI Partners, Inc.
	# version: 2022.06
	# description: This script represents full load approach to ODS.
	# File Version: KPI v1.0
*/
begin;
	
	DROP TABLE if exists bec_ods.BEC_PEGGING_DATA_V;
	
	CREATE TABLE IF NOT EXISTS bec_ods.BEC_PEGGING_DATA_V
	(	pegging_id numeric(15,0)   ENCODE az64
		,PREV_PEGGING_ID numeric(15,0)   ENCODE az64
		,PREV_ITEM_ID numeric(15,0)   ENCODE az64
		,prev_item_org VARCHAR(4000)   ENCODE lzo
		,prev_item_desc VARCHAR(4000)   ENCODE lzo
		,prev_pegged_qty NUMERIC(28,10)   ENCODE az64
		,prev_order_type VARCHAR(4000)   ENCODE lzo
		,kca_operation VARCHAR(10)   ENCODE lzo
		,is_deleted_flg VARCHAR(2) ENCODE lzo
		,kca_seq_id NUMERIC(36,0)   ENCODE az64
		,kca_seq_date TIMESTAMP WITHOUT TIME ZONE ENCODE az64 
	)
	DISTSTYLE
	auto;
	insert into bec_ods.BEC_PEGGING_DATA_V
	(    pegging_id 
		,PREV_PEGGING_ID 
		,PREV_ITEM_ID 
		,prev_item_org 
		,prev_item_desc  
		,prev_pegged_qty 
		,prev_order_type 
		,kca_operation 
		,is_deleted_flg  
		,kca_seq_id 
	,kca_seq_date  )
    SELECT
	pegging_id 
	,PREV_PEGGING_ID 
	,PREV_ITEM_ID 
	,prev_item_org 
	,prev_item_desc  
	,prev_pegged_qty 
	,prev_order_type 
	,kca_operation,
	'N' as IS_DELETED_FLG ,
	cast(NULLIF(KCA_SEQ_ID,'') as numeric(36,0)) as KCA_SEQ_ID
	,kca_seq_date
    FROM
	bec_ods_stg.BEC_PEGGING_DATA_V;
	
end;


UPDATE bec_etl_ctrl.batch_ods_info
SET
load_type = 'I',
last_refresh_date = getdate()
WHERE
ods_table_name = 'bec_pegging_data_v';

commit;