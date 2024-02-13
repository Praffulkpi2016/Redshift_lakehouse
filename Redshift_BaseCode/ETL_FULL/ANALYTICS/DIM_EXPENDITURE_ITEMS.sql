/*
# Copyright(c) 2022 KPI Partners, Inc. All Rights Reserved.
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
# WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
#
# author: KPI Partners, Inc.
# version: 2022.06
# description: This script represents full load approach for Dimensions.
# File Version: KPI v1.0
*/
begin;
	
	drop table if exists bec_dwh.DIM_EXPENDITURE_ITEMS;
	
	create table bec_dwh.DIM_EXPENDITURE_ITEMS 
	diststyle all 
	sortkey(EXPENDITURE_ITEM_ID )
	as
	(
		select 
		EXPENDITURE_ITEM_ID
		,EXPENDITURE_ID
		, EXPENDITURE_TYPE 
		,orig_transaction_reference
		,transaction_source 
		,'N' as is_deleted_flg,
		(
			select
			system_id
			from
			bec_etl_ctrl.etlsourceappid
			where
		source_system = 'EBS') as source_app_id,
		(
			select
			system_id
			from
			bec_etl_ctrl.etlsourceappid
			where
		source_system = 'EBS')
		|| '-' || nvl(EXPENDITURE_ITEM_ID , 0)  as dw_load_id,
		getdate() as dw_insert_date,
		getdate() as dw_update_date
	from bec_ods.pa_expenditure_items_all );
	end;
	
	update
	bec_etl_ctrl.batch_dw_info
	set
	load_type = 'I',
	last_refresh_date = getdate()
	where
	dw_table_name = 'dim_expenditure_items'
	and batch_name = 'gl';
	
	commit;			