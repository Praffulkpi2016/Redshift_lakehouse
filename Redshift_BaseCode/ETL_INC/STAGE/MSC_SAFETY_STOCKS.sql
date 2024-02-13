/*
# Copyright(c) 2022 KPI Partners, Inc. All Rights Reserved.
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
# WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
#
# author: KPI Partners, Inc.
# version: 2022.06
# description: This script represents Incremental load approach for stage.
# File Version: KPI v1.0
*/
begin;

truncate
	table bec_ods_stg.MSC_SAFETY_STOCKS;

insert
	into
	bec_ods_stg.MSC_SAFETY_STOCKS
(plan_id,
	organization_id,
	sr_instance_id,
	inventory_item_id,
	period_start_date,
	safety_stock_quantity,
	updated,
	status,
	refresh_number,
	last_update_date,
	last_updated_by,
	creation_date,
	created_by,
	last_update_login,
	request_id,
	program_id,
	program_update_date,
	program_application_id,
	target_safety_stock,
	project_id,
	task_id,
	planning_group,
	user_defined_safety_stocks,
	user_defined_dos,
	target_days_of_supply,
	achieved_days_of_supply,
	unit_number,
	demand_var_ss_percent,
	mfg_ltvar_ss_percent,
	transit_ltvar_ss_percent,
	sup_ltvar_ss_percent,
	total_unpooled_safety_stock,
	item_type_id,
	item_type_value,
	new_plan_id,
	simulation_set_id,
	new_plan_list,
	applied,
	reserved_safety_stock_qty,
	inventory_level,
	KCA_OPERATION,
	kca_seq_id,
	kca_seq_date
)
(
	select
		plan_id,
	organization_id,
	sr_instance_id,
	inventory_item_id,
	period_start_date,
	safety_stock_quantity,
	updated,
	status,
	refresh_number,
	last_update_date,
	last_updated_by,
	creation_date,
	created_by,
	last_update_login,
	request_id,
	program_id,
	program_update_date,
	program_application_id,
	target_safety_stock,
	project_id,
	task_id,
	planning_group,
	user_defined_safety_stocks,
	user_defined_dos,
	target_days_of_supply,
	achieved_days_of_supply,
	unit_number,
	demand_var_ss_percent,
	mfg_ltvar_ss_percent,
	transit_ltvar_ss_percent,
	sup_ltvar_ss_percent,
	total_unpooled_safety_stock,
	item_type_id,
	item_type_value,
	new_plan_id,
	simulation_set_id,
	new_plan_list,
	applied,
	reserved_safety_stock_qty,
	inventory_level,
		KCA_OPERATION,
		kca_seq_id,
	kca_seq_date
	from
		bec_raw_dl_ext.MSC_SAFETY_STOCKS
	where
		kca_operation != 'DELETE'  and nvl(kca_seq_id,'')!= '' 
		and (PLAN_ID, INVENTORY_ITEM_ID, ORGANIZATION_ID,PERIOD_START_DATE,
		kca_seq_id) in 
(
		select
			PLAN_ID, INVENTORY_ITEM_ID, ORGANIZATION_ID,PERIOD_START_DATE,
			max(kca_seq_id)
		from
			bec_raw_dl_ext.MSC_SAFETY_STOCKS
		where
			kca_operation != 'DELETE'  and nvl(kca_seq_id,'')!= ''
		group by
			PLAN_ID, INVENTORY_ITEM_ID, ORGANIZATION_ID,PERIOD_START_DATE)
		and 
       ( kca_seq_date > (
		select
			(executebegints-prune_days)
		from
			bec_etl_ctrl.batch_ods_info
		where
			ods_table_name = 'msc_safety_stocks') 
			 
            )
			
);
end;