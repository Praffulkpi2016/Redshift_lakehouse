/*
# Copyright(c) 2022 KPI Partners, Inc. All Rights Reserved.
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
# WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
#
# author: KPI Partners, Inc.
# version: 2022.06
# description: This script represents Incremental load approach for ODS.
# File Version: KPI v1.0
*/
begin;
-- Delete Records

delete
from
	bec_ods.MSC_SAFETY_STOCKS
where
	(PLAN_ID,
	INVENTORY_ITEM_ID,
	ORGANIZATION_ID,
	PERIOD_START_DATE) in (
	select
		stg.PLAN_ID,
		stg.INVENTORY_ITEM_ID,
		stg.ORGANIZATION_ID,
		stg.PERIOD_START_DATE
	from
		bec_ods.MSC_SAFETY_STOCKS ods,
		bec_ods_stg.MSC_SAFETY_STOCKS stg
	where
		ods.PLAN_ID = stg.PLAN_ID
		and ods.INVENTORY_ITEM_ID = stg.INVENTORY_ITEM_ID
		and ods.ORGANIZATION_ID = stg.ORGANIZATION_ID
		and ods.PERIOD_START_DATE = stg.PERIOD_START_DATE
		and stg.kca_operation in ('INSERT', 'UPDATE')
);

commit;
-- Insert records

insert
	into
	bec_ods.MSC_SAFETY_STOCKS
       (
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
	kca_operation,
	is_deleted_flg,
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
		kca_operation,
		'N' as IS_DELETED_FLG,
		cast(nullif(KCA_SEQ_ID, '') as numeric(36, 0)) as KCA_SEQ_ID,
	kca_seq_date
	from
		bec_ods_stg.MSC_SAFETY_STOCKS
	where
		kca_operation IN ('INSERT','UPDATE')
		and (PLAN_ID,
		INVENTORY_ITEM_ID,
		ORGANIZATION_ID,
		PERIOD_START_DATE,
		kca_seq_id) in 
	(
		select
			PLAN_ID,
			INVENTORY_ITEM_ID,
			ORGANIZATION_ID,
			PERIOD_START_DATE,
			max(kca_seq_id)
		from
			bec_ods_stg.MSC_SAFETY_STOCKS
		where
			kca_operation IN ('INSERT','UPDATE')
		group by
			PLAN_ID,
			INVENTORY_ITEM_ID,
			ORGANIZATION_ID,
			PERIOD_START_DATE)
);

commit;
 
-- Soft delete
update bec_ods.MSC_SAFETY_STOCKS set IS_DELETED_FLG = 'N';
commit;
update bec_ods.MSC_SAFETY_STOCKS set IS_DELETED_FLG = 'Y'
where (PLAN_ID,INVENTORY_ITEM_ID,ORGANIZATION_ID,PERIOD_START_DATE)  in
(
select PLAN_ID,INVENTORY_ITEM_ID,ORGANIZATION_ID,PERIOD_START_DATE from bec_raw_dl_ext.MSC_SAFETY_STOCKS
where (PLAN_ID,INVENTORY_ITEM_ID,ORGANIZATION_ID,PERIOD_START_DATE,KCA_SEQ_ID)
in 
(
select PLAN_ID,INVENTORY_ITEM_ID,ORGANIZATION_ID,PERIOD_START_DATE,max(KCA_SEQ_ID) as KCA_SEQ_ID 
from bec_raw_dl_ext.MSC_SAFETY_STOCKS
group by PLAN_ID,INVENTORY_ITEM_ID,ORGANIZATION_ID,PERIOD_START_DATE
) 
and kca_operation= 'DELETE'
);
commit;
end;

update
	bec_etl_ctrl.batch_ods_info
set
	last_refresh_date = getdate()
where
	ods_table_name = 'msc_safety_stocks';

commit;