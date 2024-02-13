/*
# Copyright(c) 2022 KPI Partners, Inc. All Rights Reserved.
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
# WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
#
# author: KPI Partners, Inc.
# version: 2022.06
# description: This script represents incremental load approach for RT reports.
# File Version: KPI v1.0
*/
begin;

Truncate table bec_dwh_rpt.FACT_ASCP_HORIZONTAL_PLAN_RT;

Insert Into bec_dwh_rpt.FACT_ASCP_HORIZONTAL_PLAN_RT
(
with recursive items(plan_id,
organization_id,
inventory_item_id,
using_assembly_id,
level,
root_assembly
--,path
) as
(
select
	distinct
     bom1.plan_id,
	bom1.organization_id,
	bom1.assembly_item_id inventory_item_id,
	bom1.assembly_item_id using_assembly_id,
	0 as level,
	bom1.assembly_item_id root_assembly
from
	bec_ods.msc_boms bom1,
	bec_ods.msc_system_items msi ,
	bec_dwh.fact_ascp_hp_items_stg fahp2
where
	alternate_bom_designator is null
	and msi.inventory_item_id = bom1.assembly_item_id
	and msi.planning_make_buy_code = 1
	and msi.plan_id = bom1.plan_id
	and msi.organization_id = bom1.organization_id
	and fahp2.plan_id = bom1.plan_id
	and fahp2.organization_id = bom1.organization_id
	and fahp2.sr_instance_id = bom1.sr_instance_id
	and fahp2.inventory_item_id = bom1.assembly_item_id
union all
select
	mbc.plan_id,
	mbc.organization_id,
	mbc.inventory_item_id,
	mbc.using_assembly_id,
	level + 1 as level,
	items.root_assembly
from
	bec_ods.msc_bom_components mbc,
	bec_dwh.fact_ascp_hp_items_stg fahp3,
	items
where
	sysdate between mbc.effectivity_date and nvl(mbc.disable_date, sysdate)
	and mbc.effectivity_date <= sysdate
	and fahp3.plan_id = mbc.plan_id
	and fahp3.organization_id = mbc.organization_id
	and fahp3.sr_instance_id = mbc.sr_instance_id
	and fahp3.inventory_item_id = mbc.inventory_item_id
	and mbc.plan_id = items.plan_id
	and mbc.organization_id = items.organization_id
	and mbc.USING_ASSEMBLY_ID = items.inventory_item_id 
  ) 
select
	"dim_ascp_items"."abc_class" as "abc_class",
	"dim_ascp_items"."abc_class_name" as "abc_class_name",
	"dim_ascp_items"."atp_components_flag" as "atp_components_flag",
	"dim_ascp_items"."atp_flag" as "atp_flag",
	"dim_ascp_items"."base_item_id" as "base_item_id",
	"dim_ascp_items"."buyer_name" as "buyer_name",
	"dim_ascp_items"."carrying_cost" as "carrying_cost",
	"dim_ascp_items"."category" as "category",
	"dim_ascp_items"."category_desc" as "category_desc",
	"dim_ascp_items"."category_set_id" as "category_set_id (dim_ascp_items)",
	"fact_ascp_horizontal_plan"."category_set_id" as "category_set_id",
	"dim_ascp_plans"."compile_designator" as "compile_designator",
	"dim_ascp_plans"."curr_cutoff_date" as "curr_cutoff_date",
	"dim_ascp_plans"."curr_plan_type" as "curr_plan_type",
	"dim_ascp_plans"."curr_start_date" as "curr_start_date",
	"dim_ascp_plans"."cutoff_date" as "cutoff_date",
	"dim_ascp_plans"."daily_cutoff_bucket" as "daily_cutoff_bucket",
	"dim_ascp_plans"."data_completion_date" as "data_completion_date",
	"dim_ascp_plans"."data_start_date" as "data_start_date",
	"dim_ascp_items"."description" as "description (dim_ascp_items)",
	"dim_ascp_plans"."description" as "description",
	"dim_ascp_items"."dw_insert_date" as "dw_insert_date (dim_ascp_items)",
	"dim_ascp_organizations"."dw_insert_date" as "dw_insert_date (dim_ascp_organizations)",
	"dim_ascp_plans"."dw_insert_date" as "dw_insert_date (dim_ascp_plans)",
	"fact_ascp_horizontal_plan"."dw_insert_date" as "dw_insert_date",
	"dim_ascp_items"."dw_load_id" as "dw_load_id (dim_ascp_items)",
	"dim_ascp_organizations"."dw_load_id" as "dw_load_id (dim_ascp_organizations)",
	"dim_ascp_plans"."dw_load_id" as "dw_load_id (dim_ascp_plans)",
	cast("fact_ascp_horizontal_plan"."dw_load_id" as TEXT) as "dw_load_id",
	"dim_ascp_items"."dw_update_date" as "dw_update_date (dim_ascp_items)",
	"dim_ascp_organizations"."dw_update_date" as "dw_update_date (dim_ascp_organizations)",
	"dim_ascp_plans"."dw_update_date" as "dw_update_date (dim_ascp_plans)",
	"fact_ascp_horizontal_plan"."dw_update_date" as "dw_update_date",
	"dim_ascp_items"."end_assembly_pegging" as "end_assembly_pegging",
	"fact_ascp_horizontal_plan"."fill_kill_flag" as "fill_kill_flag",
	"dim_ascp_items"."fixed_days_supply" as "fixed_days_supply",
	"dim_ascp_items"."fixed_lead_time" as "fixed_lead_time",
	"dim_ascp_items"."fixed_lot_multiplier" as "fixed_lot_multiplier",
	"dim_ascp_items"."fixed_order_quantity" as "fixed_order_quantity",
	"dim_ascp_items"."fixed_safety_stock_qty" as "fixed_safety_stock_qty",
	"dim_ascp_items"."full_pegging" as "full_pegging",
	"dim_ascp_items"."full_pegging_text" as "full_pegging_text",
	"dim_ascp_items"."inventory_item_id" as "inventory_item_id (dim_ascp_items)",
	"fact_ascp_horizontal_plan"."inventory_item_id" as "inventory_item_id",
	"fact_ascp_horizontal_plan"."inventory_item_id_key" as "inventory_item_id_key",
	"dim_ascp_items"."inventory_use_up_date" as "inventory_use_up_date",
	"dim_ascp_items"."is_deleted_flg" as "is_deleted_flg (dim_ascp_items)",
	"dim_ascp_organizations"."is_deleted_flg" as "is_deleted_flg (dim_ascp_organizations)",
	"dim_ascp_plans"."is_deleted_flg" as "is_deleted_flg (dim_ascp_plans)",
	"fact_ascp_horizontal_plan"."is_deleted_flg" as "is_deleted_flg",
	"dim_ascp_items"."item_segments" as "item_segments",
	"dim_ascp_items"."margin" as "margin",
	"dim_ascp_organizations"."master_organization" as "master_organization",
	"dim_ascp_items"."maximum_order_quantity" as "maximum_order_quantity",
	"dim_ascp_items"."minimum_order_quantity" as "minimum_order_quantity",
	"dim_ascp_items"."mrp_planning_code_text" as "mrp_planning_code_text",
	"fact_ascp_horizontal_plan"."new_due_date" as "new_due_date",
	"dim_ascp_organizations"."operating_unit" as "operating_unit",
	"fact_ascp_horizontal_plan"."order_type_entity" as "order_type_entity",
	"dim_ascp_organizations"."organization_code" as "organization_code",
	"dim_ascp_items"."organization_id" as "organization_id (dim_ascp_items)",
	"dim_ascp_organizations"."organization_id" as "organization_id (dim_ascp_organizations)",
	"fact_ascp_horizontal_plan"."organization_id" as "organization_id",
	"fact_ascp_horizontal_plan"."organization_id_key" as "organization_id_key",
	"dim_ascp_organizations"."partner_id" as "partner_id",
	"dim_ascp_organizations"."partner_name" as "partner_name",
	"dim_ascp_items"."plan_id" as "plan_id (dim_ascp_items)",
	"dim_ascp_plans"."plan_id" as "plan_id (dim_ascp_plans)",
	"fact_ascp_horizontal_plan"."plan_id" as "plan_id",
	"fact_ascp_horizontal_plan"."plan_id_key" as "plan_id_key",
	"dim_ascp_items"."planner_code" as "planner_code",
	"dim_ascp_items"."planning_exception_set" as "planning_exception_set",
	"dim_ascp_items"."planning_make_buy_code" as "planning_make_buy_code",
	"dim_ascp_items"."planning_make_buy_code_text" as "planning_make_buy_code_text",
	"dim_ascp_items"."planning_time_fence_date" as "planning_time_fence_date",
	"dim_ascp_items"."planning_time_fence_days" as "planning_time_fence_days",
	"dim_ascp_items"."postprocessing_lead_time" as "postprocessing_lead_time",
	"dim_ascp_items"."preprocessing_lead_time" as "preprocessing_lead_time",
	"dim_ascp_items"."processing_lead_time" as "processing_lead_time",
	"fact_ascp_horizontal_plan"."quantity_rate" as "quantity_rate",
	"dim_ascp_items"."repetitive_type" as "repetitive_type",
	"dim_ascp_items"."rounding_control_type" as "rounding_control_type",
	"dim_ascp_items"."safety_stock_days" as "safety_stock_days",
	"dim_ascp_items"."safety_stock_percent" as "safety_stock_percent",
	"dim_ascp_items"."selling_price" as "selling_price",
	"dim_ascp_items"."shrinkage_rate" as "shrinkage_rate",
	"fact_ascp_horizontal_plan"."so_line_split" as "so_line_split",
	"dim_ascp_items"."source_app_id" as "source_app_id (dim_ascp_items)",
	"dim_ascp_organizations"."source_app_id" as "source_app_id (dim_ascp_organizations)",
	"dim_ascp_plans"."source_app_id" as "source_app_id (dim_ascp_plans)",
	"fact_ascp_horizontal_plan"."source_app_id" as "source_app_id",
	"dim_ascp_items"."sr_instance_id" as "sr_instance_id (dim_ascp_items)",
	"dim_ascp_organizations"."sr_instance_id" as "sr_instance_id (dim_ascp_organizations)",
	"dim_ascp_plans"."sr_instance_id" as "sr_instance_id (dim_ascp_plans)",
	"fact_ascp_horizontal_plan"."sr_instance_id" as "sr_instance_id",
	"dim_ascp_items"."standard_cost" as "standard_cost",
	"dim_ascp_items"."uom_code" as "uom_code",
	"dim_ascp_items"."variable_lead_time" as "variable_lead_time",
	"dim_ascp_plans"."weekly_cutoff_bucket" as "weekly_cutoff_bucket",
	"dim_ascp_items"."wip_supply_type_text" as "wip_supply_type_text",
	i.level,
	"dim_ascp_items_2"."item_segments" as "root_item_segments"
from
	"bec_dwh"."fact_ascp_horizontal_plan" "fact_ascp_horizontal_plan"
left join "bec_dwh"."dim_ascp_plans" "dim_ascp_plans" on
	(("fact_ascp_horizontal_plan"."plan_id" = "dim_ascp_plans"."plan_id")
		and ("fact_ascp_horizontal_plan"."sr_instance_id" = "dim_ascp_plans"."sr_instance_id"))
left join "bec_dwh"."dim_ascp_organizations" "dim_ascp_organizations" 
  on
	(("fact_ascp_horizontal_plan"."organization_id" = "dim_ascp_organizations"."organization_id")
		and ("fact_ascp_horizontal_plan"."sr_instance_id" = "dim_ascp_organizations"."sr_instance_id")
			and "dim_ascp_organizations".is_deleted_flg = 'N')
left join "bec_dwh"."dim_ascp_items" "dim_ascp_items" 
  on
	(("fact_ascp_horizontal_plan"."inventory_item_id" = "dim_ascp_items"."inventory_item_id")
		and ("fact_ascp_horizontal_plan"."organization_id" = "dim_ascp_items"."organization_id")
			and ("fact_ascp_horizontal_plan"."plan_id" = "dim_ascp_items"."plan_id")
				and ("fact_ascp_horizontal_plan"."category_set_id" = "dim_ascp_items"."category_set_id"))
inner join items i on
	fact_ascp_horizontal_plan.plan_id = i.plan_id
	and fact_ascp_horizontal_plan.organization_id = i.organization_id
	and fact_ascp_horizontal_plan.inventory_item_id = i.inventory_item_id
left join "bec_dwh"."dim_ascp_items" "dim_ascp_items_2" 
  on
	((i."root_assembly" = "dim_ascp_items_2"."inventory_item_id")
		and (i."organization_id" = "dim_ascp_items_2"."organization_id")
			and (i."plan_id" = "dim_ascp_items_2"."plan_id")
				and ("dim_ascp_items"."category_set_id" = 9)
  )
)  ;
  
 end;
 
 update
	bec_etl_ctrl.batch_dw_info
set
	load_type = 'I',
	last_refresh_date = getdate()
where
	dw_table_name = 'fact_ascp_horizontal_plan_rt'
	and batch_name = 'ascp';

commit;