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
	drop table if exists bec_dwh.DIM_ASCP_ITEMS;
	
	CREATE TABLE bec_dwh.dim_ascp_items 
	DISTKEY(plan_id)
SORTKEY(organization_id, inventory_item_id, plan_id)
	AS
	(
SELECT
		plan_id,
		sr_instance_id,
		inventory_item_id,
		item_segments,
		organization_id,
		standard_cost,
		planning_make_buy_code,
		planning_make_buy_code_text,
		description,
		buyer_name,
		planner_code,
		mrp_planning_code_text,
		preprocessing_lead_time,
		postprocessing_lead_time,
		processing_lead_time,
		fixed_lead_time,
		variable_lead_time,
		selling_price,
		margin,
		rounding_control_type,
		repetitive_type,
		carrying_cost,    
		wip_supply_type_text,
		abc_class,
		abc_class_name,
		fixed_days_supply,
		fixed_order_quantity,
		fixed_lot_multiplier,
		minimum_order_quantity,
		maximum_order_quantity,
		shrinkage_rate,
		planning_exception_set,
		base_item_id,
		planning_time_fence_date,
		planning_time_fence_days,
		uom_code,
		inventory_use_up_date,
		end_assembly_pegging,
		full_pegging,
		full_pegging_text,
		safety_stock_days,
		safety_stock_percent,
		fixed_safety_stock_qty,
		atp_flag,
		atp_components_flag,
		category_set_id,
		category,
		category_desc,
		'N' as is_deleted_flg,
		(
			SELECT
            system_id
			FROM
            bec_etl_ctrl.etlsourceappid
			WHERE
            source_system = 'EBS'
		)           AS source_app_id,
		(
			SELECT
            system_id
			FROM
            bec_etl_ctrl.etlsourceappid
			WHERE
            source_system = 'EBS'
		)
		|| '-'|| nvl(inventory_item_id,0) 
		|| '-'|| nvl(organization_id,0) 
		|| '-'|| nvl(plan_id,0) 
		|| '-'|| nvl(sr_instance_id,0) 
		|| '-'|| nvl(category_set_id,0) 
		AS dw_load_id,
		getdate()           AS dw_insert_date,
		getdate()           AS dw_update_date
		FROM
		(
			SELECT
			msi.plan_id,
			msi.sr_instance_id,
			msi.inventory_item_id,
			msi.item_name                                      item_segments,
			msi.organization_id,
			msi.standard_cost,
			msi.planning_make_buy_code,
			(
				SELECT
				meaning
				FROM
				bec_ods.fnd_lookup_values
				WHERE
                lookup_type = 'MTL_PLANNING_MAKE_BUY'
				AND lookup_code = msi.planning_make_buy_code
				AND language = 'US'
			)                                                  planning_make_buy_code_text,
			msi.description,
			msi.buyer_name,
			msi.planner_code,
			(
				SELECT
				meaning
				FROM
				bec_ods.fnd_lookup_values
				WHERE
                lookup_type = 'MRP_PLANNING_CODE'
				AND lookup_code = msi.mrp_planning_code
				AND language = 'US'
			)                                                  mrp_planning_code_text,
			msi.preprocessing_lead_time,
			msi.postprocessing_lead_time,
			msi.full_lead_time                                 processing_lead_time,
			msi.fixed_lead_time,
			msi.variable_lead_time,
			msi.list_price                                     selling_price,
			nvl(msi.list_price, 0) - nvl(msi.standard_cost, 0) margin,
			msi.rounding_control_type,
			msi.repetitive_type,
			msi.carrying_cost,
			(
				SELECT
				meaning
				FROM
				bec_ods.fnd_lookup_values
				WHERE
                lookup_type = 'WIP_SUPPLY'
				AND lookup_code = msi.wip_supply_type
				AND language = 'US'
			)                                                  wip_supply_type_text,
			msi.abc_class,
			msi.abc_class_name,
			msi.fixed_days_supply,
			msi.fixed_order_quantity,
			msi.fixed_lot_multiplier,
			msi.minimum_order_quantity,
			msi.maximum_order_quantity,
			msi.shrinkage_rate,
			msi.planning_exception_set,
			msi.base_item_id,
			msi.planning_time_fence_date,
			msi.planning_time_fence_days,
			msi.uom_code,
			msi.inventory_use_up_date,
			msi.end_assembly_pegging_flag                      end_assembly_pegging,
			msi.full_pegging,
			(
				SELECT
				meaning
				FROM
				bec_ods.fnd_lookup_values
				WHERE
                lookup_type = 'MRP_HARD_PEGGING_LEVEL'
				AND lookup_code = msi.full_pegging
				AND language = 'US'
			)                                                  full_pegging_text,
			msi.safety_stock_bucket_days                       safety_stock_days,
			msi.safety_stock_percent,
			msi.fixed_safety_stock_qty                         fixed_safety_stock_qty,
			(
				SELECT
				meaning
				FROM
				bec_ods.fnd_lookup_values
				WHERE
                lookup_type = 'MSC_ATP_COMPONENTS_FLAG'
				AND lookup_code = decode(msi.atp_flag, 'N', 1, 'Y', 2,
				'R', 3, 'C', 4, 4)
				AND language = 'US'
			)                                                  atp_flag,
			(
				SELECT
				meaning
				FROM
				bec_ods.fnd_lookup_values
				WHERE
                lookup_type = 'MSC_ATP_COMPONENTS_FLAG'
				AND lookup_code = decode(msi.atp_components_flag, 'N', 1, 'Y', 2,
				'R', 3, 'C', 4, 4)
				AND language = 'US'
			)                                                  atp_components_flag,
			mic.category_set_id                                category_set_id,
			mic.category_name                                  category,
			mic.description                                    category_desc
			FROM
			bec_ods.msc_system_items    msi,
			bec_ods.msc_plans           plans,
			bec_ods.msc_item_categories mic
			WHERE
			1 = 1
			AND msi.plan_id = plans.plan_id
			AND msi.sr_instance_id = plans.sr_instance_id
			AND msi.sr_instance_id = mic.sr_instance_id
			AND msi.inventory_item_id = mic.inventory_item_id
			AND msi.organization_id = mic.organization_id
		)
			);
	
end;


UPDATE bec_etl_ctrl.batch_dw_info
SET
load_type = 'I',
last_refresh_date = getdate()
WHERE
dw_table_name = 'dim_ascp_items' 
and batch_name = 'ascp';

COMMIT;