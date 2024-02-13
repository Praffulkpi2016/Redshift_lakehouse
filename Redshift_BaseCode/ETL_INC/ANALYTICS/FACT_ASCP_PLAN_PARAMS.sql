/*
# Copyright(c) 2022 KPI Partners, Inc. All Rights Reserved.
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
# WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
#
# author: KPI Partners, Inc.
# version: 2022.06
# description: This script represents incremental load approach for fact table.
# File Version: KPI v1.0
*/
begin;
truncate table bec_dwh.FACT_ASCP_PLAN_PARAMS;
insert into bec_dwh.FACT_ASCP_PLAN_PARAMS (
  WITH safety_stock AS (
    SELECT 
      safety_stock_quantity, 
      inventory_item_id, 
      organization_id 
    FROM 
      bec_ods.MTL_SAFETY_STOCKS 
    WHERE IS_DELETED_FLG <> 'Y'
	AND (
        effectivity_date, organization_id, 
        inventory_item_id
      ) IN (
        SELECT 
          max(effectivity_date) effectivity_date, 
          organization_id, 
          inventory_item_id 
        FROM 
          bec_ods.MTL_SAFETY_STOCKS
        WHERE 
          1 = 1 
		AND IS_DELETED_FLG <> 'Y'
        GROUP BY 
          organization_id, 
          inventory_item_id
      )
  ), 
  onhand_qty AS (
    SELECT 
      SUM(transaction_quantity) onhand_quantity, 
      moq.organization_id moq_org_id, 
      inventory_item_id moq_item_id, 
      ms.availability_type 
    FROM 
      (SELECT * FROM bec_ods.mtl_onhand_quantities WHERE IS_DELETED_FLG <> 'Y') moq, 
      (SELECT * FROM bec_ods.mtl_secondary_inventories WHERE IS_DELETED_FLG <> 'Y') ms 
    WHERE 
      moq.subinventory_code = ms.secondary_inventory_name 
      AND moq.organization_id = ms.organization_id 
      AND ms.availability_type IN (1, 2) 
    GROUP BY 
      moq.organization_id, 
      inventory_item_id, 
      ms.availability_type
  ), 
  mtl_planners AS (
    SELECT 
      planner_code, 
      organization_id, 
      employee_id 
    FROM 
      bec_ods.mtl_planners
    WHERE IS_DELETED_FLG <> 'Y'
	AND
      (
        disable_date IS NULL 
        OR disable_date > getdate()
      )
  )
  
  select 
    msi.segment1 item_number, 
    msi.description, 
    msi.creation_date, 
    msi.primary_uom_code, 
    msi.primary_unit_of_measure, 
    DECODE(
      msi.planning_make_buy_code, 1, 'Make', 
      2, 'Buy'
    ) make_buy_flag, 
    msi.inventory_item_status_code, 
    cic.item_cost std_unit_cost, 
    pob.agent_name buyer, 
    msi.postprocessing_lead_time, 
    msi.preprocessing_lead_time, 
    ml2.meaning min_max_planned, 
    msi.fixed_lead_time, 
    msi.cum_manufacturing_lead_time, 
    msi.cumulative_total_lead_time, 
    ml3.meaning mrp_planned, 
    msi.planner_code, 
    msi.full_lead_time, 
    msi.min_minmax_quantity, 
    msi.max_minmax_quantity, 
    msi.minimum_order_quantity, 
    msi.maximum_order_quantity, 
    msi.fixed_order_quantity, 
    msi.fixed_days_supply, 
    msi.planning_time_fence_code, 
    msi.planning_time_fence_days, 
    ml1.meaning wip_supply_type, 
    1 category_set_id, 
    mss.safety_stock_quantity, 
    msi.organization_id, 
    msi.inventory_item_id, 
    DECODE(
      msi.lot_control_code, 1, 'N', 2, 'Y'
    ) lot_control, 
    DECODE(
      msi.serial_number_control_code, 
      1, 'N', 'Y'
    ) serial_control, 
    msi.comms_nl_trackable_flag install_base_trackable, 
    DECODE(
      msi.serv_req_enabled_code, 'E', 'Enabled', 
      'D', 'Disabled', 'I', 'Inactive', 
      NULL
    ) service_request_enabled, 
    msi.shrinkage_rate, 
    msi.inspection_required_flag, 
    msi.release_time_fence_code, 
    msi.release_time_fence_days, 
    moh.ONHAND_QUANTITY, 
    moh_nn.ONHAND_QUANTITY NN_onhand_quantity, 
    msi.attribute5 program_name, 
--    mp.planner_code, 
    DECODE(
      mp.planner_code, NULL, 'Inactive', 
      'Active'
    ) planner_status, 
    mp.employee_id planner_emp_id, 
	(select system_id from bec_etl_ctrl.etlsourceappid where source_system = 'EBS')|| '-' || msi.organization_id as organization_id_key,
	(select system_id from bec_etl_ctrl.etlsourceappid where source_system = 'EBS')|| '-' || msi.inventory_item_id as inventory_item_id_key,
    --audit COLUMNS
    'N' as is_deleted_flg, 
    (
      select 
        system_id 
      from 
        bec_etl_ctrl.etlsourceappid 
      where 
        source_system = 'EBS'
    ) as source_app_id, 
    (
      select 
        system_id 
      from 
        bec_etl_ctrl.etlsourceappid 
      where 
        source_system = 'EBS'
    ) || '-' || nvl(msi.organization_id, 0) || '-' || nvl(msi.inventory_item_id, 0) as dw_load_id, 
    getdate() as dw_insert_date, 
    getdate() as dw_update_date 
  FROM 
    (SELECT * FROM bec_ods.mtl_system_items_b WHERE IS_DELETED_FLG <> 'Y') msi, 
    (SELECT * FROM bec_ods.cst_item_costs WHERE IS_DELETED_FLG <> 'Y') cic, 
    (SELECT * FROM bec_ods.po_agents_v WHERE IS_DELETED_FLG <> 'Y') pob, 
    (SELECT * FROM bec_ods.fnd_lookup_values WHERE IS_DELETED_FLG <> 'Y') ml1, 
    (SELECT * FROM bec_ods.fnd_lookup_values WHERE IS_DELETED_FLG <> 'Y') ml2, 
    (SELECT * FROM bec_ods.fnd_lookup_values WHERE IS_DELETED_FLG <> 'Y') ml3, 
    safety_stock mss, 
    mtl_planners mp, 
    onhand_qty moh, 
    onhand_qty moh_nn 
  WHERE 
    msi.inventory_item_id = cic.inventory_item_id(+) 
    AND msi.organization_id = cic.organization_id(+) 
    AND msi.buyer_id = pob.agent_id(+) 
    AND msi.wip_supply_type = ml1.lookup_code(+) 
    AND msi.inventory_item_id = mss.inventory_item_id(+) 
    AND msi.organization_id = mss.organization_id(+) 
    AND ml1.lookup_type(+) = 'WIP_SUPPLY' 
    AND msi.inventory_planning_code = ml2.lookup_code(+) 
    AND ml2.lookup_type(+) = 'MTL_MATERIAL_PLANNING' 
    AND msi.mrp_planning_code = ml3.lookup_code(+) 
    AND ml3.lookup_type(+) = 'MRP_PLANNING_CODE' 
    AND cic.cost_type_id(+) = 1 
    AND msi.planner_code = mp.planner_code(+) 
    AND msi.organization_id = mp.organization_id(+) 
    AND msi.organization_id = moh.moq_org_id(+) 
    AND msi.inventory_item_id = moh.moq_item_id(+) 
    AND moh.availability_type(+) = 1 
    AND msi.organization_id = moh_nn.moq_org_id(+) 
    AND msi.inventory_item_id = moh_nn.moq_item_id(+) 
    AND moh_nn.availability_type(+) = 2
);
end;
update 
  bec_etl_ctrl.batch_dw_info 
set 
  load_type = 'I', 
  last_refresh_date = getdate() 
where 
  dw_table_name = 'fact_ascp_plan_params' 
  and batch_name = 'ascp';
commit;