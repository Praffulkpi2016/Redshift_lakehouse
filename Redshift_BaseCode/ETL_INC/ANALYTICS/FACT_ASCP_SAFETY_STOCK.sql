/*
# Copyright(c) 2022 KPI Partners, Inc. All Rights Reserved.
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
# WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
#
# author: KPI Partners, Inc.
# version: 2022.06
# description: This script represents incremental load approach for Facts.
# File Version: KPI v1.0
*/
begin;
truncate table bec_dwh.FACT_ASCP_SAFETY_STOCK;
insert into bec_dwh.FACT_ASCP_SAFETY_STOCK 
(
  SELECT 
    mss.organization_id, 
    msi.segment1 item, 
    msi.description, 
    msi.primary_uom_code, 
    mss.effectivity_date, 
    mss.safety_stock_quantity, 
    msi.planner_code, 
    pov.agent_name buyer_name, 
    mss.created_by, 
    mss.last_updated_by, 
    mp.employee_id planner_emp_id, 
    cis.item_cost std_cost, 
    (
      cis.item_cost * mss.safety_stock_quantity
    ) extended_cost,
	(select system_id from bec_etl_ctrl.etlsourceappid where source_system = 'EBS')|| '-' || mss.organization_id as organization_id_key,
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
    ) || '-' || nvl(mss.organization_id, 0) || '-' || nvl(item, 'NA') || '-' || nvl(effectivity_date, '1900-01-01 12:00:00') as dw_load_id, 
    getdate() as dw_insert_date, 
    getdate() as dw_update_date -- item,effectivity_date
  FROM 
    (SELECT * FROM bec_ods.mtl_safety_stocks WHERE IS_DELETED_FLG <> 'Y') mss, 
    (SELECT * FROM bec_ods.mtl_system_items_b WHERE IS_DELETED_FLG <> 'Y') msi, 
    (SELECT * FROM bec_ods.mtl_planners WHERE IS_DELETED_FLG <> 'Y') mp, 
    (SELECT * FROM bec_ods.po_agents_v WHERE IS_DELETED_FLG <> 'Y') pov, 
    (SELECT * FROM bec_ods.cst_item_costs WHERE IS_DELETED_FLG <> 'Y') cis 
  WHERE 
    mss.inventory_item_id = msi.inventory_item_id 
    AND mss.organization_id = msi.organization_id 
    AND msi.planner_code = mp.planner_code (+) 
    AND msi.organization_id = mp.organization_id (+) 
    AND msi.buyer_id = pov.agent_id (+) 
    AND cis.inventory_item_id = mss.inventory_item_id 
    AND cis.organization_id = mss.organization_id 
    AND msi.inventory_item_id = cis.inventory_item_id 
    AND msi.organization_id = cis.organization_id 
    AND cost_type_id = '1'
);
commit;
end;
update 
  bec_etl_ctrl.batch_dw_info 
set 
  load_type = 'I', 
  last_refresh_date = getdate() 
where 
  dw_table_name = 'fact_ascp_safety_stock' 
  and batch_name = 'ascp';
commit;
