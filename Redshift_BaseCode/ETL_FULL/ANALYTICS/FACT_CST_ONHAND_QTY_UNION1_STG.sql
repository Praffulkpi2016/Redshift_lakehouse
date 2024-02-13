/*
# Copyright(c) 2022 KPI Partners, Inc. All Rights Reserved.
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
# WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
#
# author: KPI Partners, Inc.
# version: 2022.06
# description: This script represents full load approach for TEMPORARY STAGING TABLE.
# File Version: KPI v1.0
*/
begin;

drop table if exists bec_dwh.FACT_CST_ONHAND_QTY_UNION1_STG;

create table bec_dwh.FACT_CST_ONHAND_QTY_UNION1_STG diststyle all sortkey (
  segment1, inventory_item_id, organization_id
) as (
  select 
    segment1, 
    cat_seg1, 
    cat_seg2, 
    category, 
    organization_name, 
    operating_unit, 
    description, 
    primary_unit_of_measure, 
    primary_uom_code, 
    inventory_item_status_code, 
    planning_make_buy_code, 
    mrp_planning_code, 
    inventory_item_id, 
    organization_id, 
    subinventory_code, 
    locator_id, 
    create_transaction_id, 
    lot_number, 
    transaction_date, 
    transaction_source_type_id, 
    transaction_type_id, 
    locator, 
    material_account, 
    asset_inventory, 
    attribute10, 
    attribute11, 
    vmi_flag, 
    -- audit columns
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
    )|| '-' || nvl(segment1, 'NA')|| '-' || nvl(organization_id, 0) || '-' || nvl(locator, 'NA')|| '-' || nvl(lot_number, 'NA')|| '-' || nvl(
      transaction_date, '1900-01-01 12:00:00'
    )|| '-' || nvl(subinventory_code, 'NA')|| '-' || nvl(create_transaction_id, 0) as dw_load_id, 
    getdate() as dw_insert_date, 
    getdate() as dw_update_date 
  from 
    (
      select 
        mmt.segment1, 
        mmt.cat_seg1, 
        mmt.cat_seg2, 
        mmt.category, 
        mmt.organization_name, 
        mmt.operating_unit, 
        mmt.description, 
        mmt.primary_unit_of_measure, 
        mmt.primary_uom_code, 
        mmt.inventory_item_status_code, 
        mmt.planning_make_buy_code, 
        mmt.mrp_planning_code, 
        mmt.inventory_item_id, 
        mmt.organization_id, 
        miq.subinventory_code, 
        miq.locator_id, 
        miq.create_transaction_id, 
        miq.lot_number, 
        mmt.transaction_date, 
        mmt.transaction_source_type_id, 
        mmt.transaction_type_id, 
        mmt.locator, 
        sub.material_account, 
        sub.asset_inventory, 
        sub.attribute10, 
        sub.attribute11, 
        mmt.vmi_flag 
      from 
        (select * from bec_ods.mtl_onhand_quantities_detail where is_deleted_flg <> 'Y') miq, 
        bec_dwh.FACT_CST_ONHAND_QTY_STG mmt, 
        (select * from bec_ods.mtl_secondary_inventories where is_deleted_flg <> 'Y') sub 
      where 
        miq.inventory_item_id = mmt.inventory_item_id 
        and miq.organization_id = mmt.organization_id 
        and miq.create_transaction_id = mmt.transaction_id 
        and nvl(miq.locator_id, -99) = nvl(mmt.locator_id, -99) 
        and mmt.serial_number_control_code = 1 
        and miq.subinventory_code = sub.secondary_inventory_name (+) 
        and miq.organization_id = sub.organization_id (+) 
      group by 
        mmt.segment1, 
        mmt.cat_seg1, 
        mmt.cat_seg2, 
        mmt.category, 
        mmt.organization_name, 
        mmt.operating_unit, 
        mmt.description, 
        mmt.primary_unit_of_measure, 
        mmt.primary_uom_code, 
        mmt.inventory_item_status_code, 
        mmt.planning_make_buy_code, 
        mmt.mrp_planning_code, 
        mmt.inventory_item_id, 
        mmt.organization_id, 
        miq.subinventory_code, 
        miq.locator_id, 
        miq.create_transaction_id, 
        miq.lot_number, 
        mmt.transaction_date, 
        mmt.transaction_source_type_id, 
        mmt.transaction_type_id, 
        mmt.locator, 
        sub.material_account, 
        sub.asset_inventory, 
        sub.attribute10, 
        sub.attribute11, 
        mmt.vmi_flag
    )
);
end;
update 
  bec_etl_ctrl.batch_dw_info 
set 
  load_type = 'I', 
  last_refresh_date = getdate() 
where 
  dw_table_name = 'fact_cst_onhand_qty_union1_stg' 
  and batch_name = 'costing';
commit;
