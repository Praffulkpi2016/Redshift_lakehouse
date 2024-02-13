/*
# Copyright(c) 2022 KPI Partners, Inc. All Rights Reserved.
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
# WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
#
# author: KPI Partners, Inc.
# version: 2022.06
# description: This script represents full load approach for Facts.
# File Version: KPI v1.0
*/
begin;

drop table if exists bec_dwh.FACT_INV_OHQ_DETAILS;

create table bec_dwh.FACT_INV_OHQ_DETAILS 
distkey(serial_number)
sortkey(inventory_item_id,organization_id)
 as 
SELECT 
  part_number, 
  description, 
  primary_unit_of_measure, 
  subinventory, 
  locator_id, 
  serial_number, 
  lot_number, 
  QUANTITY, 
  organization_id, 
  inventory_item_id, 
  sub_system, 
  sub_class, 
  (select system_id from bec_etl_ctrl.etlsourceappid where source_system='EBS')||'-'||locator_id   locator_id_KEY,
  (select system_id from bec_etl_ctrl.etlsourceappid where source_system='EBS')||'-'||organization_id   organization_id_KEY,
  (select system_id from bec_etl_ctrl.etlsourceappid where source_system='EBS')||'-'||inventory_item_id   inventory_item_id_KEY,
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
  ) || '-' || nvl(inventory_item_id, 0)
  || '-' || nvl(organization_id, 0)
  || '-' || nvl(subinventory, 'NA')
  || '-' || nvl(locator_id, 0)
  || '-' || nvl(serial_number, 'NA')
  || '-' || nvl(lot_number, 'NA')  as dw_load_id,
  getdate() as dw_insert_date, 
  getdate() as dw_update_date 
FROM 
  (
    SELECT 
      msi.segment1 AS part_number, 
      msi.description, 
      msi.primary_unit_of_measure, 
      miq.subinventory_code AS subinventory, 
      miq.locator_id, 
      NULL serial_number, 
      miq.lot_number, 
      SUM (
        miq.primary_transaction_quantity
      ) as QUANTITY, 
      msi.organization_id, 
      msi.inventory_item_id, 
      mc.SEGMENT1 sub_system, 
      mc.SEGMENT2 sub_class 
    FROM 
      (select * from bec_ods.mtl_system_items_b where is_deleted_flg <> 'Y') msi, 
      (select * from bec_ods.mtl_onhand_quantities_detail where is_deleted_flg <> 'Y') miq, 
      (select * from bec_ods.mtl_categories_b where is_deleted_flg <> 'Y') mc, 
      (select * from bec_ods.mtl_item_categories where is_deleted_flg <> 'Y') mic 
    WHERE 
      msi.inventory_item_id = miq.inventory_item_id 
      AND miq.organization_id = msi.organization_id 
      AND miq.organization_id = miq.owning_organization_id 
      AND msi.serial_number_control_code = 1 
      AND miq.organization_id = mic.ORGANIZATION_ID(+) 
      and miq.INVENTORY_ITEM_ID = mic.INVENTORY_ITEM_ID(+) 
      and mic.CATEGORY_SET_ID(+) = 1 
      and mic.CATEGORY_ID = mc.CATEGORY_ID(+) 
    GROUP BY 
      msi.segment1, 
      msi.description, 
      msi.primary_unit_of_measure, 
      miq.subinventory_code, 
      miq.locator_id, 
      miq.lot_number, 
      msi.organization_id, 
      msi.inventory_item_id, 
      mc.SEGMENT1, 
      mc.SEGMENT2 
    UNION ALL 
    SELECT 
      DISTINCT msi.segment1 AS part_number, 
      msi.description, 
      msi.primary_unit_of_measure, 
      msn.current_subinventory_code AS subinventory, 
      msn.current_locator_id as locator_id, 
      msn.serial_number, 
      msn.lot_number, 
      1 quantity, 
      msi.organization_id, 
      msi.inventory_item_id, 
      mc.SEGMENT1 sub_system, 
      mc.SEGMENT2 sub_class 
    FROM 
      (select * from bec_ods.mtl_system_items_b where is_deleted_flg <> 'Y') msi, 
      (select * from bec_ods.mtl_serial_numbers where is_deleted_flg <> 'Y') msn, 
      (select * from bec_ods.mtl_categories_b where is_deleted_flg <> 'Y') mc, 
      (select * from bec_ods.mtl_item_categories where is_deleted_flg <> 'Y') mic 
    WHERE 
      msi.inventory_item_id = msn.inventory_item_id 
      AND msn.current_organization_id = msi.organization_id 
      AND msn.current_organization_id = msn.owning_organization_id 
      AND msn.current_status = 3 
      AND msi.serial_number_control_code <> 1 
      AND msn.CURRENT_ORGANIZATION_ID = mic.ORGANIZATION_ID(+) 
      and msn.INVENTORY_ITEM_ID = mic.INVENTORY_ITEM_ID(+) 
      and mic.CATEGORY_SET_ID(+) = 1 
      and mic.CATEGORY_ID = mc.CATEGORY_ID(+) 
    ORDER BY 
      subinventory, 
      part_number, 
      locator_id, 
      serial_number, 
      organization_id ASC
  );
  
end;
update 
  bec_etl_ctrl.batch_dw_info 
set 
  load_type = 'I', 
  last_refresh_date = getdate() 
where 
  dw_table_name = 'fact_inv_ohq_details' 
  and batch_name = 'inv';
commit;