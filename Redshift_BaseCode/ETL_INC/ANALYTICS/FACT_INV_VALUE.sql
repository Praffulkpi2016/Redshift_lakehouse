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
--delete 
truncate table bec_dwh.FACT_INV_VALUE_TMP;

insert into bec_dwh.FACT_INV_VALUE_TMP
(select distinct msi.inventory_item_id,miq.organization_id
FROM 
bec_ods.mtl_system_items_b  msi, 
bec_ods.mtl_secondary_inventories  sub, 
 (
    SELECT 
      organization_id, 
      owning_organization_id, 
      inventory_item_id, 
      subinventory_code, 
      locator_id, 
      revision, 
      SUM (primary_transaction_quantity) quantity 
    FROM 
      bec_ods.mtl_onhand_quantities_detail 
	 where 1=1
	 and kca_seq_date > (select (executebegints-prune_days) 
from bec_etl_ctrl.batch_dw_info 
where dw_table_name ='fact_inv_value' and batch_name = 'inv')
    GROUP BY 
      organization_id, 
      owning_organization_id, 
      inventory_item_id, 
      subinventory_code, 
      locator_id, 
      revision
  ) miq, 
bec_ods.cst_item_costs  ctc 
WHERE 
  msi.inventory_item_id = miq.inventory_item_id 
  AND miq.organization_id = msi.organization_id 
  AND msi.inventory_item_id = ctc.inventory_item_id 
  AND ctc.organization_id = msi.organization_id 
  AND miq.subinventory_code = sub.secondary_inventory_name 
  AND miq.organization_id = sub.organization_id 
 -- AND ctc.cost_type_id = '1'
);
commit;

delete from bec_dwh.FACT_INV_VALUE 
where exists 
(select 1 
from bec_dwh.FACT_INV_VALUE_TMP 
where inventory_item_id = FACT_INV_VALUE.inventory_item_id
and organization_id = FACT_INV_VALUE.organization_id);

--insert records
insert into bec_dwh.FACT_INV_VALUE 
SELECT 
  ctc.cost_type_id, 
  miq.locator_id, 
  msi.inventory_item_id, 
  msi.buyer_id, 
  miq.subinventory_code AS subinventory, 
  msi.segment1 AS part_number, 
  msi.description, 
  msi.inventory_item_status_code, 
  msi.primary_uom_code, 
  miq.quantity, 
  ctc.material_cost, 
  ctc.material_overhead_cost, 
  ctc.resource_cost, 
  ctc.outside_processing_cost, 
  ctc.overhead_cost, 
  ctc.item_cost, 
  miq.quantity * ctc.material_cost as tot_mat_cost, 
  miq.quantity * ctc.material_overhead_cost as tot_mat_oh_cost, 
  miq.quantity * ctc.resource_cost as tot_resource_cost, 
  miq.quantity * ctc.outside_processing_cost as tot_osp_cost, 
  miq.quantity * ctc.overhead_cost as tot_oh_cost, 
  (miq.quantity * ctc.item_cost) AS extended_cost, 
  miq.organization_id, 
  DECODE (
    miq.owning_organization_id, miq.organization_id,
    'N', 'Y'
  ) vmi_flag, 
  miq.owning_organization_id, 
  DECODE (
    sub.asset_inventory, 1, 'Asset', 2, 
    'Expense', asset_inventory :: char
  ) subinventory_type,
  (select system_id from bec_etl_ctrl.etlsourceappid where source_system='EBS')||'-'||ctc.cost_type_id   cost_type_id_KEY,
  (select system_id from bec_etl_ctrl.etlsourceappid where source_system='EBS')||'-'||miq.locator_id   locator_id_KEY,
  (select system_id from bec_etl_ctrl.etlsourceappid where source_system='EBS')||'-'||msi.inventory_item_id   inventory_item_id_KEY,
  (select system_id from bec_etl_ctrl.etlsourceappid where source_system='EBS')||'-'||msi.buyer_id   buyer_id_KEY,
  (select system_id from bec_etl_ctrl.etlsourceappid where source_system='EBS')||'-'||miq.organization_id   organization_id_KEY,
  (select system_id from bec_etl_ctrl.etlsourceappid where source_system='EBS')||'-'||miq.owning_organization_id   owning_organization_id_KEY,
  --Added "Program name" column requested by Spoorthi - DQSM-415
  msi.attribute5 as program_name,
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
  ) || '-' || nvl(msi.inventory_item_id, 0)
	|| '-' || nvl(miq.organization_id, 0)
	|| '-' || nvl(subinventory, 'NA')
	|| '-' || nvl(miq.locator_id,0)
	|| '-' || nvl(miq.owning_organization_id,0)	as dw_load_id,
  getdate() as dw_insert_date, 
  getdate() as dw_update_date
FROM 
  (select * from bec_ods.mtl_system_items_b where is_deleted_flg <> 'Y') msi, 
  (select * from bec_ods.mtl_secondary_inventories where is_deleted_flg <> 'Y') sub, 
  (
    SELECT 
      organization_id, 
      owning_organization_id, 
      inventory_item_id, 
      subinventory_code, 
      locator_id, 
      revision, 
      SUM (primary_transaction_quantity) quantity 
    FROM 
      bec_ods.mtl_onhand_quantities_detail 
	where is_deleted_flg <> 'Y' 
    GROUP BY 
      organization_id, 
      owning_organization_id, 
      inventory_item_id, 
      subinventory_code, 
      locator_id, 
      revision
  ) miq, 
  (select * from bec_ods.cst_item_costs where is_deleted_flg <> 'Y') ctc ,
  bec_dwh.FACT_INV_VALUE_TMP TMP
WHERE 
  tmp.inventory_item_id = msi.inventory_item_id 
  AND tmp.organization_id = miq.organization_id 
and msi.inventory_item_id = miq.inventory_item_id 
  AND miq.organization_id = msi.organization_id 
  AND msi.inventory_item_id = ctc.inventory_item_id 
  AND ctc.organization_id = msi.organization_id 
  AND miq.subinventory_code = sub.secondary_inventory_name 
  AND miq.organization_id = sub.organization_id 
  AND ctc.cost_type_id = '1'
;
  
end;
update 
  bec_etl_ctrl.batch_dw_info 
set 
  load_type = 'I', 
  last_refresh_date = getdate() 
where 
  dw_table_name = 'fact_inv_value' 
  and batch_name = 'inv';
commit;