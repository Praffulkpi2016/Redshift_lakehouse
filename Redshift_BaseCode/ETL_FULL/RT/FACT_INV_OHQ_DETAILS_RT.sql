/*
# Copyright(c) 2022 KPI Partners, Inc. All Rights Reserved.
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
# WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
#
# author: KPI Partners, Inc.
# version: 2022.06
# description: This script represents full load approach for RT.
# File Version: KPI v1.0
*/
begin;

drop table if exists bec_dwh_rpt.FACT_INV_OHQ_DETAILS_RT;

create table bec_dwh_rpt.FACT_INV_OHQ_DETAILS_RT
distkey(organization_id)
sortkey(organization_id,serial_number) AS 
SELECT 
   sum(moq1.quantity) as on_hand_qty, 
  moq1.serial_number,
  msi.secondary_inventory_name subinventory,
  loc.segment1 || '.' || loc.segment2 || '.' || loc.segment3 locator,
    msi_status.status_code, 
  Decode (
    Substring(
      (msi_status.status_code), 
      Length (msi_status.status_code), 
      1
    ), 
    'e', 
    'Y', 
    Substring(
      (msi_status.status_code), 
      Length (msi_status.status_code), 
      1
    )
  ) nettable,
  moq1.part_number,
  moq1.organization_id,
  moq1.inventory_item_id,
  moq1.locator_id,
  moq1.lot_number
FROM bec_dwh.FACT_INV_OHQ_DETAILS moq1, 
  bec_dwh.dim_sub_inventories msi, 
  bec_ods.MTL_MATERIAL_STATUSES_TL msi_status, 
  bec_dwh.dim_item_locations loc
  where 1=1
  AND moq1.subinventory = msi.secondary_inventory_name(+) 
  AND moq1.organization_id = msi.organization_id (+) 
  and msi.status_id = msi_status.status_id(+) 
  and moq1.locator_id = loc.inventory_location_id(+)
group by 
moq1.serial_number,
  msi.secondary_inventory_name,
  loc.segment1 || '.' || loc.segment2 || '.' || loc.segment3,
    msi_status.status_code, 
  Decode (
    Substring(
      (msi_status.status_code), 
      Length (msi_status.status_code), 
      1
    ), 
    'e', 
    'Y', 
    Substring(
      (msi_status.status_code), 
      Length (msi_status.status_code), 
      1
    )
  ) ,
  moq1.part_number,
  moq1.organization_id,
  moq1.inventory_item_id,
    moq1.locator_id,
  moq1.lot_number
;
end;
update
	bec_etl_ctrl.batch_dw_info
set
	last_refresh_date = getdate()
where
	dw_table_name = 'fact_inv_ohq_details_rt'
	and batch_name = 'inv';

commit;