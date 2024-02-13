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
--truncate temp table
TRUNCATE TABLE bec_dwh.FACT_INV_AGING_DETAILS_TMP ;
--Insert incremental data into temp table.
INSERT INTO bec_dwh.FACT_INV_AGING_DETAILS_TMP 
(
SELECT DISTINCT organization_id,inventory_item_id FROM (
SELECT 
msn.current_organization_id organization_id, 
msn.inventory_item_id
FROM 
bec_ods.mtl_serial_numbers  msn, 
bec_ods.mtl_system_items_b msi, 
bec_ods.cst_item_costs  cic 
WHERE 
--msn.current_status = 3 
msn.inventory_item_id = msi.inventory_item_id 
AND msn.current_organization_id = msi.organization_id 
AND msn.inventory_item_id = cic.inventory_item_id 
AND msn.current_organization_id = cic.organization_id 
AND cic.cost_type_id = 1 
and (msn.kca_seq_date > (select (executebegints-prune_days) 
from bec_etl_ctrl.batch_dw_info 
where dw_table_name ='fact_inv_aging_details' and batch_name = 'inv')
or cic.kca_seq_date > (select (executebegints-prune_days) 
from bec_etl_ctrl.batch_dw_info 
where dw_table_name = 'fact_inv_aging_details' and batch_name = 'inv'))
UNION ALL 
SELECT 
moq.organization_id,  
moq.inventory_item_id
FROM 
bec_ods.mtl_onhand_quantities_detail  moq, 
bec_ods.mtl_system_items_b  msi, 
bec_ods.cst_item_costs  cic, 
bec_ods.mtl_item_locations  mil 
WHERE 
1 = 1 
AND moq.organization_id = msi.organization_id 
AND moq.inventory_item_id = msi.inventory_item_id 
AND moq.organization_id = cic.organization_id 
AND moq.inventory_item_id = cic.inventory_item_id 
AND cic.cost_type_id = 1 
AND msi.serial_number_control_code = 1 
and moq.organization_id = mil.organization_id(+) 
AND moq.locator_id = mil.inventory_location_id(+) 
and (moq.kca_seq_date > (select (executebegints-prune_days) 
from bec_etl_ctrl.batch_dw_info 
where dw_table_name ='fact_inv_aging_details' and batch_name = 'inv')
or cic.kca_seq_date > (select (executebegints-prune_days) 
from bec_etl_ctrl.batch_dw_info 
where dw_table_name = 'fact_inv_aging_details' and batch_name = 'inv')
)));
--DELETE FROM FACT TABLE USING TMP TABLE
delete from bec_dwh.fact_inv_aging_details 
where (inventory_item_id,organization_id ) 
IN (select inventory_item_id,organization_id 
    from bec_dwh.FACT_INV_AGING_DETAILS_TMP) ;
--INSERT INTO FACT TABLE
Insert into bec_dwh.FACT_INV_AGING_DETAILS
SELECT 
organization_id, 
  description, 
  part_number, 
  unit_cost, 
  transaction_quantity, 
  total_cost, 
  inventory_item_id, 
  date_in, 
  subinventory_code, 
  transaction_type_id, 
  serial_number, 
  current_status, 
  current_subinventory_code, 
  locator_id,
  (select system_id from bec_etl_ctrl.etlsourceappid where source_system='EBS')||'-'||locator_id   locator_id_KEY,
  (select system_id from bec_etl_ctrl.etlsourceappid where source_system='EBS')||'-'||organization_id   organization_id_KEY,
  (select system_id from bec_etl_ctrl.etlsourceappid where source_system='EBS')||'-'||inventory_item_id   inventory_item_id_KEY,
  (select system_id from bec_etl_ctrl.etlsourceappid where source_system='EBS')||'-'||transaction_type_id   transaction_type_id_KEY,
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
  || '-' || nvl(subinventory_code, 'NA')
  || '-' || nvl(locator_id, 0)
  || '-' || nvl(serial_number, 'NA')
  || '-' || nvl(date_in, '1900-01-01 12:00:00')
  || '-' || nvl(transaction_quantity, 0)  as dw_load_id,
  getdate() as dw_insert_date, 
  getdate() as dw_update_date 
FROM 
  (
	SELECT 
		msn.current_organization_id organization_id, 
		msi.description, 
		msi.segment1 part_number, 
		cic.item_cost unit_cost, 
		1 transaction_quantity, 
		1 * cic.item_cost total_cost, 
		msn.inventory_item_id, 
		msn.LAST_UPDATE_DATE date_in, 
		msn.current_subinventory_code subinventory_code, 
		NULL transaction_type_id, 
		msn.serial_number, 
		msn.current_status, 
		msn.current_subinventory_code, 
		msn.current_locator_id as locator_id
	FROM 
		(select * from bec_ods.mtl_serial_numbers where is_deleted_flg <> 'Y') msn, 
		(select * from bec_ods.mtl_system_items_b where is_deleted_flg <> 'Y') msi, 
		(select * from bec_ods.cst_item_costs where is_deleted_flg <> 'Y') cic ,
		bec_dwh.FACT_INV_AGING_DETAILS_TMP tmp
	WHERE 
		msn.current_status = 3 
		AND msn.inventory_item_id = msi.inventory_item_id 
		AND msn.current_organization_id = msi.organization_id 
		AND msn.inventory_item_id = cic.inventory_item_id 
		AND msn.current_organization_id = cic.organization_id 
		AND cic.cost_type_id = 1 
		AND msn.inventory_item_id = tmp.inventory_item_id 
		AND msn.current_organization_id = tmp.organization_id 
	UNION ALL 
	SELECT 
		moq.organization_id, 
		msi.description, 
		msi.segment1 part_number, 
		cic.item_cost unit_cost, 
		SUM (moq.transaction_quantity) transaction_quantity, 
		SUM (moq.transaction_quantity) * cic.item_cost total_cost, 
		moq.inventory_item_id, 
		moq.date_received date_in, 
		moq.subinventory_code, 
		NULL transaction_type_id, 
		NULL serial_number, 
		NULL current_status, 
		NULL current_subinventory_code, 
		moq.locator_id 
	FROM 
		(select * from bec_ods.mtl_onhand_quantities_detail where is_deleted_flg <> 'Y' ) moq, 
		(select * from bec_ods.mtl_system_items_b where is_deleted_flg <> 'Y') msi, 
		(select * from bec_ods.cst_item_costs where is_deleted_flg <> 'Y') cic, 
		(select * from bec_ods.mtl_item_locations where is_deleted_flg <> 'Y') mil ,
		bec_dwh.FACT_INV_AGING_DETAILS_TMP tmp
	WHERE 
		1 = 1 
		AND moq.organization_id = msi.organization_id 
		AND moq.inventory_item_id = msi.inventory_item_id 
		AND moq.organization_id = cic.organization_id 
		AND moq.inventory_item_id = cic.inventory_item_id 
		AND cic.cost_type_id = 1 
		AND msi.serial_number_control_code = 1 
		and moq.organization_id = mil.organization_id(+) 
		AND moq.locator_id = mil.inventory_location_id(+) 
		AND moq.inventory_item_id = tmp.inventory_item_id 
		AND moq.organization_id = tmp.organization_id 
	GROUP BY 
		moq.organization_id, 
		msi.description, 
		msi.segment1, 
		cic.item_cost, 
		moq.inventory_item_id, 
		moq.date_received, 
		moq.subinventory_code, 
		moq.locator_id
  );
 end;
update 
  bec_etl_ctrl.batch_dw_info 
set 
  load_type = 'I', 
  last_refresh_date = getdate() 
where 
  dw_table_name = 'fact_inv_aging_details' 
  and batch_name = 'inv';
commit; 