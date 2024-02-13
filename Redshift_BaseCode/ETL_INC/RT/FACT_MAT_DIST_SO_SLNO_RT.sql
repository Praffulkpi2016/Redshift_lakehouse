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
--delete records from temp table
TRUNCATE bec_dwh_rpt.FACT_MAT_DIST_SO_SLNO_RT_TMP;
--Insert records into temp table
INSERT INTO bec_dwh_rpt.FACT_MAT_DIST_SO_SLNO_RT_TMP
(
select distinct transaction_id,inventory_item_id,organization_id 
from bec_dwh.fact_cst_inv_distribution_stg2 mta
where 1=1
and nvl(mta.dw_update_date,'2022-01-01 12:00:00.000') > (select (executebegints-prune_days) 
from bec_etl_ctrl.batch_dw_info 
where dw_table_name ='fact_mat_dist_so_slno_rt' and batch_name = 'inv')
);
--delete records from fact table
delete from bec_dwh_rpt.FACT_MAT_DIST_SO_SLNO_RT
WHERE exists (select 1 from bec_dwh_rpt.FACT_MAT_DIST_SO_SLNO_RT_TMP tmp
              where tmp.transaction_id = FACT_MAT_DIST_SO_SLNO_RT.transaction_id
			  and tmp.inventory_item_id = FACT_MAT_DIST_SO_SLNO_RT.inventory_item_id
			  and tmp.organization_id = FACT_MAT_DIST_SO_SLNO_RT.organization_id
             );
commit;

insert into bec_dwh_rpt.FACT_MAT_DIST_SO_SLNO_RT
(
With CTE_INSTALL as (
    select 
      install.party_name, 
      install.address1, 
      install.address2, 
      install.address5, 
      install.party_site_id, 
      install.site_use_code 
    from 
      bec_dwh.dim_customer_details install 
    where 
      SITE_USE_CODE = 'SHIP_TO' 
    group by 
      install.party_name, 
      install.address1, 
      install.address2, 
      install.address5, 
      install.party_site_id, 
      install.site_use_code
)
select 
  mtrx.inventory_item_id ,
  mtrx.transaction_source_type_id, 
  mtrx.transaction_id, 
  gcc.concatenated_segments code_combination, 
  gcc.segment2 dept, 
  gcc.segment3 account_number, 
  gcc.segment1 company_code, 
  gcc.segment4 intercompany, 
  gcc.segment6 future2, 
  mtrx.transaction_date, 
  mtrx.creation_date, 
  mtrx.last_update_date, 
  msi.item_name part_number, 
  msi.item_description, 
  mtrx.subinventory_code, 
  mtrx.primary_quantity primary_quantity, 
  mtrx.serial_number, 
  msi.primary_uom_code primary_uom, 
  cce.cost_element, 
  mtrx.transaction_source_id, 
  mtrx.trx_source_line_id, 
  mtrx.created_by, 
  DECODE (
    mtrx.transaction_source_type_id :: varchar, 
    '1', poh.segment1, 
    '2', oeh.order_number :: varchar, 
    '8', oeh.order_number :: varchar, 
    '12', oeh.order_number :: varchar, 
    '3', gcc.concatenated_segments, 
    '4', mtrh.request_number :: varchar, 
    '5', wtv.wip_entity_name, 
    '6', mgd.segment1, 
    '7', prh.segment1, 
    '9', mcch.cycle_count_header_name, 
    '10', mpi.physical_inventory_name, 
    '11', ccu.description, 
    mtrx.transaction_source_id :: varchar
  ) transaction_source_name,
    DECODE (
    mtrx.transaction_type_id, 24, mtrx.transaction_cost, 
    mtrx.unit_cost
  ) unit_cost, 
  mtrx.transaction_cost, 
  mtrx.rcv_transaction_id, 
  mtrx.source_line_id, 
  mtrx.organization_id, 
  mtrx.transaction_organization_id, 
  mtrx.transaction_type_id, 
  mtrx.accounting_line_type, 
  --Join with lookup 'CST_ACCOUNTING_LINE_TYPE'
  dlu.meaning accounting_line_type_name, 
  mtrx.transaction_reference, 
  oeh.order_number sales_order_number, 
  oeh.line_number sales_order_line_number, 
  bill_to.party_name bill_customer, 
  bill_to.address1 bill_address1, 
  bill_to.address2 bill_address2, 
  bill_to.address5 bill_address5, 
  oeh.ship_to_customer_name ship_customer, 
  oeh.ship_to_addr_line1 ship_address1, 
  oeh.ship_to_addr_line2 ship_address2, 
  oeh.ship_to_addr_line5 ship_address5, 
  oeh.order_type_id, 
  gcc.segment5 budget_id, 
  mic.item_category_segment1 category_seg1, 
  mic.item_category_segment2 category_seg2, 
  mic1.item_category_segment1 new_category_seg1, 
  mic1.item_category_segment2 new_category_seg2, 
  dbh.incident_number service_request, 
  dbh.task_name, 
  dbh.task_number, 
  dbh.debrief_number, 
  install.party_name install_customer, 
  install.address1 install_address1, 
  install.address2 install_address2, 
  install.address5 install_address5, 
  mtrx.REFERENCE_ACCOUNT, 
  mtrx.COST_ELEMENT_ID, 
  msi.program_name,
  oeh.sales_order_line_type,
  mtrx.dw_load_id dw_load_id_mtrx,
  (
    select 
      system_id 
    from 
      bec_etl_ctrl.etlsourceappid 
    where 
      source_system = 'EBS'
  ) || '-' || nvl(mtrx.dw_load_id, 'NA') || '-' || nvl(mtrx.serial_number, 'NA') as dw_load_id 
from 
  bec_dwh.fact_cst_inv_distribution_stg2 mtrx, 
  bec_dwh_rpt.FACT_MAT_DIST_SO_SLNO_RT_TMP tmp, 
  bec_dwh.dim_gl_accounts gcc, 
  bec_dwh.dim_inv_master_items msi, 
  (
    select 
      * 
    from 
      bec_dwh.dim_inv_item_category_set 
    where 
      category_set_id = 1
  ) mic, 
  (
    select 
      * 
    from 
      bec_dwh.dim_inv_item_category_set 
    where 
      category_set_id = 1100000081
  ) mic1, 
  bec_dwh.dim_wip_jobs wtv, 
  bec_dwh.fact_om_order_details oeh, 
  (select * from bec_ods.cst_cost_elements where is_deleted_flg <> 'Y') cce, 
  bec_dwh.dim_customer_details bill_to, 
  bec_dwh.FACT_CSF_DEBRIEF_HEADERS dbh, 
  CTE_INSTALL install, 
  (
    select 
      dl.lookup_code, 
      dl.meaning 
    from 
      bec_dwh.dim_lookups dl 
    where 
      dl.lookup_type = 'CST_ACCOUNTING_LINE_TYPE'
  ) dlu ,
  (select * from bec_ods.cst_cost_updates where is_deleted_flg <> 'Y') ccu,
  (select * from bec_ods.po_headers_all where is_deleted_flg <> 'Y') poh,
  (select * from bec_ods.mtl_txn_request_headers where is_deleted_flg <> 'Y') mtrh,
  (select * from bec_ods.mtl_generic_dispositions where is_deleted_flg <> 'Y') mgd,
  (select * from bec_ods.po_requisition_headers_all where is_deleted_flg <> 'Y') prh,
  (select * from bec_ods.mtl_cycle_count_headers where is_deleted_flg <> 'Y') mcch,
  (select * from bec_ods.mtl_physical_inventories where is_deleted_flg <> 'Y') mpi
where 1=1
and mtrx.last_update_date >= '2022-10-01 12:00:00.000'
and  mtrx.reference_account=gcc.code_combination_id  
  and mtrx.inventory_item_id = tmp.inventory_item_id 
  and mtrx.organization_id = tmp.organization_id  
  and mtrx.transaction_id = tmp.transaction_id 
  and mtrx.inventory_item_id = msi.inventory_item_id 
  and mtrx.organization_id = msi.organization_id 
  and mtrx.inventory_item_id = mic.inventory_item_id(+) 
  and mtrx.organization_id = mic.organization_id(+) 
  and mtrx.inventory_item_id = mic1.inventory_item_id(+) 
  and mtrx.organization_id = mic1.organization_id(+) 
  and mtrx.transaction_source_id =wtv.wip_entity_id(+) 
  and mtrx.organization_id = wtv.organization_id(+)
  and mtrx.trx_source_line_id = oeh.line_id(+) 
  and oeh.invoice_to_org_id = bill_to.site_use_id(+) 
  and mtrx.cost_element_id = cce.cost_element_id(+) 
  and mtrx.transaction_source_id = dbh.debrief_header_id(+) 
  and dbh.install_location_id = install.party_site_id(+) 
  and mtrx.accounting_line_type = dlu.lookup_code(+)
  --newly added
  and  mtrx.transaction_source_id = ccu.cost_update_id(+)
  and mtrx.transaction_source_id = poh.po_header_id(+)
  and mtrx.transaction_source_id =mtrh.header_id(+)
  and mtrx.transaction_source_id = mgd.disposition_id(+)
  and mtrx.transaction_source_id = prh.requisition_header_id(+)
  and mtrx.transaction_source_id = mcch.cycle_count_header_id (+)
  and mtrx.organization_id=mcch.organization_id(+)
  and mtrx.transaction_source_id = mpi.physical_inventory_id(+)
  and mtrx.organization_id = mpi.organization_id(+)
  );
end;
update 
  bec_etl_ctrl.batch_dw_info 
set 
  load_type = 'I', 
  last_refresh_date = getdate() 
where 
  dw_table_name = 'fact_mat_dist_so_slno_rt' 
  and batch_name = 'inv';
commit;