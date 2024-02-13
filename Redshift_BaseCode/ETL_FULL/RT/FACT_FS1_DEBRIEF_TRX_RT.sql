/*
# Copyright(c) 2022 KPI Partners, Inc. All Rights Reserved.
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
# WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
#
# author: KPI Partners, Inc.
# version: 2022.06
# description: This script represents full load approach for RT reports.
# File Version: KPI v1.0
*/
begin;

drop table if exists bec_dwh_rpt.FACT_FS1_DEBRIEF_TRX_RT;

create table bec_dwh_rpt.FACT_FS1_DEBRIEF_TRX_RT diststyle all sortkey(task_number) as 
SELECT 
  source, 
  service_request, 
  sr_creation_date, 
  service_request_status, 
  sr_type, 
  sr_severity, 
  padid, 
  sr_summary, 
  task_number, 
  task_creation_date, 
  task_status, 
  task_priority, 
  task_type, 
  task_owner_id, 
  owner_type_code,
  resource_id,
  resource_type_code,
  assignee, 
  task_name, 
  task_description, 
  debrief_number, 
  debrief_date, 
  material_transaction_date, 
  processed_flag, 
  line_order_category_code, 
  item, 
  description, 
  item_revision, 
  item_serial_number, 
  item_lotnumber, 
  sub_inventory_code, 
  address1, 
  address2, 
  address3, 
  address4, 
  city, 
  county, 
  "Address State", 
  postal_code, 
  LOCATOR, 
  mat_quantity, 
  uom_code, 
  des_bed_position, 
  return_reason, 
  ERROR_TEXT, 
  ORGANIZATION, 
  location_id, 
  address, 
  "Ship to location", 
  state, 
  item_cost, 
  "Material Sum", 
  "MOH/FOH Sum", 
  FREIGHT_TRACKING_NUMBER,
  getdate() as dw_insert_date, 
  getdate() as dw_update_date
FROM
(
 SELECT 
  'Oracle' source, 
  dh.incident_number service_request, 
  dh.sr_creation_date, 
  cis.NAME service_request_status, 
  (
    SELECT 
      NAME
    FROM 
      bec_ods.CS_INCIDENT_TYPES_TL 
    WHERE 
      incident_type_id = dh.incident_type_id 
      AND LANGUAGE = 'US'
  ) sr_type, 
  (
    SELECT 
      NAME 
    FROM 
      bec_ods.CS_INCIDENT_SEVERITIES_TL 
    WHERE 
      incident_severity_id = dh.incident_severity_id 
      AND LANGUAGE = 'US'
  ) sr_severity, 
  dh.padid, 
  dh.sr_summary, 
  dh.task_number, 
  dh.task_creation_date, 
  dh.task_status1 as task_status, 
  (
    SELECT 
      NAME 
    FROM 
      bec_ods.JTF_TASK_PRIORITIES_TL 
    WHERE 
      task_priority_id = dh.task_priority_id 
      AND LANGUAGE = 'US'
  ) task_priority, 
  (
    SELECT 
      name 
    FROM 
      bec_ods.jtf_task_types_tl 
    WHERE 
      task_type_id = dh.task_type_id 
      AND LANGUAGE = 'US'
  ) task_type, 
  dh.owner_id task_owner_id, 
  dh.owner_type_code, 
  dh.resource_id,
  dh.resource_type_code,
  (SELECT OWNER_NAME
   FROM bec_dwh.DIM_JTF_RESOURCE_GROUPS
   WHERE OWNER_TYPE_CODE = dh.resource_type_code
   AND OWNER_ID    = dh.resource_id
   ) assignee, 
  dh.task_name, 
  dh.description as task_description, 
  dh.debrief_number, 
  dh.debrief_date, 
  mmt.transaction_date material_transaction_date, 
  dh.processed_flag, 
  md.line_order_category_code, 
  md.item, 
  md.description, 
  md.item_revision, 
  md.item_serial_number, 
  md.item_lotnumber, 
  md.sub_inventory_code, 
  hl.address1, 
  hl.address2, 
  hl.address3, 
  hl.address4, 
  hl.city, 
  hl.county, 
  hl.state "Address State", 
  hl.postal_code, 
  md.LOCATOR, 
  md.quantity mat_quantity, 
  md.uom_code, 
  md.material_meaning des_bed_position, 
  meaning return_reason, 
  md.ERROR_TEXT, 
  (
    SELECT 
      org.organization_name 
    FROM 
      bec_ods.org_organization_definitions org 
    WHERE 
      org.organization_id = 245
  ) ORGANIZATION, 
  dh.location_id, 
  hl.address1 address, 
  (
    SELECT 
      DISTINCT hcsua.LOCATION 
    FROM 
      bec_ods.hz_cust_site_uses_all hcsua, 
      bec_ods.hz_cust_acct_sites_all hcasa 
    WHERE 
      hcsua.cust_acct_site_id = hcasa.cust_acct_site_id 
      AND hcsua.site_use_code = 'SHIP_TO' 
      AND hcsua.status = 'A' 
      AND hcasa.status = 'A' 
      and hcasa.ship_to_flag = 'Y'
      AND hcasa.party_site_id = dh.location_id
      and hcsua.primary_flag = 'Y'
  ) "Ship to location", 
  hl.state, 
  cic.item_cost, 
  (cic.material_cost * md.quantity) "Material Sum", 
  (
    cic.material_overhead_cost * md.quantity
  ) "MOH/FOH Sum",
  dh.attribute1 FREIGHT_TRACKING_NUMBER  
FROM 
  bec_dwh.FACT_CSF_DEBRIEF_HEADERS dh, 
  bec_dwh.FACT_CSF_DEBRIEF_LINES md,
  bec_ods.cs_incident_statuses_tl cis, 
  bec_ods.mtl_material_transactions mmt, 
  bec_ods.fnd_lookup_values al, 
  bec_ods.cst_item_costs cic, 
  bec_ods.hz_party_sites hps, 
  bec_ods.hz_locations hl 
WHERE 
  dh.debrief_header_id = md.debrief_header_id 
  AND dh.incident_status_id = cis.incident_status_id
  AND md.debrief_line_id = mmt.trx_source_line_id(+) 
  AND al.lookup_type(+) = 'CREDIT_MEMO_REASON' 
  AND al.lookup_code(+) = md.return_reason_code
  AND md.inventory_item_id = mmt.inventory_item_id(+)
  AND cic.organization_id = md.inventory_org_id 
  AND cic.inventory_item_id = md.inventory_item_id 
  AND cic.cost_type_id = 1 
  and dh.location_Id = hps.party_site_id(+) 
  and hps.location_id = hl.location_id(+) 
 -- AND dh.organization_id = 245
union all 
SELECT 
  'Salesforce' SOURCE, 
  substring(
    mmt.transaction_reference, 
    regexp_instr(
      mmt.transaction_reference, '~', 1
    )+ 1, 
    regexp_instr(
      mmt.transaction_reference, '~', 2
    )-1
  ) SERVICE_REQUEST, 
  mmt.transaction_date SR_CREATION_DATE, 
  null SERVICE_REQUEST_STATUS, 
  null SR_TYPE, 
  null SR_SEVERITY, 
  substring(
    mmt.transaction_reference, 
    1, 
    regexp_instr(
      mmt.transaction_reference, '~', 1
    )-1
  ) PADID, 
  null SR_SUMMARY, 
  substring(
    mmt.transaction_reference, 
    regexp_instr(
      mmt.transaction_reference, '~', 2
    )+ 1, 
    regexp_instr(
      mmt.transaction_reference, '~', 3
    )-1
  ) TASK_NUMBER, 
  null TASK_CREATION_DATE, 
  null TASK_STATUS, 
  null TASK_PRIORITY, 
  null TASK_TYPE, 
  null task_owner_id, 
  null owner_type_code,
  null resource_id,
  null resource_type_code,
  /*substring(
    mmt.transaction_reference, 
    regexp_instr(
      mmt.transaction_reference, '~',1,3 
    )+ 1
  )*/ 
  regexp_substr(mmt.transaction_reference ,'[^~]*$')  ASSIGNEE, 
  null TASK_NAME, 
  null TASK_DESCRIPTION, 
  null DEBRIEF_NUMBER, 
  null DEBRIEF_DATE, 
  mmt.transaction_date MATERIAL_TRANSACTION_DATE, 
  null PROCESSED_FLAG, 
  decode(
    mmt.transaction_type_id, 31, 'ORDER', 
    41, 'RETURN', null
  ) LINE_ORDER_CATEGORY_CODE, 
  msi.segment1 ITEM, 
  msi.description DESCRIPTION, 
  null ITEM_REVISION, 
  mut.serial_number ITEM_SERIAL_NUMBER, 
  null ITEM_LOTNUMBER, 
  mmt.subinventory_code SUB_INVENTORY_CODE, 
  hl.ADDRESS1, 
  hl.ADDRESS2, 
  hl.ADDRESS3, 
  hl.ADDRESS4, 
  hl.CITY, 
  hl.COUNTY, 
  hl.state "Address State", 
  hl.POSTAL_CODE, 
  null LOCATOR, 
  abs(
    decode(
      mut.serial_number, 
      null, 
      mmt.transaction_quantity, 
      sign(mmt.transaction_quantity)
    )
  ) MAT_QUANTITY, 
  mmt.transaction_uom UOM_CODE, 
  null DES_BED_POSITION, 
  null RETURN_REASON, 
  null ERROR_TEXT, 
  null ORGANIZATION, 
  null LOCATION_ID, 
  null ADDRESS, 
  null "Ship to location", 
  hl.state STATE, 
  ITEM_COST, 
  cic.material_cost * abs(
    decode(
      mut.serial_number, 
      null, 
      mmt.transaction_quantity, 
      sign(mmt.transaction_quantity)
    )
  ) "Material Sum", 
  cic.material_overhead_cost * abs(
    decode(
      mut.serial_number, 
      null, 
      mmt.transaction_quantity, 
      sign(mmt.transaction_quantity)
    )
  ) "MOH/FOH Sum" ,
  null FREIGHT_TRACKING_NUMBER
from 
  bec_ods.mtl_material_transactions mmt, 
  bec_ods.mtl_unit_transactions mut, 
  bec_ods.mtl_system_items_b msi, 
  bec_ods.cst_item_costs cic, 
  bec_ods.csi_item_instances cii, 
  bec_ods.hz_party_sites hps, 
  bec_ods.hz_locations hl 
where 
  mmt.transaction_type_id in (31, 41) 
  and mmt.inventory_item_id = msi.inventory_item_id 
  and mmt.organization_id = msi.organization_id 
  and mmt.transaction_id = mut.transaction_id(+) 
  and mmt.organization_id = 245 
  and mmt.inventory_item_id = cic.inventory_item_id 
  and mmt.organization_id = cic.organization_id 
  and cic.cost_type_id = 1 
  and substring(
    mmt.transaction_reference, 
    1, 
    regexp_instr(
      mmt.transaction_reference, '~', 1
    )-1
  ) = cii.external_reference(+) 
  and cii.location_Id = hps.party_site_id(+) 
  and hps.location_id = hl.location_id(+) --and mmt.transaction_date > '08-JUN-2020'
  and transaction_reference like '%~%'
)
;
  
end;
update 
  bec_etl_ctrl.batch_dw_info 
set 
  load_type = 'I', 
  last_refresh_date = getdate() 
where 
  dw_table_name = 'fact_fs1_debrief_trx_rt' 
  and batch_name = 'inv';
commit;