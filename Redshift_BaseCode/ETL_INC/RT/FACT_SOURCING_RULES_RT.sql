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

Truncate table bec_dwh.FACT_SOURCING_RULES_RT_STG1;

Insert Into bec_dwh.FACT_SOURCING_RULES_RT_STG1
(WITH MRP_ASSIGNMENTS AS (
  SELECT 
    organization_id, 
    inventory_item_id, 
    sourcing_rule_id 
  FROM 
    bec_ods.MRP_SR_ASSIGNMENTS 
  WHERE 
    assignment_set_id = 1
), 
INV_ORGS as (
  SELECT 
    organization_id, 
    organization_code 
  FROM 
    bec_ods.MTL_PARAMETERS
), 
AP_SUPP AS (
  SELECT 
    aps.VENDOR_ID, 
    VENDOR_NAME --, VENDOR_SITE_ID, VENDOR_SITE_CODE
  FROM 
    bec_ods.AP_SUPPLIERS APS
), 
AP_SUPP_SITES AS (
  SELECT 
    APSS.VENDOR_ID, 
    VENDOR_SITE_ID, 
    VENDOR_SITE_CODE 
  FROM 
    bec_ods.AP_SUPPLIER_SITES_ALL APSS
) 
SELECT 
  msi.inventory_item_status_code, 
  msi.inventory_item_Id, 
  supp.vendor_id, 
  SUPP_SITE.vendor_site_id, 
  mp.organization_code item_org, 
  msi.organization_id, 
  msi.segment1 part_number, 
  msi.description, 
  decode(
    msi.planning_make_buy_code, 1, 'Make', 
    'Buy'
  ) make_buy_code, 
  poa.agent_name buyer, 
  msi.planner_code, 
  sr.sourcing_rule_name, 
  decode(
    sr.status, 1, 'Active', 'Inactive'
  ) sr_status, 
  decode(
    sr.planning_active, 1, 'Active', 'Inactive'
  ) planning_active, 
  asgn.organization_code assignment_org, 
  sra.organization_id assignment_org_id, 
  rcv.organization_code receipt_org, 
  sra.sourcing_rule_id, 
  srro.effective_date, 
  srro.disable_date, 
  supp.vendor_name, 
  SUPP_SITE.vendor_site_code, 
  src.organization_code source_org, 
  allocation_percent, 
  (
    SELECT 
      unit_price 
    FROM 
      bec_dwh_rpt.fact_supplier_price_list_rt pl 
    WHERE 
      pl.supplier = pl.price_list_name 
      AND pl.supplier = supp.vendor_name 
      AND pl.part_number = msi.SEGMENt1 
      AND sysdate BETWEEN effectivity_start_date 
      AND nvl(effectivity_end_date, sysdate + 1)
  ) unit_price 
FROM 
  bec_ods.mtl_system_items_b msi, 
  INV_ORGS mp, 
  bec_ods.po_agents_v poa, 
  MRP_ASSIGNMENTS sra, 
  bec_ods.mrp_sourcing_rules sr, 
  bec_ods.MRP_SR_SOURCE_ORG srso, 
  bec_ods.MRP_SR_RECEIPT_ORG srro, 
  AP_SUPP SUPP, 
  AP_SUPP_SITES SUPP_SITE, 
  INV_ORGS asgn, 
  INV_ORGS rcv, 
  INV_ORGS SRC --,supp_price qp
WHERE 
  1 = 1 
  AND NOT EXISTS (
    SELECT 
      1 
    FROM 
      bec_ods.MRP_SR_ASSIGNMENTS MSA 
    WHERE 
      MSA.assignment_set_id = 1 
      AND MSA.organization_id IS NULL 
      AND MSA.inventory_item_id = MSI.inventory_item_id
  ) 
  AND msi.organization_id = mp.organization_id 
  and msi.buyer_id = poa.agent_id(+) 
  AND msi.organization_id = sra.organization_id(+) 
  AND msi.inventory_item_id = sra.inventory_item_id(+) 
  AND sra.sourcing_rule_id = sr.sourcing_rule_id(+) 
  AND sra.sourcing_rule_id = srro.sourcing_rule_id(+) 
  AND srro.sr_receipt_id = srso.sr_receipt_id(+) 
  AND srso.vendor_id = SUPP.vendor_id(+) 
  AND srso.vendor_id = SUPP_SITE.vendor_id(+) 
  AND srso.vendor_site_id = SUPP_SITE.vendor_site_id(+) --AND sra.organization_id IS NOT NULL
  AND sra.organization_id = asgn.organization_id(+) 
  AND srro.receipt_organization_id = rcv.organization_id(+) 
  AND srso.source_organization_id = src.organization_id(+) 
);

commit;

Truncate table bec_dwh.FACT_SOURCING_RULES_RT_STG2;

Insert Into bec_dwh.FACT_SOURCING_RULES_RT_STG2
(WITH MRP_ASSIGNMENTS AS (
  SELECT 
    organization_id, 
    inventory_item_id, 
    sourcing_rule_id 
  FROM 
    bec_ods.MRP_SR_ASSIGNMENTS 
  WHERE 
    assignment_set_id = 1
), 
INV_ORGS as (
  SELECT 
    organization_id, 
    organization_code 
  FROM 
    bec_ods.MTL_PARAMETERS
), 
AP_SUPP AS (
  SELECT 
    aps.VENDOR_ID, 
    VENDOR_NAME --, VENDOR_SITE_ID, VENDOR_SITE_CODE
  FROM 
    bec_ods.AP_SUPPLIERS APS
), 
AP_SUPP_SITES AS (
  SELECT 
    APSS.VENDOR_ID, 
    VENDOR_SITE_ID, 
    VENDOR_SITE_CODE 
  FROM 
    bec_ods.AP_SUPPLIER_SITES_ALL APSS
) 
  SELECT 
  msi.inventory_item_status_code, 
  msi.inventory_item_Id, 
  supp.vendor_id, 
  SUPP_SITE.vendor_site_id, 
  mp.organization_code item_org, 
  msi.organization_id, 
  msi.segment1 part_number, 
  msi.description, 
  decode(
    msi.planning_make_buy_code, 1, 'Make', 
    'Buy'
  ) make_buy_code, 
  poa.agent_name buyer, 
  msi.planner_code, 
  sr.sourcing_rule_name, 
  decode(
    sr.status, 1, 'Active', 'Inactive'
  ) sr_status, 
  decode(
    sr.planning_active, 1, 'Active', 'Inactive'
  ) planning_active, 
  asgn.organization_code assignment_org, 
  sra.organization_id assignment_org_id, 
  rcv.organization_code receipt_org, 
  sra.sourcing_rule_id, 
  srro.effective_date, 
  srro.disable_date, 
  supp.vendor_name, 
  SUPP_SITE.vendor_site_code, 
  src.organization_code source_org, 
  allocation_percent, 
  (
    SELECT 
      unit_price 
    FROM 
      bec_dwh_rpt.fact_supplier_price_list_rt pl 
    WHERE 
      pl.supplier = pl.price_list_name 
      AND pl.supplier = supp.vendor_name 
      AND pl.part_number = msi.SEGMENt1 
      AND sysdate BETWEEN effectivity_start_date 
      AND nvl(effectivity_end_date, sysdate + 1)
  ) unit_price 
FROM 
  bec_ods.mtl_system_items_b msi, 
  INV_ORGS mp, 
  bec_ods.po_agents_v poa, 
  MRP_ASSIGNMENTS sra, 
  bec_ods.mrp_sourcing_rules sr, 
  bec_ods.MRP_SR_SOURCE_ORG srso, 
  bec_ods.MRP_SR_RECEIPT_ORG srro, 
  AP_SUPP SUPP, 
  AP_SUPP_SITES SUPP_SITE, 
  INV_ORGS asgn, 
  INV_ORGS rcv, 
  INV_ORGS SRC --,supp_price qp
WHERE 
  1 = 1 
  AND msi.organization_id = mp.organization_id 
  and msi.buyer_id = poa.agent_id(+) --AND msi.organization_id = sra.organization_id(+)
  AND msi.inventory_item_id = sra.inventory_item_id 
  AND sra.sourcing_rule_id = sr.sourcing_rule_id(+) 
  AND sra.sourcing_rule_id = srro.sourcing_rule_id(+) 
  AND srro.sr_receipt_id = srso.sr_receipt_id(+) 
  AND srso.vendor_id = SUPP.vendor_id(+) 
  AND srso.vendor_id = SUPP_SITE.vendor_id(+) 
  AND srso.vendor_site_id = SUPP_SITE.vendor_site_id(+) 
  AND sra.organization_id IS NULL 
  AND sra.organization_id = asgn.organization_id(+) 
  AND srro.receipt_organization_id = rcv.organization_id(+) 
  AND srso.source_organization_id = src.organization_id(+)
);

commit;	
	
Truncate Table bec_dwh_rpt.FACT_SOURCING_RULES_RT;
  
Insert Into bec_dwh_rpt.FACT_SOURCING_RULES_RT 
(
  select * from bec_dwh.FACT_SOURCING_RULES_RT_STG1
  UNION ALL 
  select * from bec_dwh.FACT_SOURCING_RULES_RT_STG2
);

end;
update 
  bec_etl_ctrl.batch_dw_info 
set 
  load_type = 'I', 
  last_refresh_date = getdate() 
where 
  dw_table_name = 'fact_sourcing_rules_rt' 
  and batch_name = 'inv';
commit;