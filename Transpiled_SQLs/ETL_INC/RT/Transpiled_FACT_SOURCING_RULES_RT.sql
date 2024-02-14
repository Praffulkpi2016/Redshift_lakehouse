TRUNCATE table gold_bec_dwh.FACT_SOURCING_RULES_RT_STG1;
WITH MRP_ASSIGNMENTS AS (
  SELECT
    organization_id,
    inventory_item_id,
    sourcing_rule_id
  FROM silver_bec_ods.MRP_SR_ASSIGNMENTS
  WHERE
    assignment_set_id = 1
), INV_ORGS AS (
  SELECT
    organization_id,
    organization_code
  FROM silver_bec_ods.MTL_PARAMETERS
), AP_SUPP AS (
  SELECT
    aps.VENDOR_ID,
    VENDOR_NAME /* , VENDOR_SITE_ID, VENDOR_SITE_CODE */
  FROM silver_bec_ods.AP_SUPPLIERS AS APS
), AP_SUPP_SITES AS (
  SELECT
    APSS.VENDOR_ID,
    VENDOR_SITE_ID,
    VENDOR_SITE_CODE
  FROM silver_bec_ods.AP_SUPPLIER_SITES_ALL AS APSS
)
INSERT INTO gold_bec_dwh.FACT_SOURCING_RULES_RT_STG1
(
  SELECT
    msi.inventory_item_status_code,
    msi.inventory_item_Id,
    supp.vendor_id,
    SUPP_SITE.vendor_site_id,
    mp.organization_code AS item_org,
    msi.organization_id,
    msi.segment1 AS part_number,
    msi.description,
    CASE WHEN msi.planning_make_buy_code = 1 THEN 'Make' ELSE 'Buy' END AS make_buy_code,
    poa.agent_name AS buyer,
    msi.planner_code,
    sr.sourcing_rule_name,
    CASE WHEN sr.status = 1 THEN 'Active' ELSE 'Inactive' END AS sr_status,
    CASE WHEN sr.planning_active = 1 THEN 'Active' ELSE 'Inactive' END AS planning_active,
    asgn.organization_code AS assignment_org,
    sra.organization_id AS assignment_org_id,
    rcv.organization_code AS receipt_org,
    sra.sourcing_rule_id,
    srro.effective_date,
    srro.disable_date,
    supp.vendor_name,
    SUPP_SITE.vendor_site_code,
    src.organization_code AS source_org,
    allocation_percent,
    (
      SELECT
        unit_price
      FROM gold_bec_dwh_rpt.fact_supplier_price_list_rt AS pl
      WHERE
        pl.supplier = pl.price_list_name
        AND pl.supplier = supp.vendor_name
        AND pl.part_number = msi.SEGMENt1
        AND CURRENT_TIMESTAMP() BETWEEN effectivity_start_date AND COALESCE(effectivity_end_date, CURRENT_TIMESTAMP() + 1)
    ) AS unit_price
  FROM silver_bec_ods.mtl_system_items_b AS msi, INV_ORGS AS mp, silver_bec_ods.po_agents_v AS poa, MRP_ASSIGNMENTS AS sra, silver_bec_ods.mrp_sourcing_rules AS sr, silver_bec_ods.MRP_SR_SOURCE_ORG AS srso, silver_bec_ods.MRP_SR_RECEIPT_ORG AS srro, AP_SUPP AS SUPP, AP_SUPP_SITES AS SUPP_SITE, INV_ORGS AS asgn, INV_ORGS AS rcv, INV_ORGS AS SRC /* ,supp_price qp */
  WHERE
    1 = 1
    AND NOT EXISTS(
      SELECT
        1
      FROM silver_bec_ods.MRP_SR_ASSIGNMENTS AS MSA
      WHERE
        MSA.assignment_set_id = 1
        AND MSA.organization_id IS NULL
        AND MSA.inventory_item_id = MSI.inventory_item_id
    )
    AND msi.organization_id = mp.organization_id
    AND msi.buyer_id = poa.AGENT_ID()
    AND msi.organization_id = sra.ORGANIZATION_ID()
    AND msi.inventory_item_id = sra.INVENTORY_ITEM_ID()
    AND sra.sourcing_rule_id = sr.SOURCING_RULE_ID()
    AND sra.sourcing_rule_id = srro.SOURCING_RULE_ID()
    AND srro.sr_receipt_id = srso.SR_RECEIPT_ID()
    AND srso.vendor_id = SUPP.VENDOR_ID()
    AND srso.vendor_id = SUPP_SITE.VENDOR_ID()
    AND srso.vendor_site_id = SUPP_SITE.VENDOR_SITE_ID() /* AND sra.organization_id IS NOT NULL */
    AND sra.organization_id = asgn.ORGANIZATION_ID()
    AND srro.receipt_organization_id = rcv.ORGANIZATION_ID()
    AND srso.source_organization_id = src.ORGANIZATION_ID()
);
TRUNCATE table gold_bec_dwh.FACT_SOURCING_RULES_RT_STG2;
WITH MRP_ASSIGNMENTS AS (
  SELECT
    organization_id,
    inventory_item_id,
    sourcing_rule_id
  FROM silver_bec_ods.MRP_SR_ASSIGNMENTS
  WHERE
    assignment_set_id = 1
), INV_ORGS AS (
  SELECT
    organization_id,
    organization_code
  FROM silver_bec_ods.MTL_PARAMETERS
), AP_SUPP AS (
  SELECT
    aps.VENDOR_ID,
    VENDOR_NAME /* , VENDOR_SITE_ID, VENDOR_SITE_CODE */
  FROM silver_bec_ods.AP_SUPPLIERS AS APS
), AP_SUPP_SITES AS (
  SELECT
    APSS.VENDOR_ID,
    VENDOR_SITE_ID,
    VENDOR_SITE_CODE
  FROM silver_bec_ods.AP_SUPPLIER_SITES_ALL AS APSS
)
INSERT INTO gold_bec_dwh.FACT_SOURCING_RULES_RT_STG2
(
  SELECT
    msi.inventory_item_status_code,
    msi.inventory_item_Id,
    supp.vendor_id,
    SUPP_SITE.vendor_site_id,
    mp.organization_code AS item_org,
    msi.organization_id,
    msi.segment1 AS part_number,
    msi.description,
    CASE WHEN msi.planning_make_buy_code = 1 THEN 'Make' ELSE 'Buy' END AS make_buy_code,
    poa.agent_name AS buyer,
    msi.planner_code,
    sr.sourcing_rule_name,
    CASE WHEN sr.status = 1 THEN 'Active' ELSE 'Inactive' END AS sr_status,
    CASE WHEN sr.planning_active = 1 THEN 'Active' ELSE 'Inactive' END AS planning_active,
    asgn.organization_code AS assignment_org,
    sra.organization_id AS assignment_org_id,
    rcv.organization_code AS receipt_org,
    sra.sourcing_rule_id,
    srro.effective_date,
    srro.disable_date,
    supp.vendor_name,
    SUPP_SITE.vendor_site_code,
    src.organization_code AS source_org,
    allocation_percent,
    (
      SELECT
        unit_price
      FROM gold_bec_dwh_rpt.fact_supplier_price_list_rt AS pl
      WHERE
        pl.supplier = pl.price_list_name
        AND pl.supplier = supp.vendor_name
        AND pl.part_number = msi.SEGMENt1
        AND CURRENT_TIMESTAMP() BETWEEN effectivity_start_date AND COALESCE(effectivity_end_date, CURRENT_TIMESTAMP() + 1)
    ) AS unit_price
  FROM silver_bec_ods.mtl_system_items_b AS msi, INV_ORGS AS mp, silver_bec_ods.po_agents_v AS poa, MRP_ASSIGNMENTS AS sra, silver_bec_ods.mrp_sourcing_rules AS sr, silver_bec_ods.MRP_SR_SOURCE_ORG AS srso, silver_bec_ods.MRP_SR_RECEIPT_ORG AS srro, AP_SUPP AS SUPP, AP_SUPP_SITES AS SUPP_SITE, INV_ORGS AS asgn, INV_ORGS AS rcv, INV_ORGS AS SRC /* ,supp_price qp */
  WHERE
    1 = 1
    AND msi.organization_id = mp.organization_id
    AND msi.buyer_id = poa.AGENT_ID() /* AND msi.organization_id = sra.organization_id(+) */
    AND msi.inventory_item_id = sra.inventory_item_id
    AND sra.sourcing_rule_id = sr.SOURCING_RULE_ID()
    AND sra.sourcing_rule_id = srro.SOURCING_RULE_ID()
    AND srro.sr_receipt_id = srso.SR_RECEIPT_ID()
    AND srso.vendor_id = SUPP.VENDOR_ID()
    AND srso.vendor_id = SUPP_SITE.VENDOR_ID()
    AND srso.vendor_site_id = SUPP_SITE.VENDOR_SITE_ID()
    AND sra.organization_id IS NULL
    AND sra.organization_id = asgn.ORGANIZATION_ID()
    AND srro.receipt_organization_id = rcv.ORGANIZATION_ID()
    AND srso.source_organization_id = src.ORGANIZATION_ID()
);
TRUNCATE table gold_bec_dwh_rpt.FACT_SOURCING_RULES_RT;
INSERT INTO gold_bec_dwh_rpt.FACT_SOURCING_RULES_RT
(
  SELECT
    *
  FROM gold_bec_dwh.FACT_SOURCING_RULES_RT_STG1
  UNION ALL
  SELECT
    *
  FROM gold_bec_dwh.FACT_SOURCING_RULES_RT_STG2
);
UPDATE bec_etl_ctrl.batch_dw_info SET load_type = 'I', last_refresh_date = CURRENT_TIMESTAMP()
WHERE
  dw_table_name = 'fact_sourcing_rules_rt' AND batch_name = 'inv';