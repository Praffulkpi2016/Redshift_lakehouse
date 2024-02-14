DROP table IF EXISTS gold_bec_dwh_rpt.FACT_FS1_DEBRIEF_TRX_RT;
CREATE TABLE gold_bec_dwh_rpt.FACT_FS1_DEBRIEF_TRX_RT AS
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
  `Address State`,
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
  `Ship to location`,
  state,
  item_cost,
  `Material Sum`,
  `MOH/FOH Sum`,
  FREIGHT_TRACKING_NUMBER,
  CURRENT_TIMESTAMP() AS dw_insert_date,
  CURRENT_TIMESTAMP() AS dw_update_date
FROM (
  SELECT
    'Oracle' AS source,
    dh.incident_number AS service_request,
    dh.sr_creation_date,
    cis.NAME AS service_request_status,
    (
      SELECT
        NAME
      FROM silver_bec_ods.CS_INCIDENT_TYPES_TL
      WHERE
        incident_type_id = dh.incident_type_id AND LANGUAGE = 'US'
    ) AS sr_type,
    (
      SELECT
        NAME
      FROM silver_bec_ods.CS_INCIDENT_SEVERITIES_TL
      WHERE
        incident_severity_id = dh.incident_severity_id AND LANGUAGE = 'US'
    ) AS sr_severity,
    dh.padid,
    dh.sr_summary,
    dh.task_number,
    dh.task_creation_date,
    dh.task_status1 AS task_status,
    (
      SELECT
        NAME
      FROM silver_bec_ods.JTF_TASK_PRIORITIES_TL
      WHERE
        task_priority_id = dh.task_priority_id AND LANGUAGE = 'US'
    ) AS task_priority,
    (
      SELECT
        name
      FROM silver_bec_ods.jtf_task_types_tl
      WHERE
        task_type_id = dh.task_type_id AND LANGUAGE = 'US'
    ) AS task_type,
    dh.owner_id AS task_owner_id,
    dh.owner_type_code,
    dh.resource_id,
    dh.resource_type_code,
    (
      SELECT
        OWNER_NAME
      FROM gold_bec_dwh.DIM_JTF_RESOURCE_GROUPS
      WHERE
        OWNER_TYPE_CODE = dh.resource_type_code AND OWNER_ID = dh.resource_id
    ) AS assignee,
    dh.task_name,
    dh.description AS task_description,
    dh.debrief_number,
    dh.debrief_date,
    mmt.transaction_date AS material_transaction_date,
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
    hl.state AS `Address State`,
    hl.postal_code,
    md.LOCATOR,
    md.quantity AS mat_quantity,
    md.uom_code,
    md.material_meaning AS des_bed_position,
    meaning AS return_reason,
    md.ERROR_TEXT,
    (
      SELECT
        org.organization_name
      FROM silver_bec_ods.org_organization_definitions AS org
      WHERE
        org.organization_id = 245
    ) AS ORGANIZATION,
    dh.location_id,
    hl.address1 AS address,
    (
      SELECT DISTINCT
        hcsua.LOCATION
      FROM silver_bec_ods.hz_cust_site_uses_all AS hcsua, silver_bec_ods.hz_cust_acct_sites_all AS hcasa
      WHERE
        hcsua.cust_acct_site_id = hcasa.cust_acct_site_id
        AND hcsua.site_use_code = 'SHIP_TO'
        AND hcsua.status = 'A'
        AND hcasa.status = 'A'
        AND hcasa.ship_to_flag = 'Y'
        AND hcasa.party_site_id = dh.location_id
        AND hcsua.primary_flag = 'Y'
    ) AS `Ship to location`,
    hl.state,
    cic.item_cost,
    (
      cic.material_cost * md.quantity
    ) AS `Material Sum`,
    (
      cic.material_overhead_cost * md.quantity
    ) AS `MOH/FOH Sum`,
    dh.attribute1 AS FREIGHT_TRACKING_NUMBER
  FROM gold_bec_dwh.FACT_CSF_DEBRIEF_HEADERS AS dh, gold_bec_dwh.FACT_CSF_DEBRIEF_LINES AS md, silver_bec_ods.cs_incident_statuses_tl AS cis, silver_bec_ods.mtl_material_transactions AS mmt, silver_bec_ods.fnd_lookup_values AS al, silver_bec_ods.cst_item_costs AS cic, silver_bec_ods.hz_party_sites AS hps, silver_bec_ods.hz_locations AS hl
  WHERE
    dh.debrief_header_id = md.debrief_header_id
    AND dh.incident_status_id = cis.incident_status_id
    AND md.debrief_line_id = mmt.TRX_SOURCE_LINE_ID()
    AND al.LOOKUP_TYPE() = 'CREDIT_MEMO_REASON'
    AND al.LOOKUP_CODE() = md.return_reason_code
    AND md.inventory_item_id = mmt.INVENTORY_ITEM_ID()
    AND cic.organization_id = md.inventory_org_id
    AND cic.inventory_item_id = md.inventory_item_id
    AND cic.cost_type_id = 1
    AND dh.location_Id = hps.PARTY_SITE_ID()
    AND hps.location_id = hl.LOCATION_ID() /* AND dh.organization_id = 245 */
  UNION ALL
  SELECT
    'Salesforce' AS SOURCE,
    SUBSTRING(
      mmt.transaction_reference,
      REGEXP_INSTR(mmt.transaction_reference, '~', 1) + 1,
      REGEXP_INSTR(mmt.transaction_reference, '~', 2) - 1
    ) AS SERVICE_REQUEST,
    mmt.transaction_date AS SR_CREATION_DATE,
    NULL AS SERVICE_REQUEST_STATUS,
    NULL AS SR_TYPE,
    NULL AS SR_SEVERITY,
    SUBSTRING(mmt.transaction_reference, 1, REGEXP_INSTR(mmt.transaction_reference, '~', 1) - 1) AS PADID,
    NULL AS SR_SUMMARY,
    SUBSTRING(
      mmt.transaction_reference,
      REGEXP_INSTR(mmt.transaction_reference, '~', 2) + 1,
      REGEXP_INSTR(mmt.transaction_reference, '~', 3) - 1
    ) AS TASK_NUMBER,
    NULL AS TASK_CREATION_DATE,
    NULL AS TASK_STATUS,
    NULL AS TASK_PRIORITY,
    NULL AS TASK_TYPE,
    NULL AS task_owner_id,
    NULL AS owner_type_code,
    NULL AS resource_id,
    NULL AS resource_type_code,
    REGEXP_SUBSTR(mmt.transaction_reference, '[^~]*$') AS ASSIGNEE, /* substring(
        mmt.transaction_reference, 
        regexp_instr(
          mmt.transaction_reference, '~',1,3 
        )+ 1
      ) */
    NULL AS TASK_NAME,
    NULL AS TASK_DESCRIPTION,
    NULL AS DEBRIEF_NUMBER,
    NULL AS DEBRIEF_DATE,
    mmt.transaction_date AS MATERIAL_TRANSACTION_DATE,
    NULL AS PROCESSED_FLAG,
    CASE
      WHEN mmt.transaction_type_id = 31
      THEN 'ORDER'
      WHEN mmt.transaction_type_id = 41
      THEN 'RETURN'
      ELSE NULL
    END AS LINE_ORDER_CATEGORY_CODE,
    msi.segment1 AS ITEM,
    msi.description AS DESCRIPTION,
    NULL AS ITEM_REVISION,
    mut.serial_number AS ITEM_SERIAL_NUMBER,
    NULL AS ITEM_LOTNUMBER,
    mmt.subinventory_code AS SUB_INVENTORY_CODE,
    hl.ADDRESS1,
    hl.ADDRESS2,
    hl.ADDRESS3,
    hl.ADDRESS4,
    hl.CITY,
    hl.COUNTY,
    hl.state AS `Address State`,
    hl.POSTAL_CODE,
    NULL AS LOCATOR,
    ABS(
      CASE
        WHEN mut.serial_number IS NULL
        THEN mmt.transaction_quantity
        ELSE SIGN(mmt.transaction_quantity)
      END
    ) AS MAT_QUANTITY,
    mmt.transaction_uom AS UOM_CODE,
    NULL AS DES_BED_POSITION,
    NULL AS RETURN_REASON,
    NULL AS ERROR_TEXT,
    NULL AS ORGANIZATION,
    NULL AS LOCATION_ID,
    NULL AS ADDRESS,
    NULL AS `Ship to location`,
    hl.state AS STATE,
    ITEM_COST,
    cic.material_cost * ABS(
      CASE
        WHEN mut.serial_number IS NULL
        THEN mmt.transaction_quantity
        ELSE SIGN(mmt.transaction_quantity)
      END
    ) AS `Material Sum`,
    cic.material_overhead_cost * ABS(
      CASE
        WHEN mut.serial_number IS NULL
        THEN mmt.transaction_quantity
        ELSE SIGN(mmt.transaction_quantity)
      END
    ) AS `MOH/FOH Sum`,
    NULL AS FREIGHT_TRACKING_NUMBER
  FROM silver_bec_ods.mtl_material_transactions AS mmt, silver_bec_ods.mtl_unit_transactions AS mut, silver_bec_ods.mtl_system_items_b AS msi, silver_bec_ods.cst_item_costs AS cic, silver_bec_ods.csi_item_instances AS cii, silver_bec_ods.hz_party_sites AS hps, silver_bec_ods.hz_locations AS hl
  WHERE
    mmt.transaction_type_id IN (31, 41)
    AND mmt.inventory_item_id = msi.inventory_item_id
    AND mmt.organization_id = msi.organization_id
    AND mmt.transaction_id = mut.TRANSACTION_ID()
    AND mmt.organization_id = 245
    AND mmt.inventory_item_id = cic.inventory_item_id
    AND mmt.organization_id = cic.organization_id
    AND cic.cost_type_id = 1
    AND SUBSTRING(mmt.transaction_reference, 1, REGEXP_INSTR(mmt.transaction_reference, '~', 1) - 1) = cii.EXTERNAL_REFERENCE()
    AND cii.location_Id = hps.PARTY_SITE_ID()
    AND hps.location_id = hl.LOCATION_ID() /* and mmt.transaction_date > '08-JUN-2020' */
    AND transaction_reference LIKE '%~%'
);
UPDATE bec_etl_ctrl.batch_dw_info SET load_type = 'I', last_refresh_date = CURRENT_TIMESTAMP()
WHERE
  dw_table_name = 'fact_fs1_debrief_trx_rt' AND batch_name = 'inv';