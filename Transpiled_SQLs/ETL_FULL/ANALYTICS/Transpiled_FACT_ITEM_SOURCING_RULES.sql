DROP table IF EXISTS gold_bec_dwh.FACT_ITEM_SOURCING_RULES;
CREATE TABLE gold_bec_dwh.FACT_ITEM_SOURCING_RULES AS
(
  SELECT
    sso.SR_SOURCE_ID,
    (
      SELECT
        system_id
      FROM bec_etl_ctrl.etlsourceappid
      WHERE
        source_system = 'EBS'
    ) || '-' || sso.SR_SOURCE_ID AS SR_SOURCE_ID_KEY,
    sr_asgn.assignment_id,
    (
      SELECT
        system_id
      FROM bec_etl_ctrl.etlsourceappid
      WHERE
        source_system = 'EBS'
    ) || '-' || sr_asgn.assignment_id AS assignment_id_key,
    sr_asgn.assignment_set_id,
    (
      SELECT
        system_id
      FROM bec_etl_ctrl.etlsourceappid
      WHERE
        source_system = 'EBS'
    ) || '-' || sr_asgn.assignment_set_id AS assignment_set_id_key,
    sr_asgn.sourcing_rule_id,
    (
      SELECT
        system_id
      FROM bec_etl_ctrl.etlsourceappid
      WHERE
        source_system = 'EBS'
    ) || '-' || sr_asgn.sourcing_rule_id AS sourcing_rule_id_key,
    sr.sourcing_rule_name,
    sr.description AS sourcing_rule_desc,
    sro.effective_date,
    sro.disable_date,
    sso.vendor_id,
    (
      SELECT
        system_id
      FROM bec_etl_ctrl.etlsourceappid
      WHERE
        source_system = 'EBS'
    ) || '-' || sso.vendor_id AS vendor_id_key,
    sso.vendor_site_id,
    (
      SELECT
        system_id
      FROM bec_etl_ctrl.etlsourceappid
      WHERE
        source_system = 'EBS'
    ) || '-' || sso.vendor_site_id AS vendor_site_id_key,
    sso.allocation_percent,
    sso.RANK,
    sso.ship_method,
    sm.intransit_time,
    CASE
      WHEN sso.source_type = 0
      THEN 'ATP'
      WHEN sso.source_type = 1
      THEN 'Transfer From'
      WHEN sso.source_type = 2
      THEN 'Make At'
      WHEN sso.source_type = 3
      THEN 'Buy From'
      WHEN sso.source_type = 4
      THEN 'Return To'
      WHEN sso.source_type = 5
      THEN 'Repair At'
    END AS source_type,
    sr.organization_id AS organization_id,
    (
      SELECT
        system_id
      FROM bec_etl_ctrl.etlsourceappid
      WHERE
        source_system = 'EBS'
    ) || '-' || sr.organization_id AS organization_id_key,
    sro.RECEIPT_ORGANIZATION_ID,
    (
      SELECT
        system_id
      FROM bec_etl_ctrl.etlsourceappid
      WHERE
        source_system = 'EBS'
    ) || '-' || sro.RECEIPT_ORGANIZATION_ID AS RECEIPT_ORGANIZATION_ID_KEY, /* sro.organization_code mrp_organization, */
    sr.planning_active,
    sr_asgn.customer_id,
    (
      SELECT
        system_id
      FROM bec_etl_ctrl.etlsourceappid
      WHERE
        source_system = 'EBS'
    ) || '-' || sr_asgn.customer_id AS customer_id_key,
    sr_asgn.ship_to_site_id,
    (
      SELECT
        system_id
      FROM bec_etl_ctrl.etlsourceappid
      WHERE
        source_system = 'EBS'
    ) || '-' || sr_asgn.ship_to_site_id AS ship_to_site_id_key,
    msib.segment1 AS item,
    msib.description AS item_desc,
    item_cat.item_category,
    item_cat.category_set_name,
    mas.assignment_set_name,
    sso.SOURCE_ORGANIZATION_ID,
    (
      SELECT
        system_id
      FROM bec_etl_ctrl.etlsourceappid
      WHERE
        source_system = 'EBS'
    ) || '-' || sso.SOURCE_ORGANIZATION_ID AS SOURCE_ORGANIZATION_ID_KEY, /* , sso.SOURCE_ORGANIZATION_CODE */
    sr_asgn.inventory_item_id, /* ,sr_asgn.organization_id */
    NULL AS customer_name,
    NULL AS ship_to_address,
    sr_asgn.sourcing_rule_type, /* audit columns */
    'N' AS is_deleted_flg,
    (
      SELECT
        system_id
      FROM bec_etl_ctrl.etlsourceappid
      WHERE
        source_system = 'EBS'
    ) AS source_app_id,
    (
      SELECT
        system_id
      FROM bec_etl_ctrl.etlsourceappid
      WHERE
        source_system = 'EBS'
    ) || '-' || COALESCE(sr_asgn.assignment_id, 0) || '-' || COALESCE(sr_asgn.assignment_set_id, 0) || '-' || COALESCE(sr_asgn.sourcing_rule_id, 0) || '-' || COALESCE(sso.SR_SOURCE_ID, 0) AS dw_load_id,
    CURRENT_TIMESTAMP() AS dw_insert_date,
    CURRENT_TIMESTAMP() AS dw_update_date
  FROM (
    SELECT
      *
    FROM silver_bec_ods.mrp_sr_source_org
    WHERE
      is_deleted_flg <> 'Y'
  ) AS sso, (
    SELECT
      *
    FROM silver_bec_ods.mrp_sr_receipt_org
    WHERE
      is_deleted_flg <> 'Y'
  ) AS sro, (
    SELECT
      *
    FROM silver_bec_ods.mrp_sourcing_rules
    WHERE
      is_deleted_flg <> 'Y'
  ) AS sr, (
    SELECT
      mic.inventory_item_id,
      mic.organization_id,
      mc.segment2 AS item_category,
      CSET.CATEGORY_SET_NAME
    FROM (
      SELECT
        *
      FROM silver_bec_ods.mtl_item_categories
      WHERE
        is_deleted_flg <> 'Y'
    ) AS mic, (
      SELECT
        *
      FROM silver_bec_ods.mtl_categories_b
      WHERE
        is_deleted_flg <> 'Y'
    ) AS mc, (
      SELECT
        *
      FROM silver_bec_ods.MTL_CATEGORY_SETS_TL
      WHERE
        is_deleted_flg <> 'Y'
    ) AS CSET, (
      SELECT
        *
      FROM silver_bec_ods.mtl_system_items_b
      WHERE
        is_deleted_flg <> 'Y'
    ) AS msi
    WHERE
      1 = 1
      AND MIC.CATEGORY_SET_ID = CSET.CATEGORY_SET_ID
      AND MIC.CATEGORY_ID = MC.CATEGORY_ID
      AND CSET.LANGUAGE = 'US'
      AND CSET.CATEGORY_SET_NAME = 'Inventory'
      AND mic.inventory_item_id = msi.inventory_item_id
      AND mic.organization_id = msi.organization_id
      AND msi.enabled_flag = 'Y'
      AND msi.inventory_item_status_code = 'Active'
  ) AS item_cat, (
    SELECT
      *
    FROM silver_bec_ods.mrp_assignment_sets
    WHERE
      is_deleted_flg <> 'Y'
  ) AS mas, (
    SELECT
      *
    FROM silver_bec_ods.mrp_sr_assignments
    WHERE
      is_deleted_flg <> 'Y' AND assignment_type IN (1, 2, 4, 5)
      OR NOT organization_id IS NULL
      OR (
        assignment_type IN (3, 6)
        AND organization_id IS NULL
        AND (
          (
            inventory_item_id IN (
              SELECT
                inventory_item_id
              FROM silver_bec_ods.mtl_system_items_b
              WHERE
                organization_id = 90
            )
          )
        )
      )
  ) AS sr_asgn, (
    SELECT
      *
    FROM silver_bec_ods.MTL_INTERORG_SHIP_METHODS
    WHERE
      is_deleted_flg <> 'Y'
  ) AS SM, (
    SELECT
      *
    FROM silver_bec_ods.mtl_system_items_b
    WHERE
      is_deleted_flg <> 'Y' AND organization_id = 106
  ) AS msib
  WHERE
    1 = 1
    AND sro.sr_receipt_id = sso.sr_receipt_id
    AND sr.sourcing_rule_id = sro.sourcing_rule_id
    AND COALESCE(sr.organization_id, 106) = COALESCE(sro.receipt_organization_id, 106)
    AND sr_asgn.assignment_set_id = mas.ASSIGNMENT_SET_ID()
    AND sr_asgn.SOURCING_RULE_ID() = sro.sourcing_rule_id
    AND mas.ASSIGNMENT_SET_NAME() = 'BLOOM ASSIGNMENT SET'
    AND COALESCE(sr_asgn.ORGANIZATION_ID(), 106) = COALESCE(sro.receipt_organization_id, 106)
    AND sr_asgn.inventory_item_id = item_cat.INVENTORY_ITEM_ID()
    AND sr_asgn.organization_id = item_cat.ORGANIZATION_ID()
    AND sr_asgn.inventory_item_id = msib.INVENTORY_ITEM_ID()
    AND COALESCE(sr_asgn.organization_id, 106) = msib.ORGANIZATION_ID()
    AND sso.ship_method = sm.SHIP_METHOD()
    AND (
      (
        sso.SHIP_METHOD IS NULL
      )
      OR (
        SM.FROM_ORGANIZATION_ID = sso.SOURCE_ORGANIZATION_ID
        AND SM.TO_ORGANIZATION_ID = sro.RECEIPT_ORGANIZATION_ID
      )
    )
  ORDER BY
    sr.sourcing_rule_name NULLS LAST
);
UPDATE bec_etl_ctrl.batch_dw_info SET load_type = 'I', last_refresh_date = CURRENT_TIMESTAMP()
WHERE
  dw_table_name = 'fact_item_sourcing_rules' AND batch_name = 'inv';