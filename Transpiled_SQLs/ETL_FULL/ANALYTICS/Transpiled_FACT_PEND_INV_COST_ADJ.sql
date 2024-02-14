DROP table IF EXISTS gold_bec_dwh.FACT_PEND_INV_COST_ADJ;
CREATE TABLE gold_bec_dwh.FACT_PEND_INV_COST_ADJ AS
(
  SELECT
    inventory_item_id,
    organization_id,
    item,
    description,
    uom,
    subinventory_code,
    organization_code,
    new_cost_element_id,
    cost_type,
    new_cost_type_id,
    cost_element,
    old_unit_cost,
    new_unit_cost,
    onhand,
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
    ) || '-' || COALESCE(inventory_item_id, 0) || '-' || COALESCE(organization_id, 0) || '-' || COALESCE(subinventory_code, 'NA') || '-' || COALESCE(new_cost_element_id, 0) || '-' || COALESCE(new_cost_type_id, 0) AS dw_load_id,
    CURRENT_TIMESTAMP() AS dw_insert_date,
    CURRENT_TIMESTAMP() AS dw_update_date
  FROM (
    SELECT
      q.inventory_item_id,
      q.organization_id,
      m.segment1 AS item,
      REPLACE(m.description, '&', ' AND ') AS description,
      m.primary_uom_code AS uom,
      q.subinventory_code,
      o.organization_code,
      new1.cost_element_id AS new_cost_element_id,
      (
        SELECT
          COST_TYPE
        FROM (
          SELECT
            *
          FROM silver_bec_ods.cst_cost_types
          WHERE
            is_deleted_flg <> 'Y'
        ) AS cct
        WHERE
          cct.COST_TYPE_ID = new1.cost_type_id
      ) AS cost_type,
      new1.cost_type_id AS new_cost_type_id,
      CASE
        WHEN new1.cost_element_id = 1
        THEN 'Material'
        WHEN new1.cost_element_id = 2
        THEN 'Material Overhead'
        WHEN new1.cost_element_id = 3
        THEN 'Resource'
        WHEN new1.cost_element_id = 4
        THEN 'Outside Processing'
        WHEN new1.cost_element_id = 5
        THEN 'Overhead'
        ELSE 'Others'
      END AS cost_element,
      COALESCE(old1.old_unit_cost, 0) AS old_unit_cost,
      COALESCE(new1.new_unit_cost, 0) AS new_unit_cost,
      SUM(q.transaction_quantity) AS onhand
    FROM (
      SELECT
        *
      FROM silver_bec_ods.mtl_onhand_quantities_detail
      WHERE
        is_deleted_flg <> 'Y'
    ) AS q
    INNER JOIN (
      SELECT
        *
      FROM silver_bec_ods.mtl_system_items_b
      WHERE
        is_deleted_flg <> 'Y'
    ) AS m
      ON q.inventory_item_id = m.inventory_item_id
      AND q.organization_id = m.organization_id
      AND q.owning_organization_id = m.organization_id
    INNER JOIN (
      SELECT
        *
      FROM silver_bec_ods.org_organization_definitions
      WHERE
        is_deleted_flg <> 'Y'
    ) AS o
      ON q.organization_id = o.organization_id
    INNER JOIN (
      SELECT
        *
      FROM silver_bec_ods.mtl_secondary_inventories
      WHERE
        is_deleted_flg <> 'Y'
    ) AS s
      ON s.secondary_inventory_name = q.subinventory_code
      AND s.organization_id = q.organization_id
      AND COALESCE(s.disable_date, (
        CURRENT_TIMESTAMP() + 999
      )) > CURRENT_TIMESTAMP()
      AND s.asset_inventory = 1
    INNER JOIN (
      SELECT
        inventory_item_id,
        organization_id,
        cost_element_id,
        cost_type_id,
        SUM(ITEM_COST) AS new_unit_cost
      FROM (
        SELECT
          *
        FROM silver_bec_ods.CST_ITEM_COST_DETAILS
        WHERE
          is_deleted_flg <> 'Y'
      ) AS CST_ITEM_COST_DETAILS
      WHERE
        1 = 1
      GROUP BY
        inventory_item_id,
        organization_id,
        cost_element_id,
        cost_type_id
    ) AS new1
      ON q.inventory_item_id = new1.inventory_item_id
      AND q.organization_id = new1.organization_id
    LEFT OUTER JOIN (
      SELECT
        inventory_item_id,
        organization_id,
        cost_element_id,
        cost_type_id,
        SUM(ITEM_COST) AS old_unit_cost
      FROM (
        SELECT
          *
        FROM silver_bec_ods.CST_ITEM_COST_DETAILS
        WHERE
          is_deleted_flg <> 'Y'
      ) AS CST_ITEM_COST_DETAILS
      WHERE
        1 = 1 AND cost_type_id = 1
      GROUP BY
        inventory_item_id,
        organization_id,
        cost_element_id,
        cost_type_id
    ) AS old1
      ON q.inventory_item_id = old1.inventory_item_id
      AND q.organization_id = old1.organization_id
      AND new1.cost_element_id = old1.cost_element_id
    GROUP BY
      q.inventory_item_id,
      q.organization_id,
      m.segment1,
      m.description,
      m.primary_uom_code,
      new1.cost_element_id,
      new1.cost_type_id,
      old1.old_unit_cost,
      new1.new_unit_cost,
      q.subinventory_code,
      o.organization_code
  )
);
UPDATE bec_etl_ctrl.batch_dw_info SET load_type = 'I', last_refresh_date = CURRENT_TIMESTAMP()
WHERE
  dw_table_name = 'fact_pend_inv_cost_adj' AND batch_name = 'costing';