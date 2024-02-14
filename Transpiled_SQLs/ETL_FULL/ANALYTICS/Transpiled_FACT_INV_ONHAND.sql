DROP table IF EXISTS gold_bec_dwh.FACT_INV_ONHAND;
CREATE TABLE gold_bec_dwh.FACT_INV_ONHAND AS
SELECT DISTINCT
  TRANSACTION_ID,
  inventory_item_id,
  organization_id,
  cost_type_id,
  locator_id,
  (
    SELECT
      system_id
    FROM bec_etl_ctrl.etlsourceappid
    WHERE
      source_system = 'EBS'
  ) || '-' || TRANSACTION_ID AS TRANSACTION_ID_KEY,
  (
    SELECT
      system_id
    FROM bec_etl_ctrl.etlsourceappid
    WHERE
      source_system = 'EBS'
  ) || '-' || inventory_item_id AS INVENTORY_ITEM_ID_KEY,
  (
    SELECT
      system_id
    FROM bec_etl_ctrl.etlsourceappid
    WHERE
      source_system = 'EBS'
  ) || '-' || organization_id AS ORGANIZATION_ID_KEY,
  (
    SELECT
      system_id
    FROM bec_etl_ctrl.etlsourceappid
    WHERE
      source_system = 'EBS'
  ) || '-' || cost_type_id AS COST_TYPE_ID_KEY,
  (
    SELECT
      system_id
    FROM bec_etl_ctrl.etlsourceappid
    WHERE
      source_system = 'EBS'
  ) || '-' || locator_id AS LOCATOR_ID_KEY,
  part_number,
  part_desc,
  primary_unit_of_measure,
  primary_uom_code,
  inventory_item_status_code,
  PLANNING_MAKE_BUY_CODE,
  MRP_PLANNING_CODE,
  material_cost,
  material_overhead_cost,
  resource_cost,
  outside_processing_cost,
  overhead_cost,
  item_cost,
  subinventory,
  serial_number,
  lot_number,
  QUANTITY,
  EXT_MATERIAL_COST,
  EXT_MATERIAL_OVERHEAD_COST,
  EXT_RESOURCE_COST,
  EXT_OUTSIDE_PROCESSING_COST,
  EXT_OVERHEAD_COST,
  EXTENDED_COST,
  transaction_date,
  TRANSACTION_TYPE,
  ATTRIBUTE1,
  attribute3,
  attribute4,
  rcv_attr4,
  rcv_attr5,
  rcv_attr6,
  transaction_source_type_id,
  transaction_source_type_name,
  transaction_source_id,
  source_line_id,
  trx_source_line_id,
  organization_name,
  revision,
  move_order_line_id,
  subinventory_type,
  vmi_flag,
  onhand_by_subinventory,
  (
    SELECT
      SUM(qty.primary_transaction_quantity)
    FROM silver_bec_ods.mtl_onhand_quantities_detail AS qty
    WHERE
      is_deleted_flg <> 'Y'
      AND qty.inventory_item_id = inv_onhand.inventory_item_id
      AND qty.organization_id = inv_onhand.organization_id
  ) AS org_onhand_qty,
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
  ) || '-' || COALESCE(TRANSACTION_ID, 0) || '-' || COALESCE(inventory_item_id, 0) || '-' || COALESCE(organization_id, 0) || '-' || COALESCE(serial_number, 'NA') || '-' || COALESCE(lot_number, 'NA') AS dw_load_id,
  CURRENT_TIMESTAMP() AS dw_insert_date,
  CURRENT_TIMESTAMP() AS dw_update_date
FROM (
  SELECT
    miq.create_transaction_id AS TRANSACTION_ID,
    miq.inventory_item_id,
    miq.organization_id,
    ctc.cost_type_id,
    miq.locator_id,
    msi.segment1 AS part_number,
    msi.description AS part_desc,
    msi.primary_unit_of_measure,
    msi.primary_uom_code,
    msi.inventory_item_status_code,
    CASE
      WHEN msi.planning_make_buy_code = 1
      THEN 'Make'
      WHEN msi.planning_make_buy_code = 2
      THEN 'Buy'
    END AS PLANNING_MAKE_BUY_CODE,
    (
      SELECT
        mfl.meaning
      FROM (
        SELECT
          *
        FROM silver_bec_ods.fnd_lookup_values
        WHERE
          is_deleted_flg <> 'Y'
      ) AS mfl
      WHERE
        mfl.lookup_type = 'MRP_PLANNING_CODE' AND mfl.lookup_code = msi.mrp_planning_code
    ) AS MRP_PLANNING_CODE, /*           mc.segment1 CAT_SEG1, */ /*			 mc.segment2 CAT_SEG2, */ /*			 mc.segment1 || '.' || mc.segment2 CATEGORY, */
    ctc.material_cost,
    ctc.material_overhead_cost,
    ctc.resource_cost,
    ctc.outside_processing_cost,
    ctc.overhead_cost,
    ctc.item_cost,
    miq.subinventory_code AS subinventory,
    NULL AS serial_number, /* mit.segment1|| '.' || mit.segment2  || '.' || mit.segment3 AS LOCATOR, */
    miq.lot_number,
    SUM(miq.primary_transaction_quantity) AS QUANTITY,
    SUM(miq.primary_transaction_quantity) * ctc.material_cost AS EXT_MATERIAL_COST,
    SUM(miq.primary_transaction_quantity) * ctc.material_overhead_cost AS EXT_MATERIAL_OVERHEAD_COST,
    SUM(miq.primary_transaction_quantity) * ctc.resource_cost AS EXT_RESOURCE_COST,
    SUM(miq.primary_transaction_quantity) * ctc.outside_processing_cost AS EXT_OUTSIDE_PROCESSING_COST,
    SUM(miq.primary_transaction_quantity) * ctc.overhead_cost AS EXT_OVERHEAD_COST,
    SUM(miq.primary_transaction_quantity) * ctc.item_cost AS EXTENDED_COST,
    mmt.transaction_date,
    mtt.transaction_type_name AS TRANSACTION_TYPE,
    mmt.attribute1 AS ATTRIBUTE1,
    mmt.attribute3 AS attribute3,
    mmt.attribute4 AS attribute4,
    rct.attribute4 AS rcv_attr4,
    rct.attribute5 AS rcv_attr5,
    rct.attribute6 AS rcv_attr6,
    mmt.transaction_source_type_id,
    mts.transaction_source_type_name,
    CASE
      WHEN mts.transaction_source_type_name = 'Job or Schedule'
      THEN (
        SELECT
          wip_entity_name
        FROM (
          SELECT
            *
          FROM silver_bec_ods.wip_entities
          WHERE
            is_deleted_flg <> 'Y'
        ) AS wip_entities
        WHERE
          wip_entity_id = mmt.transaction_source_id
      )
      WHEN mts.transaction_source_type_name = 'RMA'
      THEN CAST((
        SELECT
          order_number
        FROM (
          SELECT
            *
          FROM silver_bec_ods.oe_order_lines_all
          WHERE
            is_deleted_flg <> 'Y'
        ) AS ola, (
          SELECT
            *
          FROM silver_bec_ods.oe_order_headers_all
          WHERE
            is_deleted_flg <> 'Y'
        ) AS oha
        WHERE
          ola.line_id = mmt.trx_source_line_id AND ola.header_id = oha.header_id
      ) AS STRING)
      ELSE CAST(mmt.transaction_source_id AS STRING)
    END AS transaction_source_id,
    mmt.source_line_id,
    mmt.trx_source_line_id,
    ood.organization_name,
    miq.revision,
    mmt.move_order_line_id,
    CASE
      WHEN sub.asset_inventory = 1
      THEN 'Asset'
      WHEN sub.asset_inventory = 2
      THEN 'Expense'
      ELSE 'asset_inventory'
    END AS subinventory_type,
    CASE
      WHEN miq.owning_organization_id = miq.organization_id
      OR (
        miq.owning_organization_id IS NULL AND miq.organization_id IS NULL
      )
      THEN 'N'
      ELSE 'Y'
    END AS vmi_flag,
    (
      SELECT
        SUM(primary_transaction_quantity)
      FROM (
        SELECT
          *
        FROM silver_bec_ods.mtl_onhand_quantities_detail
        WHERE
          is_deleted_flg <> 'Y'
      ) AS mq
      WHERE
        miq.inventory_item_id = mq.inventory_item_id
        AND miq.organization_id = mq.organization_id
        AND miq.subinventory_code = mq.subinventory_code
      GROUP BY
        inventory_item_id,
        organization_id,
        subinventory_code
    ) AS onhand_by_subinventory
  FROM (
    SELECT
      *
    FROM silver_bec_ods.mtl_system_items_b
    WHERE
      is_deleted_flg <> 'Y'
  ) AS msi, (
    SELECT
      *
    FROM silver_bec_ods.mtl_onhand_quantities_detail
    WHERE
      is_deleted_flg <> 'Y'
  ) AS miq, (
    SELECT
      *
    FROM silver_bec_ods.mtl_material_transactions
    WHERE
      is_deleted_flg <> 'Y'
  ) AS mmt, (
    SELECT
      *
    FROM silver_bec_ods.mtl_transaction_types
    WHERE
      is_deleted_flg <> 'Y'
  ) AS mtt, (
    SELECT
      *
    FROM silver_bec_ods.cst_item_costs
    WHERE
      is_deleted_flg <> 'Y'
  ) AS ctc, (
    SELECT
      *
    FROM silver_bec_ods.rcv_transactions
    WHERE
      is_deleted_flg <> 'Y'
  ) AS rct, (
    SELECT
      *
    FROM silver_bec_ods.mtl_txn_source_types
    WHERE
      is_deleted_flg <> 'Y'
  ) AS mts, (
    SELECT
      *
    FROM silver_bec_ods.org_organization_definitions
    WHERE
      is_deleted_flg <> 'Y'
  ) AS ood, (
    SELECT
      *
    FROM silver_bec_ods.mtl_secondary_inventories
    WHERE
      is_deleted_flg <> 'Y'
  ) AS sub
  WHERE
    msi.inventory_item_id = miq.inventory_item_id
    AND miq.organization_id = msi.organization_id
    AND miq.create_transaction_id = mmt.TRANSACTION_ID()
    AND mmt.transaction_source_type_id = mts.TRANSACTION_SOURCE_TYPE_ID()
    AND msi.serial_number_control_code = 1
    AND mmt.transaction_type_id = mtt.TRANSACTION_TYPE_ID()
    AND mmt.rcv_transaction_id = rct.TRANSACTION_ID()
    AND miq.organization_id = ctc.ORGANIZATION_ID()
    AND miq.inventory_item_id = ctc.INVENTORY_ITEM_ID()
    AND ctc.COST_TYPE_ID() = '1'
    AND msi.organization_id = ood.organization_id
    AND miq.subinventory_code = sub.SECONDARY_INVENTORY_NAME()
    AND miq.organization_id = sub.ORGANIZATION_ID()
  GROUP BY
    miq.inventory_item_id,
    miq.organization_id,
    ctc.cost_type_id,
    miq.locator_id,
    msi.segment1,
    msi.description,
    msi.primary_unit_of_measure,
    msi.primary_uom_code,
    miq.subinventory_code,
    miq.lot_number,
    miq.create_transaction_id,
    mmt.transaction_date,
    mtt.transaction_type_name,
    msi.organization_id,
    mmt.attribute1,
    mmt.attribute3,
    mmt.attribute4,
    rct.attribute4,
    rct.attribute5,
    rct.attribute6,
    msi.inventory_item_status_code,
    msi.planning_make_buy_code,
    msi.mrp_planning_code,
    ctc.material_cost,
    ctc.material_overhead_cost,
    ctc.resource_cost,
    ctc.outside_processing_cost,
    ctc.overhead_cost,
    ctc.item_cost,
    mmt.transaction_source_type_id,
    mts.transaction_source_type_name,
    mmt.transaction_source_id,
    mmt.source_line_id,
    mmt.trx_source_line_id,
    ood.organization_name,
    miq.revision,
    mmt.move_order_line_id,
    CASE
      WHEN sub.asset_inventory = 1
      THEN 'Asset'
      WHEN sub.asset_inventory = 2
      THEN 'Expense'
      ELSE 'asset_inventory'
    END,
    CASE
      WHEN miq.owning_organization_id = miq.organization_id
      OR (
        miq.owning_organization_id IS NULL AND miq.organization_id IS NULL
      )
      THEN 'N'
      ELSE 'Y'
    END
  UNION ALL
  SELECT DISTINCT
    msn.last_transaction_id AS TRANSACTION_ID,
    msi.inventory_item_id,
    msi.organization_id,
    ctc.cost_type_id,
    msn.current_locator_id AS locator_id,
    msi.segment1 AS part_number,
    msi.description AS part_desc,
    msi.primary_unit_of_measure,
    msi.primary_uom_code,
    msi.inventory_item_status_code,
    CASE
      WHEN msi.planning_make_buy_code = 1
      THEN 'Make'
      WHEN msi.planning_make_buy_code = 2
      THEN 'Buy'
    END AS PLANNING_MAKE_BUY_CODE,
    (
      SELECT
        mfl.meaning
      FROM (
        SELECT
          *
        FROM silver_bec_ods.fnd_lookup_values
        WHERE
          is_deleted_flg <> 'Y'
      ) AS mfl
      WHERE
        mfl.lookup_type = 'MRP_PLANNING_CODE' AND mfl.lookup_code = msi.mrp_planning_code
    ) AS MRP_PLANNING_CODE, /* mc.segment1 CAT_SEG1, */ /* mc.segment2 CAT_SEG2, */ /* mc.segment1 || '.' || mc.segment2 CATEGORY, */
    ctc.material_cost,
    ctc.material_overhead_cost,
    ctc.resource_cost,
    ctc.outside_processing_cost,
    ctc.overhead_cost,
    ctc.item_cost,
    msn.current_subinventory_code AS subinventory,
    msn.serial_number,
    msn.lot_number,
    1 AS quantity,
    ctc.material_cost AS EXT_MATERIAL_COST,
    ctc.material_overhead_cost AS EXT_MATERIAL_OVERHEAD_COST,
    ctc.resource_cost AS EXT_RESOURCE_COST,
    ctc.outside_processing_cost AS EXT_OUTSIDE_PROCESSING_COST,
    ctc.overhead_cost AS EXT_OVERHEAD_COST,
    ctc.item_cost AS EXTENDED_COST,
    mut.transaction_date,
    mtt.transaction_type_name AS TRANSACTION_TYPE,
    mmt.attribute1,
    mmt.attribute3,
    mmt.attribute4,
    rct.attribute4 AS rcv_attr4,
    rct.attribute5 AS rcv_attr5,
    rct.attribute6 AS rcv_attr6,
    mmt.transaction_source_type_id,
    mts.transaction_source_type_name,
    CASE
      WHEN mts.transaction_source_type_name = 'Job or Schedule'
      THEN (
        SELECT
          wip_entity_name
        FROM (
          SELECT
            *
          FROM silver_bec_ods.wip_entities
          WHERE
            is_deleted_flg <> 'Y'
        ) AS wip_entities
        WHERE
          wip_entity_id = mmt.transaction_source_id
      )
      WHEN mts.transaction_source_type_name = 'RMA'
      THEN CAST((
        SELECT
          order_number
        FROM (
          SELECT
            *
          FROM silver_bec_ods.oe_order_lines_all
          WHERE
            is_deleted_flg <> 'Y'
        ) AS ola, (
          SELECT
            *
          FROM silver_bec_ods.oe_order_headers_all
          WHERE
            is_deleted_flg <> 'Y'
        ) AS oha
        WHERE
          ola.line_id = mmt.trx_source_line_id AND ola.header_id = oha.header_id
      ) AS STRING)
      ELSE CAST(mmt.transaction_source_id AS STRING)
    END AS transaction_source_id,
    mmt.source_line_id,
    mmt.trx_source_line_id,
    ood.organization_name,
    mmt.revision,
    mmt.move_order_line_id,
    CASE
      WHEN sub.asset_inventory = 1
      THEN 'Asset'
      WHEN sub.asset_inventory = 2
      THEN 'Expense'
      ELSE 'asset_inventory'
    END AS subinventory_type,
    CASE
      WHEN msn.owning_organization_id = msn.current_organization_id
      OR (
        msn.owning_organization_id IS NULL AND msn.current_organization_id IS NULL
      )
      THEN 'N'
      ELSE 'Y'
    END AS vmi_flag,
    0 AS onhand_by_subinventory
  FROM (
    SELECT
      *
    FROM silver_bec_ods.mtl_system_items_b
    WHERE
      is_deleted_flg <> 'Y'
  ) AS msi, (
    SELECT
      *
    FROM silver_bec_ods.mtl_serial_numbers
    WHERE
      is_deleted_flg <> 'Y'
  ) AS msn, (
    SELECT
      *
    FROM silver_bec_ods.mtl_material_transactions
    WHERE
      is_deleted_flg <> 'Y'
  ) AS mmt, (
    SELECT
      *
    FROM silver_bec_ods.mtl_unit_transactions
    WHERE
      is_deleted_flg <> 'Y'
  ) AS mut, (
    SELECT
      *
    FROM silver_bec_ods.mtl_transaction_types
    WHERE
      is_deleted_flg <> 'Y'
  ) AS mtt, (
    SELECT
      *
    FROM silver_bec_ods.cst_item_costs
    WHERE
      is_deleted_flg <> 'Y'
  ) AS ctc, (
    SELECT
      *
    FROM silver_bec_ods.rcv_transactions
    WHERE
      is_deleted_flg <> 'Y'
  ) AS rct, (
    SELECT
      *
    FROM silver_bec_ods.mtl_txn_source_types
    WHERE
      is_deleted_flg <> 'Y'
  ) AS mts, (
    SELECT
      *
    FROM silver_bec_ods.org_organization_definitions
    WHERE
      is_deleted_flg <> 'Y'
  ) AS ood, (
    SELECT
      *
    FROM silver_bec_ods.mtl_secondary_inventories
    WHERE
      is_deleted_flg <> 'Y'
  ) AS sub
  WHERE
    msi.inventory_item_id = msn.inventory_item_id
    AND msn.current_organization_id = msi.organization_id
    AND msn.last_transaction_id = mut.TRANSACTION_ID()
    AND mut.transaction_id = mmt.TRANSACTION_ID()
    AND msn.serial_number = mut.SERIAL_NUMBER()
    AND mmt.transaction_type_id = mtt.TRANSACTION_TYPE_ID()
    AND mmt.transaction_source_type_id = mts.TRANSACTION_SOURCE_TYPE_ID()
    AND msn.current_status = 3
    AND msi.serial_number_control_code <> 1
    AND mmt.rcv_transaction_id = rct.TRANSACTION_ID()
    AND msn.current_organization_id = ctc.ORGANIZATION_ID()
    AND msn.inventory_item_id = ctc.INVENTORY_ITEM_ID()
    AND ctc.COST_TYPE_ID() = '1'
    AND msn.current_subinventory_code = sub.SECONDARY_INVENTORY_NAME()
    AND msn.current_organization_id = sub.ORGANIZATION_ID()
    AND msi.organization_id = ood.organization_id
  ORDER BY
    subinventory NULLS LAST,
    part_number NULLS LAST,
    serial_number ASC NULLS LAST
) AS inv_onhand;
UPDATE bec_etl_ctrl.batch_dw_info SET load_type = 'I', last_refresh_date = CURRENT_TIMESTAMP()
WHERE
  dw_table_name = 'fact_inv_onhand' AND batch_name = 'inv';