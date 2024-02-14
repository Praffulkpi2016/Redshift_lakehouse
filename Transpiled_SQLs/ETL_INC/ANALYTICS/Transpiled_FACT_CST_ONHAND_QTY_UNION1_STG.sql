TRUNCATE table gold_bec_dwh.FACT_CST_ONHAND_QTY_UNION1_STG;
INSERT INTO gold_bec_dwh.FACT_CST_ONHAND_QTY_UNION1_STG
(
  SELECT
    segment1,
    cat_seg1,
    cat_seg2,
    category,
    organization_name,
    operating_unit,
    description,
    primary_unit_of_measure,
    primary_uom_code,
    inventory_item_status_code,
    planning_make_buy_code,
    mrp_planning_code,
    inventory_item_id,
    organization_id,
    subinventory_code,
    locator_id,
    create_transaction_id,
    lot_number,
    transaction_date,
    transaction_source_type_id,
    transaction_type_id,
    locator,
    material_account,
    asset_inventory,
    attribute10,
    attribute11,
    vmi_flag,
    'N' AS is_deleted_flg, /* audit columns */
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
    ) || '-' || COALESCE(segment1, 'NA') || '-' || COALESCE(organization_id, 0) || '-' || COALESCE(locator, 'NA') || '-' || COALESCE(lot_number, 'NA') || '-' || COALESCE(transaction_date, '1900-01-01 12:00:00') || '-' || COALESCE(subinventory_code, 'NA') || '-' || COALESCE(create_transaction_id, 0) AS dw_load_id,
    CURRENT_TIMESTAMP() AS dw_insert_date,
    CURRENT_TIMESTAMP() AS dw_update_date
  FROM (
    SELECT
      mmt.segment1,
      mmt.cat_seg1,
      mmt.cat_seg2,
      mmt.category,
      mmt.organization_name,
      mmt.operating_unit,
      mmt.description,
      mmt.primary_unit_of_measure,
      mmt.primary_uom_code,
      mmt.inventory_item_status_code,
      mmt.planning_make_buy_code,
      mmt.mrp_planning_code,
      mmt.inventory_item_id,
      mmt.organization_id,
      miq.subinventory_code,
      miq.locator_id,
      miq.create_transaction_id,
      miq.lot_number,
      mmt.transaction_date,
      mmt.transaction_source_type_id,
      mmt.transaction_type_id,
      mmt.locator,
      sub.material_account,
      sub.asset_inventory,
      sub.attribute10,
      sub.attribute11,
      mmt.vmi_flag
    FROM (
      SELECT
        *
      FROM silver_bec_ods.mtl_onhand_quantities_detail
      WHERE
        is_deleted_flg <> 'Y'
    ) AS miq, gold_bec_dwh.FACT_CST_ONHAND_QTY_STG AS mmt, (
      SELECT
        *
      FROM silver_bec_ods.mtl_secondary_inventories
      WHERE
        is_deleted_flg <> 'Y'
    ) AS sub
    WHERE
      miq.inventory_item_id = mmt.inventory_item_id
      AND miq.organization_id = mmt.organization_id
      AND miq.create_transaction_id = mmt.transaction_id
      AND COALESCE(miq.locator_id, -99) = COALESCE(mmt.locator_id, -99)
      AND mmt.serial_number_control_code = 1
      AND miq.subinventory_code = sub.SECONDARY_INVENTORY_NAME()
      AND miq.organization_id = sub.ORGANIZATION_ID()
    GROUP BY
      mmt.segment1,
      mmt.cat_seg1,
      mmt.cat_seg2,
      mmt.category,
      mmt.organization_name,
      mmt.operating_unit,
      mmt.description,
      mmt.primary_unit_of_measure,
      mmt.primary_uom_code,
      mmt.inventory_item_status_code,
      mmt.planning_make_buy_code,
      mmt.mrp_planning_code,
      mmt.inventory_item_id,
      mmt.organization_id,
      miq.subinventory_code,
      miq.locator_id,
      miq.create_transaction_id,
      miq.lot_number,
      mmt.transaction_date,
      mmt.transaction_source_type_id,
      mmt.transaction_type_id,
      mmt.locator,
      sub.material_account,
      sub.asset_inventory,
      sub.attribute10,
      sub.attribute11,
      mmt.vmi_flag
  )
);
UPDATE bec_etl_ctrl.batch_dw_info SET load_type = 'I', last_refresh_date = CURRENT_TIMESTAMP()
WHERE
  dw_table_name = 'fact_cst_onhand_qty_union1_stg' AND batch_name = 'costing';