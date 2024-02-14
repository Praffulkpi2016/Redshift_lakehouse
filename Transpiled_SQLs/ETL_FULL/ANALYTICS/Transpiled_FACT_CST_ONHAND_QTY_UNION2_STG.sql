DROP table IF EXISTS gold_bec_dwh.FACT_CST_ONHAND_QTY_UNION2_STG;
CREATE TABLE gold_bec_dwh.FACT_CST_ONHAND_QTY_UNION2_STG AS
(
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
    mut.transaction_date,
    msn.initialization_date,
    mmt.transaction_source_type_id,
    mmt.transaction_type_id,
    msn.lot_number,
    msn.serial_number,
    (
      mil.segment1 || '.' || mil.segment2 || '.' || mil.segment3
    ) AS locator,
    msn.current_subinventory_code,
    sub.material_account,
    sub.asset_inventory,
    sub.attribute10,
    sub.attribute11,
    mmt.vmi_flag,
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
    ) || '-' || COALESCE(mmt.segment1, 'NA') || '-' || COALESCE(mmt.organization_id, 0) || '-' || COALESCE(mmt.locator, 'NA') || '-' || COALESCE(msn.lot_number, 'NA') || '-' || COALESCE(mut.transaction_date, '1900-01-01 12:00:00') || '-' || COALESCE(msn.serial_number, 'NA') AS dw_load_id,
    CURRENT_TIMESTAMP() AS dw_insert_date,
    CURRENT_TIMESTAMP() AS dw_update_date
  FROM gold_bec_dwh.FACT_CST_ONHAND_QTY_STG AS mmt, (
    SELECT
      *
    FROM silver_bec_ods.mtl_serial_numbers
    WHERE
      is_deleted_flg <> 'Y'
  ) AS msn, (
    SELECT
      *
    FROM silver_bec_ods.mtl_unit_transactions
    WHERE
      is_deleted_flg <> 'Y'
  ) AS mut, (
    SELECT
      *
    FROM silver_bec_ods.mtl_secondary_inventories
    WHERE
      is_deleted_flg <> 'Y'
  ) AS sub, (
    SELECT
      *
    FROM silver_bec_ods.mtl_item_locations
    WHERE
      is_deleted_flg <> 'Y'
  ) AS mil
  WHERE
    msn.current_organization_id = mmt.organization_id
    AND msn.last_transaction_id = mut.transaction_id
    AND mut.transaction_id = mmt.TRANSACTION_ID()
    AND msn.serial_number = mut.SERIAL_NUMBER()
    AND mil.INVENTORY_LOCATION_ID() = msn.current_locator_id
    AND msn.current_status = 3
    AND mmt.serial_number_control_code <> 1
    AND msn.inventory_item_id = mmt.inventory_item_id
    AND msn.current_subinventory_code = sub.SECONDARY_INVENTORY_NAME()
    AND msn.current_organization_id = sub.ORGANIZATION_ID()
);
UPDATE bec_etl_ctrl.batch_dw_info SET load_type = 'I', last_refresh_date = CURRENT_TIMESTAMP()
WHERE
  dw_table_name = 'fact_cst_onhand_qty_union2_stg' AND batch_name = 'costing';