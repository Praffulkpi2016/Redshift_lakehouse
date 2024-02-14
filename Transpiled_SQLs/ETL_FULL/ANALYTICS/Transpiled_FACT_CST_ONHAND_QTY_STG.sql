DROP table IF EXISTS gold_bec_dwh.FACT_CST_ONHAND_QTY_STG;
CREATE TABLE gold_bec_dwh.FACT_CST_ONHAND_QTY_STG AS
(
  SELECT
    msi.segment1,
    msi.description,
    mmt.transaction_date,
    mmt.transaction_action_id,
    mmt.logical_transaction,
    msi.primary_unit_of_measure,
    msi.inventory_item_status_code,
    msi.planning_make_buy_code,
    msi.mrp_planning_code,
    mmt.revision,
    mmt.transaction_type_id,
    mmt.transaction_source_type_id,
    mmt.transaction_source_id,
    mmt.subinventory_code,
    mmt.transfer_subinventory,
    msi.primary_uom_code,
    msi.serial_number_control_code,
    msi.lot_control_code,
    CASE
      WHEN mmt.owning_organization_id = mmt.organization_id
      OR (
        mmt.owning_organization_id IS NULL AND mmt.organization_id IS NULL
      )
      THEN 'N'
      ELSE 'Y'
    END AS vmi_flag,
    mmt.new_cost,
    mmt.transaction_id,
    mmt.transaction_set_id,
    mmt.created_by,
    mmt.transaction_reference,
    mil1.segment1 || '.' || mil1.segment2 || '.' || mil1.segment3 AS locator,
    mmt.locator_id,
    mmt.inventory_item_id,
    mmt.organization_id,
    mmt.rcv_transaction_id,
    mmt.move_order_line_id,
    mmt.move_transaction_id,
    mmt.rma_line_id,
    mmt.operation_seq_num,
    mmt.distribution_account_id,
    mmt.shipment_number,
    mmt.last_update_date,
    mmt.primary_quantity AS mmt_primary_quantity,
    mc.segment1 AS cat_seg1,
    mc.segment2 AS cat_seg2,
    (
      mc.segment1 || '.' || mc.segment2
    ) AS category,
    ood.organization_name,
    ood.operating_unit AS operating_unit,
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
    ) || '-' || COALESCE(mmt.inventory_item_id, 0) || '-' || COALESCE(mmt.organization_id, 0) || '-' || COALESCE(mmt.transaction_id, 0) AS dw_load_id,
    CURRENT_TIMESTAMP() AS dw_insert_date,
    CURRENT_TIMESTAMP() AS dw_update_date
  FROM (
    SELECT
      *
    FROM silver_bec_ods.mtl_material_transactions
    WHERE
      is_deleted_flg <> 'Y'
  ) AS mmt, (
    SELECT
      *
    FROM silver_bec_ods.mtl_system_items_b
    WHERE
      is_deleted_flg <> 'Y'
  ) AS msi, (
    SELECT
      *
    FROM silver_bec_ods.mtl_item_locations
    WHERE
      is_deleted_flg <> 'Y'
  ) AS mil1, (
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
    FROM silver_bec_ods.org_organization_definitions
    WHERE
      is_deleted_flg <> 'Y'
  ) AS ood
  WHERE
    msi.inventory_item_id = mic.INVENTORY_ITEM_ID()
    AND msi.organization_id = mic.ORGANIZATION_ID()
    AND mic.category_id = mc.CATEGORY_ID()
    AND mic.CATEGORY_SET_ID() = 1
    AND msi.organization_id = ood.organization_id
    AND COALESCE(ood.disable_date, (
      CURRENT_TIMESTAMP() + 999
    )) > CURRENT_TIMESTAMP()
    AND mmt.inventory_item_id = msi.INVENTORY_ITEM_ID()
    AND mmt.organization_id = msi.ORGANIZATION_ID()
    AND mmt.locator_id = mil1.INVENTORY_LOCATION_ID()
    AND mmt.organization_id = mil1.ORGANIZATION_ID()
);
UPDATE bec_etl_ctrl.batch_dw_info SET load_type = 'I', last_refresh_date = CURRENT_TIMESTAMP()
WHERE
  dw_table_name = 'fact_cst_onhand_qty_stg' AND batch_name = 'costing';