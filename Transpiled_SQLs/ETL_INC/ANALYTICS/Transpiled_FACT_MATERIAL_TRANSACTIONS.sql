/* delete */
DELETE FROM gold_bec_dwh.FACT_MATERIAL_TRANSACTIONS
WHERE
  EXISTS(
    SELECT
      1
    FROM silver_bec_ods.mtl_material_transactions AS mmt, silver_bec_ods.mtl_system_items_b AS msi, (
      SELECT DISTINCT
        transaction_id,
        inventory_item_id,
        organization_id,
        serial_number
      FROM silver_bec_ods.mtl_unit_transactions
    ) AS mut, (
      SELECT
        transaction_id,
        inventory_item_id,
        organization_id,
        lot_number,
        SUM(primary_quantity) AS primary_quantity
      FROM silver_bec_ods.mtl_transaction_lot_numbers
      WHERE
        is_deleted_flg <> 'Y'
      GROUP BY
        transaction_id,
        inventory_item_id,
        organization_id,
        lot_number
    ) AS mln, silver_bec_ods.mtl_item_locations AS mil1, silver_bec_ods.mtl_item_locations AS mil2
    WHERE
      mmt.inventory_item_id = msi.INVENTORY_ITEM_ID()
      AND mmt.organization_id = msi.ORGANIZATION_ID()
      AND mmt.transaction_id = mut.TRANSACTION_ID()
      AND mmt.inventory_item_id = mut.INVENTORY_ITEM_ID()
      AND mmt.organization_id = mut.ORGANIZATION_ID()
      AND mmt.transaction_id = mln.TRANSACTION_ID()
      AND mmt.inventory_item_id = mln.INVENTORY_ITEM_ID()
      AND mmt.organization_id = mln.ORGANIZATION_ID()
      AND mmt.locator_id = mil1.INVENTORY_LOCATION_ID()
      AND mmt.organization_id = mil1.ORGANIZATION_ID()
      AND mmt.transfer_locator_id = mil2.INVENTORY_LOCATION_ID()
      AND mmt.organization_id = mil2.ORGANIZATION_ID()
      AND (
        mmt.kca_seq_date > (
          SELECT
            (
              executebegints - prune_days
            )
          FROM bec_etl_ctrl.batch_dw_info
          WHERE
            dw_table_name = 'fact_material_transactions' AND batch_name = 'inv'
        )
      )
      AND fact_material_transactions.transaction_id = mmt.transaction_id
      AND mmt.inventory_item_id = fact_material_transactions.inventory_item_id
      AND mmt.organization_id = fact_material_transactions.organization_id
  );
/* INSERT */
INSERT INTO gold_bec_dwh.FACT_MATERIAL_TRANSACTIONS
SELECT
  msi.segment1,
  msi.description,
  mmt.transaction_date,
  mmt.revision,
  mmt.transaction_type_id,
  mmt.transaction_source_type_id,
  mmt.transaction_source_id,
  mmt.subinventory_code,
  mmt.transfer_subinventory,
  msi.primary_uom_code,
  mmt.new_cost,
  mmt.transaction_id,
  mmt.transaction_set_id,
  mmt.created_by,
  mmt.transaction_reference,
  mil1.segment1 || '.' || mil1.segment2 || '.' || mil1.segment3 AS locator,
  mil2.segment1 || '.' || mil2.segment2 || '.' || mil2.segment3 AS transfer_locator,
  mut.serial_number,
  mln.lot_number,
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
  mln.primary_quantity AS mln_primary_quantity,
  mmt.primary_quantity AS mmt_primary_quantity,
  CASE
    WHEN mln.lot_number IS NULL
    THEN CASE
      WHEN mut.serial_number IS NULL
      THEN mmt.primary_quantity
      ELSE SIGN(mmt.primary_quantity) * 1
    END
    ELSE CASE
      WHEN mut.serial_number IS NULL
      THEN mmt.primary_quantity
      ELSE SIGN(mmt.primary_quantity) * 1
    END / CASE
      WHEN mut.serial_number IS NULL
      THEN mmt.primary_quantity
      ELSE SIGN(mmt.primary_quantity) * 1
    END * mln.primary_quantity
  END AS primary_quantity,
  'N' AS IS_DELETED_FLG, /* audit columns */
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
  ) || '-' || COALESCE(mmt.transaction_id, 0) || '-' || COALESCE(mmt.inventory_item_id, 0) || '-' || COALESCE(mmt.organization_id, 0) || '-' || COALESCE(mut.serial_number, 'NA') || '-' || COALESCE(mln.lot_number, 'NA') AS dw_load_id,
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
  SELECT DISTINCT
    transaction_id,
    inventory_item_id,
    organization_id,
    serial_number
  FROM silver_bec_ods.mtl_unit_transactions
  WHERE
    is_deleted_flg <> 'Y'
) AS mut, (
  SELECT
    transaction_id,
    inventory_item_id,
    organization_id,
    lot_number,
    SUM(primary_quantity) AS primary_quantity
  FROM silver_bec_ods.mtl_transaction_lot_numbers
  WHERE
    is_deleted_flg <> 'Y'
  GROUP BY
    transaction_id,
    inventory_item_id,
    organization_id,
    lot_number
) AS mln, (
  SELECT
    *
  FROM silver_bec_ods.mtl_item_locations
  WHERE
    is_deleted_flg <> 'Y'
) AS mil1, (
  SELECT
    *
  FROM silver_bec_ods.mtl_item_locations
  WHERE
    is_deleted_flg <> 'Y'
) AS mil2
WHERE
  mmt.inventory_item_id = msi.INVENTORY_ITEM_ID()
  AND mmt.organization_id = msi.ORGANIZATION_ID()
  AND mmt.transaction_id = mut.TRANSACTION_ID()
  AND mmt.inventory_item_id = mut.INVENTORY_ITEM_ID()
  AND mmt.organization_id = mut.ORGANIZATION_ID()
  AND mmt.transaction_id = mln.TRANSACTION_ID()
  AND mmt.inventory_item_id = mln.INVENTORY_ITEM_ID()
  AND mmt.organization_id = mln.ORGANIZATION_ID()
  AND mmt.locator_id = mil1.INVENTORY_LOCATION_ID()
  AND mmt.organization_id = mil1.ORGANIZATION_ID()
  AND mmt.transfer_locator_id = mil2.INVENTORY_LOCATION_ID()
  AND mmt.organization_id = mil2.ORGANIZATION_ID()
  AND (
    mmt.kca_seq_date > (
      SELECT
        (
          executebegints - prune_days
        )
      FROM bec_etl_ctrl.batch_dw_info
      WHERE
        dw_table_name = 'fact_material_transactions' AND batch_name = 'inv'
    )
  );
UPDATE bec_etl_ctrl.batch_dw_info SET load_type = 'I', last_refresh_date = CURRENT_TIMESTAMP()
WHERE
  dw_table_name = 'fact_material_transactions' AND batch_name = 'inv';