TRUNCATE table gold_bec_dwh.FACT_INV_INTRANS_SHIPMENTS_TMP;
/* Insert records into temp table */
INSERT INTO gold_bec_dwh.FACT_INV_INTRANS_SHIPMENTS_TMP
(
  SELECT DISTINCT
    mmt1.transaction_id,
    mmt1.inventory_item_id,
    mmt1.organization_id
  FROM silver_bec_ods.mtl_material_transactions AS mmt1, silver_bec_ods.mtl_material_transactions AS mtl3, silver_bec_ods.cst_item_costs AS cic
  WHERE
    1 = 1
    AND mmt1.INVENTORY_ITEM_ID = cic.INVENTORY_ITEM_ID
    AND mmt1.ORGANIZATION_ID = cic.ORGANIZATION_ID
    AND cic.COST_TYPE_ID = 1
    AND mtl3.TRANSFER_TRANSACTION_ID() = mmt1.transaction_id
    AND (
      mmt1.kca_seq_date > (
        SELECT
          (
            executebegints - prune_days
          )
        FROM bec_etl_ctrl.batch_dw_info
        WHERE
          dw_table_name = 'fact_inv_intrans_shipments' AND batch_name = 'inv'
      )
      OR cic.kca_seq_date > (
        SELECT
          (
            executebegints - prune_days
          )
        FROM bec_etl_ctrl.batch_dw_info
        WHERE
          dw_table_name = 'fact_inv_intrans_shipments' AND batch_name = 'inv'
      )
    )
);
/* delete records from fact table */
DELETE FROM gold_bec_dwh.FACT_INV_INTRANS_SHIPMENTS
WHERE
  EXISTS(
    SELECT
      1
    FROM gold_bec_dwh.FACT_INV_INTRANS_SHIPMENTS_TMP AS tmp
    WHERE
      tmp.transaction_id = FACT_INV_INTRANS_SHIPMENTS.transaction_id
      AND tmp.inventory_item_id = FACT_INV_INTRANS_SHIPMENTS.inventory_item_id
      AND tmp.organization_id = FACT_INV_INTRANS_SHIPMENTS.organization_id
  );
/* insert records into fact table */
INSERT INTO gold_bec_dwh.FACT_INV_INTRANS_SHIPMENTS
(
  SELECT
    transaction_id,
    inventory_item_id,
    transaction_type_id,
    transaction_source_type_id,
    `Transaction Type`,
    `Transaction Source`,
    `Shipment Number`,
    `Transaction Date`,
    `Shipped Quantity`,
    TOTAL_RCV,
    organization_id,
    transfer_organization_id,
    ITEM_COST,
    ext_cost,
    (
      SELECT
        system_id
      FROM bec_etl_ctrl.etlsourceappid
      WHERE
        source_system = 'EBS'
    ) || '-' || inventory_item_id AS inventory_item_id_key,
    (
      SELECT
        system_id
      FROM bec_etl_ctrl.etlsourceappid
      WHERE
        source_system = 'EBS'
    ) || '-' || transaction_id AS transaction_id_key,
    (
      SELECT
        system_id
      FROM bec_etl_ctrl.etlsourceappid
      WHERE
        source_system = 'EBS'
    ) || '-' || transaction_type_id AS transaction_type_id_key,
    (
      SELECT
        system_id
      FROM bec_etl_ctrl.etlsourceappid
      WHERE
        source_system = 'EBS'
    ) || '-' || transaction_source_type_id AS transaction_source_type_id_key,
    (
      SELECT
        system_id
      FROM bec_etl_ctrl.etlsourceappid
      WHERE
        source_system = 'EBS'
    ) || '-' || organization_id AS organization_id_key,
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
    ) || '-' || COALESCE(transaction_id, 0) AS dw_load_id,
    CURRENT_TIMESTAMP() AS dw_insert_date,
    CURRENT_TIMESTAMP() AS dw_update_date
  FROM (
    SELECT
      mmt1.transaction_id,
      mmt1.inventory_item_id,
      mmt1.transaction_type_id,
      mmt1.transaction_source_type_id,
      CASE WHEN mmt1.transaction_type_id = '21' THEN 'Intransit Shipment' END AS `Transaction Type`,
      CASE WHEN mmt1.transaction_source_type_id = '13' THEN 'Inventory' END AS `Transaction Source`,
      mmt1.shipment_number AS `Shipment Number`,
      mmt1.transaction_date AS `Transaction Date`,
      mmt1.transaction_quantity * -1 AS `Shipped Quantity`,
      0 AS TOTAL_RCV,
      mmt1.organization_id,
      mmt1.transfer_organization_id,
      cic.ITEM_COST,
      mmt1.transaction_quantity * -1 * cic.ITEM_COST AS ext_cost
    FROM (
      SELECT
        *
      FROM silver_bec_ods.mtl_material_transactions
      WHERE
        is_deleted_flg <> 'y'
    ) AS mmt1, (
      SELECT
        *
      FROM silver_bec_ods.mtl_material_transactions
      WHERE
        is_deleted_flg <> 'y'
    ) AS mtl3, (
      SELECT
        *
      FROM silver_bec_ods.cst_item_costs
      WHERE
        is_deleted_flg <> 'y'
    ) AS cic, gold_bec_dwh.FACT_INV_INTRANS_SHIPMENTS_TMP AS tmp
    WHERE
      mmt1.transaction_type_id = 21
      AND mmt1.transaction_source_type_id = 13
      AND mmt1.INVENTORY_ITEM_ID = cic.INVENTORY_ITEM_ID
      AND mmt1.ORGANIZATION_ID = cic.ORGANIZATION_ID
      AND cic.COST_TYPE_ID = 1
      AND mtl3.TRANSFER_TRANSACTION_ID() = mmt1.transaction_id
      AND NOT EXISTS(
        SELECT
          1
        FROM (
          SELECT
            *
          FROM silver_bec_ods.mtl_material_transactions
          WHERE
            is_deleted_flg <> 'Y'
        ) AS mmt2
        WHERE
          mmt2.transfer_transaction_id = mmt1.transaction_id
          AND mmt2.shipment_number = mmt1.shipment_number
          AND mmt2.transaction_type_id = 12
          AND mmt2.transaction_source_type_id = 13
      )
      AND mmt1.transaction_id = tmp.transaction_id
      AND mmt1.inventory_item_id = tmp.inventory_item_id
      AND mmt1.organization_id = tmp.organization_id
    GROUP BY
      mmt1.transaction_id,
      mmt1.inventory_item_id,
      mmt1.transaction_type_id,
      mmt1.transaction_source_type_id,
      mmt1.transaction_type_id,
      mmt1.transaction_source_type_id,
      mmt1.shipment_number,
      mmt1.transaction_date,
      mmt1.transaction_quantity * -1,
      mmt1.organization_id,
      mmt1.transfer_organization_id,
      cic.item_cost
    UNION
    SELECT
      mmt1.transaction_id,
      mmt1.inventory_item_id,
      mmt1.transaction_type_id,
      mmt1.transaction_source_type_id,
      CASE WHEN mmt1.transaction_type_id = '21' THEN 'Intransit Shipment' END AS `Transaction Type`,
      CASE WHEN mmt1.transaction_source_type_id = '13' THEN 'Inventory' END AS `Transaction Source`,
      mmt1.shipment_number AS `Shipment Number`,
      mmt1.transaction_date AS `Transaction Date`,
      mmt1.transaction_quantity * -1 AS `Shipped Quantity`,
      SUM(mtl3.transaction_quantity) AS TOTAL_RCV,
      mmt1.organization_id,
      mmt1.transfer_organization_id,
      cic.ITEM_COST,
      mmt1.transaction_quantity * -1 * cic.ITEM_COST AS ext_cost
    FROM (
      SELECT
        *
      FROM silver_bec_ods.mtl_material_transactions
      WHERE
        is_deleted_flg <> 'Y'
    ) AS mmt1, (
      SELECT
        transfer_transaction_id,
        transaction_quantity
      FROM silver_bec_ods.mtl_material_transactions
      WHERE
        is_deleted_flg <> 'Y'
    ) AS mtl3, (
      SELECT
        *
      FROM silver_bec_ods.cst_item_costs
      WHERE
        is_deleted_flg <> 'Y'
    ) AS cic, gold_bec_dwh.FACT_INV_INTRANS_SHIPMENTS_TMP AS tmp
    WHERE
      mmt1.transaction_type_id = 21
      AND mmt1.transaction_source_type_id = 13
      AND mmt1.INVENTORY_ITEM_ID = cic.INVENTORY_ITEM_ID
      AND mmt1.ORGANIZATION_ID = cic.ORGANIZATION_ID
      AND cic.COST_TYPE_ID = 1
      AND mtl3.transfer_transaction_id = mmt1.transaction_id
      AND mmt1.transaction_quantity * -1 > (
        SELECT
          SUM(transaction_quantity)
        FROM (
          SELECT
            *
          FROM silver_bec_ods.mtl_material_transactions
          WHERE
            is_deleted_flg <> 'Y'
        ) AS mmt2
        WHERE
          mmt2.transfer_transaction_id = mmt1.transaction_id
          AND mmt2.shipment_number = mmt1.shipment_number
          AND mmt2.transaction_type_id = 12
          AND mmt2.transaction_source_type_id = 13
        GROUP BY
          mmt2.transfer_transaction_id
      )
      AND mmt1.transaction_id = tmp.transaction_id
      AND mmt1.inventory_item_id = tmp.inventory_item_id
      AND mmt1.organization_id = tmp.organization_id
    GROUP BY
      mmt1.transaction_id,
      mmt1.inventory_item_id,
      mmt1.transaction_type_id,
      mmt1.transaction_source_type_id,
      mmt1.transaction_type_id,
      mmt1.transaction_source_type_id,
      mmt1.shipment_number,
      mmt1.transaction_date,
      mmt1.transaction_quantity * -1,
      mmt1.organization_id,
      mmt1.transfer_organization_id,
      cic.item_cost
  )
);
UPDATE bec_etl_ctrl.batch_dw_info SET load_type = 'I', last_refresh_date = CURRENT_TIMESTAMP()
WHERE
  dw_table_name = 'fact_inv_intrans_shipments' AND batch_name = 'inv';