TRUNCATE table gold_bec_dwh.FACT_INV_VALUE_TMP;
INSERT INTO gold_bec_dwh.FACT_INV_VALUE_TMP
(
  SELECT DISTINCT
    msi.inventory_item_id,
    miq.organization_id
  FROM silver_bec_ods.mtl_system_items_b AS msi, silver_bec_ods.mtl_secondary_inventories AS sub, (
    SELECT
      organization_id,
      owning_organization_id,
      inventory_item_id,
      subinventory_code,
      locator_id,
      revision,
      SUM(primary_transaction_quantity) AS quantity
    FROM silver_bec_ods.mtl_onhand_quantities_detail
    WHERE
      1 = 1
      AND kca_seq_date > (
        SELECT
          (
            executebegints - prune_days
          )
        FROM bec_etl_ctrl.batch_dw_info
        WHERE
          dw_table_name = 'fact_inv_value' AND batch_name = 'inv'
      )
    GROUP BY
      organization_id,
      owning_organization_id,
      inventory_item_id,
      subinventory_code,
      locator_id,
      revision
  ) AS miq, silver_bec_ods.cst_item_costs AS ctc
  WHERE
    msi.inventory_item_id = miq.inventory_item_id
    AND miq.organization_id = msi.organization_id
    AND msi.inventory_item_id = ctc.inventory_item_id
    AND ctc.organization_id = msi.organization_id
    AND miq.subinventory_code = sub.secondary_inventory_name
    AND miq.organization_id = sub.organization_id
) /* AND ctc.cost_type_id = '1' */;
DELETE FROM gold_bec_dwh.FACT_INV_VALUE
WHERE
  EXISTS(
    SELECT
      1
    FROM gold_bec_dwh.FACT_INV_VALUE_TMP
    WHERE
      inventory_item_id = FACT_INV_VALUE.inventory_item_id
      AND organization_id = FACT_INV_VALUE.organization_id
  );
/* insert records */
INSERT INTO gold_bec_dwh.FACT_INV_VALUE
SELECT
  ctc.cost_type_id,
  miq.locator_id,
  msi.inventory_item_id,
  msi.buyer_id,
  miq.subinventory_code AS subinventory,
  msi.segment1 AS part_number,
  msi.description,
  msi.inventory_item_status_code,
  msi.primary_uom_code,
  miq.quantity,
  ctc.material_cost,
  ctc.material_overhead_cost,
  ctc.resource_cost,
  ctc.outside_processing_cost,
  ctc.overhead_cost,
  ctc.item_cost,
  miq.quantity * ctc.material_cost AS tot_mat_cost,
  miq.quantity * ctc.material_overhead_cost AS tot_mat_oh_cost,
  miq.quantity * ctc.resource_cost AS tot_resource_cost,
  miq.quantity * ctc.outside_processing_cost AS tot_osp_cost,
  miq.quantity * ctc.overhead_cost AS tot_oh_cost,
  (
    miq.quantity * ctc.item_cost
  ) AS extended_cost,
  miq.organization_id,
  CASE
    WHEN miq.owning_organization_id = miq.organization_id
    OR (
      miq.owning_organization_id IS NULL AND miq.organization_id IS NULL
    )
    THEN 'N'
    ELSE 'Y'
  END AS vmi_flag,
  miq.owning_organization_id,
  CASE
    WHEN sub.asset_inventory = 1
    THEN 'Asset'
    WHEN sub.asset_inventory = 2
    THEN 'Expense'
    ELSE CAST(asset_inventory AS CHAR)
  END AS subinventory_type,
  (
    SELECT
      system_id
    FROM bec_etl_ctrl.etlsourceappid
    WHERE
      source_system = 'EBS'
  ) || '-' || ctc.cost_type_id AS cost_type_id_KEY,
  (
    SELECT
      system_id
    FROM bec_etl_ctrl.etlsourceappid
    WHERE
      source_system = 'EBS'
  ) || '-' || miq.locator_id AS locator_id_KEY,
  (
    SELECT
      system_id
    FROM bec_etl_ctrl.etlsourceappid
    WHERE
      source_system = 'EBS'
  ) || '-' || msi.inventory_item_id AS inventory_item_id_KEY,
  (
    SELECT
      system_id
    FROM bec_etl_ctrl.etlsourceappid
    WHERE
      source_system = 'EBS'
  ) || '-' || msi.buyer_id AS buyer_id_KEY,
  (
    SELECT
      system_id
    FROM bec_etl_ctrl.etlsourceappid
    WHERE
      source_system = 'EBS'
  ) || '-' || miq.organization_id AS organization_id_KEY,
  (
    SELECT
      system_id
    FROM bec_etl_ctrl.etlsourceappid
    WHERE
      source_system = 'EBS'
  ) || '-' || miq.owning_organization_id AS owning_organization_id_KEY,
  msi.attribute5 AS program_name,
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
  ) || '-' || COALESCE(msi.inventory_item_id, 0) || '-' || COALESCE(miq.organization_id, 0) || '-' || COALESCE(subinventory, 'NA') || '-' || COALESCE(miq.locator_id, 0) || '-' || COALESCE(miq.owning_organization_id, 0) AS dw_load_id,
  CURRENT_TIMESTAMP() AS dw_insert_date,
  CURRENT_TIMESTAMP() AS dw_update_date
FROM (
  SELECT
    *
  FROM silver_bec_ods.mtl_system_items_b
  WHERE
    is_deleted_flg <> 'Y'
) AS msi, (
  SELECT
    *
  FROM silver_bec_ods.mtl_secondary_inventories
  WHERE
    is_deleted_flg <> 'Y'
) AS sub, (
  SELECT
    organization_id,
    owning_organization_id,
    inventory_item_id,
    subinventory_code,
    locator_id,
    revision,
    SUM(primary_transaction_quantity) AS quantity
  FROM silver_bec_ods.mtl_onhand_quantities_detail
  WHERE
    is_deleted_flg <> 'Y'
  GROUP BY
    organization_id,
    owning_organization_id,
    inventory_item_id,
    subinventory_code,
    locator_id,
    revision
) AS miq, (
  SELECT
    *
  FROM silver_bec_ods.cst_item_costs
  WHERE
    is_deleted_flg <> 'Y'
) AS ctc, gold_bec_dwh.FACT_INV_VALUE_TMP AS TMP
WHERE
  tmp.inventory_item_id = msi.inventory_item_id
  AND tmp.organization_id = miq.organization_id
  AND msi.inventory_item_id = miq.inventory_item_id
  AND miq.organization_id = msi.organization_id
  AND msi.inventory_item_id = ctc.inventory_item_id
  AND ctc.organization_id = msi.organization_id
  AND miq.subinventory_code = sub.secondary_inventory_name
  AND miq.organization_id = sub.organization_id
  AND ctc.cost_type_id = '1';
UPDATE bec_etl_ctrl.batch_dw_info SET load_type = 'I', last_refresh_date = CURRENT_TIMESTAMP()
WHERE
  dw_table_name = 'fact_inv_value' AND batch_name = 'inv';