DROP table IF EXISTS gold_bec_dwh.FACT_INV_PHY_ADJUSTMENT;
CREATE TABLE gold_bec_dwh.FACT_INV_PHY_ADJUSTMENT AS
(
  SELECT
    mpi.physical_inventory_name,
    msi.segment1 AS item,
    msi.description AS item_desc,
    mpa.revision AS rev,
    mpa.subinventory_name AS subinv,
    mpa.creation_date AS adjustment_date,
    CASE
      WHEN loc.segment1 || '.' || loc.segment2 || '.' || loc.segment3 = '..'
      THEN NULL
      ELSE loc.segment1 || '.' || loc.segment2 || '.' || loc.segment3
    END AS locator,
    mpa.lot_number,
    mpa.serial_number,
    mpa.system_quantity,
    COALESCE(mpa.count_quantity, 0) AS count_quantity,
    msi.primary_uom_code AS uom,
    mpa.adjustment_quantity,
    COALESCE(mpa.system_quantity * mpa.actual_cost, 0) AS system_value,
    COALESCE(mpa.count_quantity * mpa.actual_cost, 0) AS count_value,
    COALESCE(mpa.adjustment_quantity * mpa.actual_cost, 0) AS adjustment_value,
    mpa.outermost_lpn_id,
    mpa.parent_lpn_id,
    mpa.cost_group_id,
    mpa.approved_by_employee_id,
    msi.inventory_item_id,
    mpa.organization_id,
    org.organization_name,
    COALESCE(mpa.count_quantity * mpa.actual_cost, 0) - COALESCE(system_quantity * mpa.actual_cost, 0) AS sort_by,
    on_hand.total_qoh AS perpetual_qty,
    (
      SELECT
        cic.item_cost
      FROM (
        SELECT
          *
        FROM silver_bec_ods.cst_item_costs
        WHERE
          is_deleted_flg <> 'Y'
      ) AS cic
      WHERE
        cic.cost_type_id IN (
          SELECT
            cct.cost_type_id
          FROM (
            SELECT
              *
            FROM silver_bec_ods.cst_cost_types
            WHERE
              is_deleted_flg <> 'Y'
          ) AS cct
          WHERE
            cct.cost_type = 'Frozen'
        )
        AND organization_id = msi.organization_id
        AND inventory_item_id = msi.inventory_item_id
    ) AS item_cost,
    on_hand.total_qoh * (
      SELECT
        cic.item_cost
      FROM (
        SELECT
          *
        FROM silver_bec_ods.cst_item_costs
        WHERE
          is_deleted_flg <> 'Y'
      ) AS cic
      WHERE
        cic.cost_type_id IN (
          SELECT
            cct.cost_type_id
          FROM (
            SELECT
              *
            FROM silver_bec_ods.cst_cost_types
            WHERE
              is_deleted_flg <> 'Y'
          ) AS cct
          WHERE
            cct.cost_type = 'Frozen'
        )
        AND organization_id = msi.organization_id
        AND inventory_item_id = msi.inventory_item_id
    ) AS perpetual_value,
    COALESCE(mpa.approval_status, 1) AS item_approval_status,
    CASE
      WHEN COALESCE(mpa.approval_status, 1) = 1
      THEN 'Non-Rejected'
      WHEN COALESCE(mpa.approval_status, 1) = 3
      THEN 'Non-Rejected'
      WHEN COALESCE(mpa.approval_status, 1) = 2
      THEN 'Rejected'
      ELSE NULL
    END AS Include_Rej_Items, /* Added for quickSight */
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
    ) || '-' || COALESCE(mpa.organization_id, 0) || '-' || COALESCE(msi.inventory_item_id, 0) || '-' || COALESCE(mpi.physical_inventory_name, 'NA') || '-' || COALESCE(mpa.subinventory_name, 'NA') || '-' || COALESCE(locator, 'NA') || '-' || COALESCE(mpa.serial_number, 'NA') || '-' || COALESCE(mpa.lot_number, 'NA') AS dw_load_id,
    CURRENT_TIMESTAMP() AS dw_insert_date,
    CURRENT_TIMESTAMP() AS dw_update_date
  FROM (
    SELECT
      *
    FROM silver_bec_ods.mtl_system_items_b
    WHERE
      is_deleted_flg <> 'Y'
  ) AS msi
  INNER JOIN (
    SELECT
      *
    FROM silver_bec_ods.mtl_physical_adjustments
    WHERE
      is_deleted_flg <> 'Y'
  ) AS mpa
    ON msi.inventory_item_id = mpa.inventory_item_id
    AND msi.organization_id = mpa.organization_id
  LEFT OUTER JOIN (
    SELECT
      *
    FROM silver_bec_ods.mtl_item_locations
    WHERE
      is_deleted_flg <> 'Y'
  ) AS loc
    ON mpa.locator_id = loc.inventory_location_id
    AND mpa.organization_id = loc.organization_id
  LEFT OUTER JOIN (
    SELECT
      *
    FROM silver_bec_ods.mtl_physical_inventories
    WHERE
      is_deleted_flg <> 'Y'
  ) AS mpi
    ON mpa.organization_id = mpi.organization_id
    AND mpa.physical_inventory_id = mpi.physical_inventory_id
  INNER JOIN (
    SELECT
      *
    FROM silver_bec_ods.org_organization_definitions
    WHERE
      is_deleted_flg <> 'Y'
  ) AS org
    ON mpa.organization_id = org.organization_id
  LEFT OUTER JOIN (
    SELECT
      SUM(total_qoh) AS total_qoh,
      locator_id,
      organization_id,
      inventory_item_id,
      subinventory_code
    FROM (
      SELECT
        *
      FROM silver_bec_ods.mtl_onhand_locator_v
      WHERE
        is_deleted_flg <> 'Y'
    ) AS mtl_onhand_locator_v
    GROUP BY
      locator_id,
      organization_id,
      inventory_item_id,
      subinventory_code
  ) AS on_hand
    ON (
      (
        on_hand.locator_id = loc.inventory_location_id OR on_hand.locator_id IS NULL
      )
      AND on_hand.organization_id = mpa.organization_id
      AND mpa.inventory_item_id = on_hand.inventory_item_id
      AND on_hand.SUBINVENTORY_CODE() = mpa.subinventory_name
    )
  WHERE
    mpa.adjustment_quantity <> 0
  ORDER BY
    sort_by DESC NULLS FIRST,
    subinv ASC NULLS LAST,
    item ASC NULLS LAST,
    rev ASC NULLS LAST,
    locator ASC NULLS LAST,
    lot_number ASC NULLS LAST,
    serial_number ASC NULLS LAST
);
UPDATE bec_etl_ctrl.batch_dw_info SET load_type = 'I', last_refresh_date = CURRENT_TIMESTAMP()
WHERE
  dw_table_name = 'fact_inv_phy_adjustment' AND batch_name = 'inv';