DELETE FROM gold_bec_dwh.FACT_INV_PHY_ADJUSTMENT
WHERE
  (COALESCE(ORGANIZATION_ID, 0), COALESCE(INVENTORY_ITEM_ID, 0), COALESCE(physical_inventory_name, 'NA'), COALESCE(subinv, 'NA'), COALESCE(locator, 'NA'), COALESCE(serial_number, 'NA'), COALESCE(lot_number, 'NA')) IN (
    SELECT
      COALESCE(ods.ORGANIZATION_ID, 0) AS ORGANIZATION_ID,
      COALESCE(ods.INVENTORY_ITEM_ID, 0) AS INVENTORY_ITEM_ID,
      COALESCE(ods.physical_inventory_name, 'NA') AS physical_inventory_name,
      COALESCE(ods.subinv, 'NA') AS subinv,
      COALESCE(ods.locator, 'NA') AS locator,
      COALESCE(ods.serial_number, 'NA') AS serial_number,
      COALESCE(ods.lot_number, 'NA') AS lot_number
    FROM gold_bec_dwh.FACT_INV_PHY_ADJUSTMENT AS dw, (
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
          FROM silver_bec_ods.cst_item_costs AS cic
          WHERE
            cic.cost_type_id IN (
              SELECT
                cct.cost_type_id
              FROM silver_bec_ods.cst_cost_types AS cct
              WHERE
                cct.cost_type = 'Frozen'
            )
            AND organization_id = msi.organization_id
            AND inventory_item_id = msi.inventory_item_id
        ) AS item_cost,
        on_hand.total_qoh * (
          SELECT
            cic.item_cost
          FROM silver_bec_ods.cst_item_costs AS cic
          WHERE
            cic.cost_type_id IN (
              SELECT
                cct.cost_type_id
              FROM silver_bec_ods.cst_cost_types AS cct
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
      FROM silver_bec_ods.mtl_system_items_b AS msi
      INNER JOIN silver_bec_ods.mtl_physical_adjustments AS mpa
        ON msi.inventory_item_id = mpa.inventory_item_id
        AND msi.organization_id = mpa.organization_id
      LEFT OUTER JOIN silver_bec_ods.mtl_item_locations AS loc
        ON mpa.locator_id = loc.inventory_location_id
        AND mpa.organization_id = loc.organization_id
      LEFT OUTER JOIN silver_bec_ods.mtl_physical_inventories AS mpi
        ON mpa.organization_id = mpi.organization_id
        AND mpa.physical_inventory_id = mpi.physical_inventory_id
      INNER JOIN silver_bec_ods.org_organization_definitions AS org
        ON mpa.organization_id = org.organization_id
      LEFT OUTER JOIN (
        SELECT
          SUM(total_qoh) AS total_qoh,
          locator_id,
          organization_id,
          inventory_item_id,
          subinventory_code,
          is_deleted_flg
        FROM silver_bec_ods.mtl_onhand_locator_v AS mtl_onhand_locator_v
        GROUP BY
          locator_id,
          organization_id,
          inventory_item_id,
          subinventory_code,
          is_deleted_flg
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
        AND (
          mpa.kca_seq_date > (
            SELECT
              (
                executebegints - prune_days
              )
            FROM bec_etl_ctrl.batch_dw_info
            WHERE
              dw_table_name = 'fact_inv_phy_adjustment' AND batch_name = 'inv'
          )
          OR msi.kca_seq_date > (
            SELECT
              (
                executebegints - prune_days
              )
            FROM bec_etl_ctrl.batch_dw_info
            WHERE
              dw_table_name = 'fact_inv_phy_adjustment' AND batch_name = 'inv'
          )
          OR mpi.kca_seq_date > (
            SELECT
              (
                executebegints - prune_days
              )
            FROM bec_etl_ctrl.batch_dw_info
            WHERE
              dw_table_name = 'fact_inv_phy_adjustment' AND batch_name = 'inv'
          )
          OR mpa.is_deleted_flg = 'Y'
          OR msi.is_deleted_flg = 'Y'
          OR loc.is_deleted_flg = 'Y'
          OR mpi.is_deleted_flg = 'Y'
          OR org.is_deleted_flg = 'Y'
          OR on_hand.is_deleted_flg = 'Y'
        )
      ORDER BY
        sort_by DESC NULLS FIRST,
        subinv ASC NULLS LAST,
        item ASC NULLS LAST,
        rev ASC NULLS LAST,
        locator ASC NULLS LAST,
        lot_number ASC NULLS LAST,
        serial_number ASC NULLS LAST
    ) AS ods
    WHERE
      dw.dw_load_id = (
        SELECT
          system_id
        FROM bec_etl_ctrl.etlsourceappid
        WHERE
          source_system = 'EBS'
      ) || '-' || COALESCE(ods.organization_id, 0) || '-' || COALESCE(ods.inventory_item_id, 0) || '-' || COALESCE(ods.physical_inventory_name, 'NA') || '-' || COALESCE(ods.subinv, 'NA') || '-' || COALESCE(ods.locator, 'NA') || '-' || COALESCE(ods.serial_number, 'NA') || '-' || COALESCE(ods.lot_number, 'NA')
  );
/* Insert Records */
INSERT INTO gold_bec_dwh.FACT_INV_PHY_ADJUSTMENT (
  physical_inventory_name,
  item,
  item_desc,
  rev,
  subinv,
  adjustment_date,
  `locator`,
  lot_number,
  serial_number,
  system_quantity,
  count_quantity,
  uom,
  adjustment_quantity,
  system_value,
  count_value,
  adjustment_value,
  outermost_lpn_id,
  parent_lpn_id,
  cost_group_id,
  approved_by_employee_id,
  inventory_item_id,
  organization_id,
  organization_name,
  sort_by,
  perpetual_qty,
  item_cost,
  perpetual_value,
  item_approval_status,
  Include_Rej_Items,
  is_deleted_flg,
  source_app_id,
  dw_load_id,
  dw_insert_date,
  dw_update_date
)
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
    AND (
      mpa.kca_seq_date > (
        SELECT
          (
            executebegints - prune_days
          )
        FROM bec_etl_ctrl.batch_dw_info
        WHERE
          dw_table_name = 'fact_inv_phy_adjustment' AND batch_name = 'inv'
      )
      OR msi.kca_seq_date > (
        SELECT
          (
            executebegints - prune_days
          )
        FROM bec_etl_ctrl.batch_dw_info
        WHERE
          dw_table_name = 'fact_inv_phy_adjustment' AND batch_name = 'inv'
      )
      OR mpi.kca_seq_date > (
        SELECT
          (
            executebegints - prune_days
          )
        FROM bec_etl_ctrl.batch_dw_info
        WHERE
          dw_table_name = 'fact_inv_phy_adjustment' AND batch_name = 'inv'
      )
    )
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