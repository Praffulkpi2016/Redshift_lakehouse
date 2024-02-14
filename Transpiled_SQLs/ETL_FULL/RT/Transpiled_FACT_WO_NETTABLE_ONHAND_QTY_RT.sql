/* TEMPORARY Table */
DROP table IF EXISTS gold_bec_dwh.wo_temp;
CREATE TABLE gold_bec_dwh.wo_temp AS
SELECT
  *
FROM (
  WITH on_hand_qty AS (
    SELECT
      SUM(quantity) AS quantity,
      part_number,
      organization_id,
      subinventory,
      locator_id,
      serial_number,
      lot_number
    FROM gold_bec_dwh.fact_inv_onhand AS moq1
    WHERE
      vmi_flag <> 'Y'
    GROUP BY
      part_number,
      organization_id,
      subinventory,
      locator_id,
      serial_number,
      lot_number
  )
  SELECT
    SUM(moq1.quantity) AS on_hand_qty,
    moq1.serial_number,
    msi.secondary_inventory_name AS subinventory,
    loc.segment1 || '.' || loc.segment2 || '.' || loc.segment3 AS locator,
    msi_status.status_code,
    CASE
      WHEN SUBSTRING((
        msi_status.status_code
      ), LENGTH(msi_status.status_code), 1) = 'e'
      THEN 'Y'
      ELSE SUBSTRING((
        msi_status.status_code
      ), LENGTH(msi_status.status_code), 1)
    END AS nettable,
    moq1.part_number,
    moq1.organization_id,
    moq1.locator_id,
    moq1.lot_number
  FROM on_hand_qty AS moq1, gold_bec_dwh.dim_sub_inventories AS msi, silver_bec_ods.MTL_MATERIAL_STATUSES_TL AS msi_status, gold_bec_dwh.dim_item_locations AS loc
  WHERE
    1 = 1
    AND moq1.subinventory = msi.SECONDARY_INVENTORY_NAME()
    AND moq1.organization_id = msi.ORGANIZATION_ID()
    AND msi.status_id = msi_status.STATUS_ID()
    AND moq1.locator_id = loc.INVENTORY_LOCATION_ID()
  GROUP BY
    moq1.serial_number,
    msi.secondary_inventory_name,
    loc.segment1 || '.' || loc.segment2 || '.' || loc.segment3,
    msi_status.status_code,
    CASE
      WHEN SUBSTRING((
        msi_status.status_code
      ), LENGTH(msi_status.status_code), 1) = 'e'
      THEN 'Y'
      ELSE SUBSTRING((
        msi_status.status_code
      ), LENGTH(msi_status.status_code), 1)
    END,
    moq1.part_number,
    moq1.organization_id,
    moq1.locator_id,
    moq1.lot_number
);
/* Main Table */
DROP table IF EXISTS gold_bec_dwh_rpt.FACT_WO_NETTABLE_ONHAND_QTY_RT;
CREATE TABLE gold_bec_dwh_rpt.FACT_WO_NETTABLE_ONHAND_QTY_RT AS
SELECT
  wo.job_no AS job_no,
  wo.WIP_ENTITY_ID,
  wo.creation_date,
  wo.description,
  wo.primary_part_no,
  wo.primary_part_description,
  wo.job_type,
  wo.class_code,
  wo.scheduled_start_date,
  wo.date_released,
  wo.scheduled_completion_date,
  wo.date_completed,
  wo.start_quantity,
  wo.QUANTITY_REMAINING,
  wo.QUANTITY_COMPLETED,
  wo.QUANTITY_SCRAPPED,
  wo.qty_in_queue,
  wo.qty_running,
  wo.qty_waiting_to_move,
  wo.qty_rejected,
  wo.net_quantity,
  wo.bom_revision,
  wo.schedule_group_name,
  wo.project_no,
  wo.task_no,
  wo.job_status_type,
  wo.supply_type AS wip_supply_type_disp,
  wo.organization_id,
  wo.organization_code,
  wo.ORGANIZATION_NAME,
  wo.component AS part_no,
  wo.comp_description,
  wo.part_inventory_item_id,
  wo.operation_seq_num,
  wo.item_primary_uom_code,
  wo.date_required,
  wo.required_quantity,
  wo.quantity_issued,
  CASE
    WHEN (
      wo.required_quantity - wo.quantity_issued
    ) = 0
    THEN NULL
    ELSE CASE
      WHEN SIGN(wo.required_quantity) = -1 * SIGN(wo.quantity_issued)
      OR (
        SIGN(wo.required_quantity) IS NULL AND -1 * SIGN(wo.quantity_issued) IS NULL
      )
      THEN (
        wo.required_quantity - wo.quantity_issued
      )
      ELSE CASE
        WHEN SIGN(ABS(wo.required_quantity) - ABS(wo.quantity_issued)) = -1
        OR (
          SIGN(ABS(wo.required_quantity) - ABS(wo.quantity_issued)) IS NULL AND -1 IS NULL
        )
        THEN NULL
        ELSE (
          wo.required_quantity - wo.quantity_issued
        )
      END
    END
  END AS quantity_open,
  wo.quantity_per_assembly,
  temp.on_hand_qty,
  temp.serial_number,
  temp.subinventory,
  temp.locator,
  temp.locator_id,
  temp.lot_number,
  temp.status_code,
  temp.nettable,
  wo.item_cost,
  (
    SELECT
      system_id
    FROM bec_etl_ctrl.etlsourceappid
    WHERE
      source_system = 'EBS'
  ) || '-' || wo.dw_load_id || '-' || COALESCE(temp.subinventory, 'NA') || '-' || COALESCE(temp.locator, 'NA') || '-' || COALESCE(temp.serial_number, 'NA') || '-' || COALESCE(temp.lot_number, 'NA') AS dw_load_id,
  'Y' AS ACTIVE_FLG
FROM gold_bec_dwh.fact_wo_requirments AS wo, gold_bec_dwh.wo_temp AS temp
WHERE
  1 = 1
  AND COALESCE(wo.date_completed, CURRENT_TIMESTAMP()) >= '2022-01-01 00:00:00.000'
  AND wo.component = temp.PART_NUMBER()
  AND wo.organization_id = temp.ORGANIZATION_ID();
UPDATE bec_etl_ctrl.batch_dw_info SET load_type = 'I', last_refresh_date = CURRENT_TIMESTAMP()
WHERE
  dw_table_name = 'fact_wo_nettable_onhand_qty_rt' AND batch_name = 'wip';