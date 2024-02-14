DROP table IF EXISTS gold_bec_dwh_rpt.FACT_INV_OHQ_DETAILS_RT;
CREATE TABLE gold_bec_dwh_rpt.FACT_INV_OHQ_DETAILS_RT AS
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
  moq1.inventory_item_id,
  moq1.locator_id,
  moq1.lot_number
FROM gold_bec_dwh.FACT_INV_OHQ_DETAILS AS moq1, gold_bec_dwh.dim_sub_inventories AS msi, silver_bec_ods.MTL_MATERIAL_STATUSES_TL AS msi_status, gold_bec_dwh.dim_item_locations AS loc
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
  moq1.inventory_item_id,
  moq1.locator_id,
  moq1.lot_number;
UPDATE bec_etl_ctrl.batch_dw_info SET last_refresh_date = CURRENT_TIMESTAMP()
WHERE
  dw_table_name = 'fact_inv_ohq_details_rt' AND batch_name = 'inv';