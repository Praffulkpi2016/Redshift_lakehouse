DROP table IF EXISTS gold_bec_dwh.FACT_INV_AGING_DETAILS;
CREATE TABLE gold_bec_dwh.FACT_INV_AGING_DETAILS AS
SELECT
  organization_id,
  description,
  part_number,
  unit_cost,
  transaction_quantity,
  total_cost,
  inventory_item_id,
  date_in,
  subinventory_code,
  transaction_type_id,
  serial_number,
  current_status,
  current_subinventory_code,
  locator_id,
  (
    SELECT
      system_id
    FROM bec_etl_ctrl.etlsourceappid
    WHERE
      source_system = 'EBS'
  ) || '-' || locator_id AS locator_id_KEY,
  (
    SELECT
      system_id
    FROM bec_etl_ctrl.etlsourceappid
    WHERE
      source_system = 'EBS'
  ) || '-' || organization_id AS organization_id_KEY,
  (
    SELECT
      system_id
    FROM bec_etl_ctrl.etlsourceappid
    WHERE
      source_system = 'EBS'
  ) || '-' || inventory_item_id AS inventory_item_id_KEY,
  (
    SELECT
      system_id
    FROM bec_etl_ctrl.etlsourceappid
    WHERE
      source_system = 'EBS'
  ) || '-' || transaction_type_id AS transaction_type_id_KEY,
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
  ) || '-' || COALESCE(inventory_item_id, 0) || '-' || COALESCE(organization_id, 0) || '-' || COALESCE(subinventory_code, 'NA') || '-' || COALESCE(locator_id, 0) || '-' || COALESCE(serial_number, 'NA') || '-' || COALESCE(date_in, '1900-01-01 12:00:00') || '-' || COALESCE(transaction_quantity, 0) AS dw_load_id,
  CURRENT_TIMESTAMP() AS dw_insert_date,
  CURRENT_TIMESTAMP() AS dw_update_date
FROM (
  SELECT
    msn.current_organization_id AS organization_id,
    msi.description,
    msi.segment1 AS part_number,
    cic.item_cost AS unit_cost,
    1 AS transaction_quantity,
    1 * cic.item_cost AS total_cost,
    msn.inventory_item_id,
    msn.LAST_UPDATE_DATE AS date_in,
    msn.current_subinventory_code AS subinventory_code,
    NULL AS transaction_type_id,
    msn.serial_number,
    msn.current_status,
    msn.current_subinventory_code,
    msn.current_locator_id AS locator_id
  FROM (
    SELECT
      *
    FROM silver_bec_ods.mtl_serial_numbers
    WHERE
      is_deleted_flg <> 'Y'
  ) AS msn, (
    SELECT
      *
    FROM silver_bec_ods.mtl_system_items_b
    WHERE
      is_deleted_flg <> 'Y'
  ) AS msi, (
    SELECT
      *
    FROM silver_bec_ods.cst_item_costs
    WHERE
      is_deleted_flg <> 'Y'
  ) AS cic
  WHERE
    msn.current_status = 3
    AND msn.inventory_item_id = msi.inventory_item_id
    AND msn.current_organization_id = msi.organization_id
    AND msn.inventory_item_id = cic.inventory_item_id
    AND msn.current_organization_id = cic.organization_id
    AND cic.cost_type_id = 1
  UNION ALL
  SELECT
    moq.organization_id,
    msi.description,
    msi.segment1 AS part_number,
    cic.item_cost AS unit_cost,
    SUM(moq.transaction_quantity) AS transaction_quantity,
    SUM(moq.transaction_quantity) * cic.item_cost AS total_cost,
    moq.inventory_item_id,
    moq.date_received AS date_in,
    moq.subinventory_code,
    NULL AS transaction_type_id,
    NULL AS serial_number,
    NULL AS current_status,
    NULL AS current_subinventory_code,
    moq.locator_id
  FROM (
    SELECT
      *
    FROM silver_bec_ods.mtl_onhand_quantities_detail
    WHERE
      is_deleted_flg <> 'Y'
  ) AS moq, (
    SELECT
      *
    FROM silver_bec_ods.mtl_system_items_b
    WHERE
      is_deleted_flg <> 'Y'
  ) AS msi, (
    SELECT
      *
    FROM silver_bec_ods.cst_item_costs
    WHERE
      is_deleted_flg <> 'Y'
  ) AS cic, (
    SELECT
      *
    FROM silver_bec_ods.mtl_item_locations
    WHERE
      is_deleted_flg <> 'Y'
  ) AS mil
  WHERE
    1 = 1
    AND moq.organization_id = msi.organization_id
    AND moq.inventory_item_id = msi.inventory_item_id
    AND moq.organization_id = cic.organization_id
    AND moq.inventory_item_id = cic.inventory_item_id
    AND cic.cost_type_id = 1
    AND msi.serial_number_control_code = 1
    AND moq.organization_id = mil.ORGANIZATION_ID()
    AND moq.locator_id = mil.INVENTORY_LOCATION_ID()
  GROUP BY
    moq.organization_id,
    msi.description,
    msi.segment1,
    cic.item_cost,
    moq.inventory_item_id,
    moq.date_received,
    moq.subinventory_code,
    moq.locator_id
);
UPDATE bec_etl_ctrl.batch_dw_info SET load_type = 'I', last_refresh_date = CURRENT_TIMESTAMP()
WHERE
  dw_table_name = 'fact_inv_aging_details' AND batch_name = 'inv';