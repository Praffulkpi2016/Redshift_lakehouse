DROP table IF EXISTS gold_bec_dwh.FACT_INV_OHQ_DETAILS;
CREATE TABLE gold_bec_dwh.FACT_INV_OHQ_DETAILS AS
SELECT
  part_number,
  description,
  primary_unit_of_measure,
  subinventory,
  locator_id,
  serial_number,
  lot_number,
  QUANTITY,
  organization_id,
  inventory_item_id,
  sub_system,
  sub_class,
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
  ) || '-' || COALESCE(inventory_item_id, 0) || '-' || COALESCE(organization_id, 0) || '-' || COALESCE(subinventory, 'NA') || '-' || COALESCE(locator_id, 0) || '-' || COALESCE(serial_number, 'NA') || '-' || COALESCE(lot_number, 'NA') AS dw_load_id,
  CURRENT_TIMESTAMP() AS dw_insert_date,
  CURRENT_TIMESTAMP() AS dw_update_date
FROM (
  SELECT
    msi.segment1 AS part_number,
    msi.description,
    msi.primary_unit_of_measure,
    miq.subinventory_code AS subinventory,
    miq.locator_id,
    NULL AS serial_number,
    miq.lot_number,
    SUM(miq.primary_transaction_quantity) AS QUANTITY,
    msi.organization_id,
    msi.inventory_item_id,
    mc.SEGMENT1 AS sub_system,
    mc.SEGMENT2 AS sub_class
  FROM (
    SELECT
      *
    FROM silver_bec_ods.mtl_system_items_b
    WHERE
      is_deleted_flg <> 'Y'
  ) AS msi, (
    SELECT
      *
    FROM silver_bec_ods.mtl_onhand_quantities_detail
    WHERE
      is_deleted_flg <> 'Y'
  ) AS miq, (
    SELECT
      *
    FROM silver_bec_ods.mtl_categories_b
    WHERE
      is_deleted_flg <> 'Y'
  ) AS mc, (
    SELECT
      *
    FROM silver_bec_ods.mtl_item_categories
    WHERE
      is_deleted_flg <> 'Y'
  ) AS mic
  WHERE
    msi.inventory_item_id = miq.inventory_item_id
    AND miq.organization_id = msi.organization_id
    AND miq.organization_id = miq.owning_organization_id
    AND msi.serial_number_control_code = 1
    AND miq.organization_id = mic.ORGANIZATION_ID()
    AND miq.INVENTORY_ITEM_ID = mic.INVENTORY_ITEM_ID()
    AND mic.CATEGORY_SET_ID() = 1
    AND mic.CATEGORY_ID = mc.CATEGORY_ID()
  GROUP BY
    msi.segment1,
    msi.description,
    msi.primary_unit_of_measure,
    miq.subinventory_code,
    miq.locator_id,
    miq.lot_number,
    msi.organization_id,
    msi.inventory_item_id,
    mc.SEGMENT1,
    mc.SEGMENT2
  UNION ALL
  SELECT DISTINCT
    msi.segment1 AS part_number,
    msi.description,
    msi.primary_unit_of_measure,
    msn.current_subinventory_code AS subinventory,
    msn.current_locator_id AS locator_id,
    msn.serial_number,
    msn.lot_number,
    1 AS quantity,
    msi.organization_id,
    msi.inventory_item_id,
    mc.SEGMENT1 AS sub_system,
    mc.SEGMENT2 AS sub_class
  FROM (
    SELECT
      *
    FROM silver_bec_ods.mtl_system_items_b
    WHERE
      is_deleted_flg <> 'Y'
  ) AS msi, (
    SELECT
      *
    FROM silver_bec_ods.mtl_serial_numbers
    WHERE
      is_deleted_flg <> 'Y'
  ) AS msn, (
    SELECT
      *
    FROM silver_bec_ods.mtl_categories_b
    WHERE
      is_deleted_flg <> 'Y'
  ) AS mc, (
    SELECT
      *
    FROM silver_bec_ods.mtl_item_categories
    WHERE
      is_deleted_flg <> 'Y'
  ) AS mic
  WHERE
    msi.inventory_item_id = msn.inventory_item_id
    AND msn.current_organization_id = msi.organization_id
    AND msn.current_organization_id = msn.owning_organization_id
    AND msn.current_status = 3
    AND msi.serial_number_control_code <> 1
    AND msn.CURRENT_ORGANIZATION_ID = mic.ORGANIZATION_ID()
    AND msn.INVENTORY_ITEM_ID = mic.INVENTORY_ITEM_ID()
    AND mic.CATEGORY_SET_ID() = 1
    AND mic.CATEGORY_ID = mc.CATEGORY_ID()
  ORDER BY
    subinventory NULLS LAST,
    part_number NULLS LAST,
    locator_id NULLS LAST,
    serial_number NULLS LAST,
    organization_id ASC NULLS LAST
);
UPDATE bec_etl_ctrl.batch_dw_info SET load_type = 'I', last_refresh_date = CURRENT_TIMESTAMP()
WHERE
  dw_table_name = 'fact_inv_ohq_details' AND batch_name = 'inv';