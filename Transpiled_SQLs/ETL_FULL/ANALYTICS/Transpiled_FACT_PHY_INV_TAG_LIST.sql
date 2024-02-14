DROP table IF EXISTS gold_bec_dwh.FACT_PHY_INV_TAG_LIST;
CREATE TABLE gold_bec_dwh.FACT_PHY_INV_TAG_LIST AS
SELECT DISTINCT
  mpi.physical_inventory_name,
  pit.tag_number,
  pit.revision,
  pit.lot_number,
  pit.serial_num AS serial_number,
  pit.subinventory,
  pit.tag_quantity AS quantity,
  pit.tag_uom AS uom,
  pit.inventory_item_id,
  pit.organization_id,
  CASE WHEN pit.void_flag = 1 THEN 'Void' WHEN pit.void_flag = 2 THEN 'Active' END AS void_flag,
  pit.locator_id,
  pit.counted_by_employee_id,
  pit.TAG_ID,
  (
    SELECT
      system_id
    FROM bec_etl_ctrl.etlsourceappid
    WHERE
      source_system = 'EBS'
  ) || '-' || pit.locator_id AS locator_id_KEY,
  (
    SELECT
      system_id
    FROM bec_etl_ctrl.etlsourceappid
    WHERE
      source_system = 'EBS'
  ) || '-' || pit.organization_id AS organization_id_KEY,
  (
    SELECT
      system_id
    FROM bec_etl_ctrl.etlsourceappid
    WHERE
      source_system = 'EBS'
  ) || '-' || pit.inventory_item_id AS inventory_item_id_KEY,
  (
    SELECT
      system_id
    FROM bec_etl_ctrl.etlsourceappid
    WHERE
      source_system = 'EBS'
  ) || '-' || pit.TAG_ID AS TAG_ID_KEY,
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
  ) || '-' || COALESCE(TAG_ID, 0) AS dw_load_id,
  CURRENT_TIMESTAMP() AS dw_insert_date,
  CURRENT_TIMESTAMP() AS dw_update_date
FROM (
  SELECT
    *
  FROM silver_bec_ods.mtl_physical_inventory_tags
  WHERE
    is_deleted_flg <> 'Y'
) AS pit, (
  SELECT
    *
  FROM silver_bec_ods.mtl_physical_inventories
  WHERE
    is_deleted_flg <> 'Y'
) AS mpi
WHERE
  1 = 1 AND pit.physical_inventory_id = mpi.PHYSICAL_INVENTORY_ID()
ORDER BY
  mpi.physical_inventory_name NULLS LAST,
  pit.tag_number NULLS LAST;
UPDATE bec_etl_ctrl.batch_dw_info SET load_type = 'I', last_refresh_date = CURRENT_TIMESTAMP()
WHERE
  dw_table_name = 'fact_phy_inv_tag_list' AND batch_name = 'inv';