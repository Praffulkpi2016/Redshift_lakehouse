/* Delete Records */
DELETE FROM gold_bec_dwh.FACT_PHY_INV_TAG_LIST
WHERE
  (
    COALESCE(TAG_ID, 0)
  ) IN (
    SELECT
      COALESCE(ods.TAG_ID, 0) AS TAG_ID
    FROM gold_bec_dwh.FACT_PHY_INV_TAG_LIST AS dw, (
      SELECT
        pit.TAG_ID
      FROM silver_bec_ods.mtl_physical_inventory_tags AS pit, silver_bec_ods.mtl_physical_inventories AS mpi
      WHERE
        1 = 1
        AND pit.physical_inventory_id = mpi.PHYSICAL_INVENTORY_ID()
        AND (
          pit.kca_seq_date > (
            SELECT
              (
                executebegints - prune_days
              )
            FROM bec_etl_ctrl.batch_dw_info
            WHERE
              dw_table_name = 'fact_phy_inv_tag_list' AND batch_name = 'inv'
          )
          OR pit.is_deleted_flg = 'Y'
          OR mpi.is_deleted_flg = 'Y'
        )
    ) AS ods
    WHERE
      dw.dw_load_id = (
        SELECT
          system_id
        FROM bec_etl_ctrl.etlsourceappid
        WHERE
          source_system = 'EBS'
      ) || '-' || COALESCE(ods.TAG_ID, 0)
  );
/* Insert records */
INSERT INTO gold_bec_dwh.FACT_PHY_INV_TAG_LIST (
  physical_inventory_name,
  tag_number,
  revision,
  lot_number,
  serial_number,
  subinventory,
  quantity,
  uom,
  inventory_item_id,
  organization_id,
  void_flag,
  locator_id,
  counted_by_employee_id,
  TAG_ID,
  locator_id_KEY,
  organization_id_KEY,
  inventory_item_id_KEY,
  TAG_ID_KEY,
  is_deleted_flg,
  source_app_id,
  dw_load_id,
  dw_insert_date,
  dw_update_date
)
(
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
    1 = 1
    AND pit.physical_inventory_id = mpi.PHYSICAL_INVENTORY_ID()
    AND pit.kca_seq_date > (
      SELECT
        (
          executebegints - prune_days
        )
      FROM bec_etl_ctrl.batch_dw_info
      WHERE
        dw_table_name = 'fact_phy_inv_tag_list' AND batch_name = 'inv'
    )
  ORDER BY
    mpi.physical_inventory_name NULLS LAST,
    pit.tag_number NULLS LAST
);
UPDATE bec_etl_ctrl.batch_dw_info SET load_type = 'I', last_refresh_date = CURRENT_TIMESTAMP()
WHERE
  dw_table_name = 'fact_phy_inv_tag_list' AND batch_name = 'inv';