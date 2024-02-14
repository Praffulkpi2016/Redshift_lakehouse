/* Delete Records */
DELETE FROM gold_bec_dwh.DIM_INV_SAFTY_STOCK
WHERE
  (COALESCE(INVENTORY_ITEM_ID, 0), COALESCE(ORGANIZATION_ID, 0), COALESCE(EFFECTIVITY_DATE, '1900-01-01 12:00:00')) IN (
    SELECT
      COALESCE(ods.INVENTORY_ITEM_ID, 0) AS INVENTORY_ITEM_ID,
      COALESCE(ods.ORGANIZATION_ID, 0) AS ORGANIZATION_ID,
      COALESCE(ods.EFFECTIVITY_DATE, '1900-01-01 12:00:00') AS EFFECTIVITY_DATE
    FROM gold_bec_dwh.DIM_INV_SAFTY_STOCK AS dw, (
      SELECT
        organization_id,
        inventory_item_id,
        effectivity_date
      FROM silver_bec_ods.mtl_safety_stocks
      WHERE
        kca_seq_date > (
          SELECT
            (
              executebegints - prune_days
            )
          FROM bec_etl_ctrl.batch_dw_info
          WHERE
            dw_table_name = 'dim_inv_safty_stock' AND batch_name = 'inv'
        )
    ) AS ods
    WHERE
      1 = 1
      AND dw.dw_load_id = (
        SELECT
          system_id
        FROM bec_etl_ctrl.etlsourceappid
        WHERE
          source_system = 'EBS'
      ) || '-' || COALESCE(ods.INVENTORY_ITEM_ID, 0) || '-' || COALESCE(ods.ORGANIZATION_ID, 0) || '-' || COALESCE(ods.EFFECTIVITY_DATE, '1900-01-01 12:00:00')
  );
/* Insert Records */
INSERT INTO gold_bec_dwh.DIM_INV_SAFTY_STOCK (
  organization_id,
  inventory_item_id,
  safety_stock_quantity,
  effectivity_date,
  safety_stock_code,
  is_deleted_flg,
  source_app_id,
  dw_load_id,
  dw_insert_date,
  dw_update_date
)
(
  SELECT
    organization_id,
    inventory_item_id, /* ,plan_id */
    safety_stock_quantity,
    effectivity_date,
    safety_stock_code,
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
    ) || '-' || COALESCE(INVENTORY_ITEM_ID, 0) || '-' || COALESCE(ORGANIZATION_ID, 0) || '-' || COALESCE(EFFECTIVITY_DATE, '1900-01-01 12:00:00') AS dw_load_id,
    CURRENT_TIMESTAMP() AS dw_insert_date,
    CURRENT_TIMESTAMP() AS dw_update_date
  FROM silver_bec_ods.mtl_safety_stocks
  WHERE
    kca_seq_date > (
      SELECT
        (
          executebegints - prune_days
        )
      FROM bec_etl_ctrl.batch_dw_info
      WHERE
        dw_table_name = 'dim_inv_safty_stock' AND batch_name = 'inv'
    )
);
/* Soft Delete */
UPDATE gold_bec_dwh.DIM_INV_SAFTY_STOCK SET is_deleted_flg = 'Y'
WHERE
  NOT (COALESCE(INVENTORY_ITEM_ID, 0), COALESCE(ORGANIZATION_ID, 0), COALESCE(EFFECTIVITY_DATE, '1900-01-01 12:00:00')) IN (
    SELECT
      COALESCE(ods.INVENTORY_ITEM_ID, 0) AS INVENTORY_ITEM_ID,
      COALESCE(ods.ORGANIZATION_ID, 0) AS ORGANIZATION_ID,
      COALESCE(ods.EFFECTIVITY_DATE, '1900-01-01 12:00:00') AS EFFECTIVITY_DATE
    FROM gold_bec_dwh.DIM_INV_SAFTY_STOCK AS dw, (
      SELECT
        organization_id,
        inventory_item_id,
        effectivity_date
      FROM (
        SELECT
          *
        FROM silver_bec_ods.mtl_safety_stocks
        WHERE
          is_deleted_flg <> 'Y'
      )
    ) AS ods
    WHERE
      1 = 1
      AND dw.dw_load_id = (
        SELECT
          system_id
        FROM bec_etl_ctrl.etlsourceappid
        WHERE
          source_system = 'EBS'
      ) || '-' || COALESCE(ods.INVENTORY_ITEM_ID, 0) || '-' || COALESCE(ods.ORGANIZATION_ID, 0) || '-' || COALESCE(ods.EFFECTIVITY_DATE, '1900-01-01 12:00:00')
  );
UPDATE bec_etl_ctrl.batch_dw_info SET load_type = 'I', last_refresh_date = CURRENT_TIMESTAMP()
WHERE
  dw_table_name = 'dim_inv_safty_stock' AND batch_name = 'inv';