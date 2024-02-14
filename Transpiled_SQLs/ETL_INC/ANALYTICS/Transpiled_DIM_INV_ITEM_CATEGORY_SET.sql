/* Delete Records */
DELETE FROM gold_bec_dwh.DIM_INV_ITEM_CATEGORY_SET
WHERE
  EXISTS(
    SELECT
      1
    FROM silver_bec_ods.MTL_ITEM_CATEGORIES AS MIC
    INNER JOIN silver_bec_ods.MTL_CATEGORY_SETS_TL AS CSET
      ON MIC.CATEGORY_SET_ID = CSET.CATEGORY_SET_ID AND CSET.LANGUAGE = 'US'
    INNER JOIN silver_bec_ods.MTL_SYSTEM_ITEMS_B AS MSI
      ON MIC.INVENTORY_ITEM_ID = MSI.INVENTORY_ITEM_ID
      AND MIC.ORGANIZATION_ID = MSI.ORGANIZATION_ID
    INNER JOIN silver_bec_ods.MTL_CATEGORIES_B AS MC
      ON MIC.CATEGORY_ID = MC.CATEGORY_ID
    WHERE
      COALESCE(MIC.INVENTORY_ITEM_ID, 0) = COALESCE(dim_inv_item_category_set.INVENTORY_ITEM_ID, 0)
      AND COALESCE(MIC.ORGANIZATION_ID, 0) = COALESCE(dim_inv_item_category_set.ORGANIZATION_ID, 0)
      AND COALESCE(MIC.CATEGORY_ID, 0) = COALESCE(dim_inv_item_category_set.CATEGORY_ID, 0)
      AND COALESCE(MIC.CATEGORY_SET_ID, 0) = COALESCE(dim_inv_item_category_set.CATEGORY_SET_ID, 0)
      AND (
        MIC.kca_seq_date > (
          SELECT
            (
              executebegints - prune_days
            )
          FROM bec_etl_ctrl.batch_dw_info
          WHERE
            dw_table_name = 'dim_inv_item_category_set' AND batch_name = 'ap'
        )
        OR MC.kca_seq_date > (
          SELECT
            (
              executebegints - prune_days
            )
          FROM bec_etl_ctrl.batch_dw_info
          WHERE
            dw_table_name = 'dim_inv_item_category_set' AND batch_name = 'ap'
        )
      )
  );
/* Insert records */
INSERT INTO gold_bec_dwh.DIM_INV_ITEM_CATEGORY_SET (
  INVENTORY_ITEM_ID,
  ORGANIZATION_ID,
  CATEGORY_SET_ID,
  STRUCTURE_ID,
  CATEGORY_ID,
  ITEM_NAME,
  ITEM_DESCRIPTION,
  CATEGORY_SET_NAME,
  CATEGORY_SET_DESC,
  ITEM_CATEGORY_SEGMENT1,
  ITEM_CATEGORY_SEGMENT2,
  ITEM_CATEGORY_SEGMENT3,
  ITEM_CATEGORY_SEGMENT4,
  ITEM_CATEGORY_DESC,
  is_deleted_flg,
  SOURCE_APP_ID,
  DW_LOAD_ID,
  DW_INSERT_DATE,
  DW_UPDATE_DATE
)
(
  SELECT
    COALESCE(MIC.INVENTORY_ITEM_ID, 0) AS INVENTORY_ITEM_ID,
    COALESCE(MIC.ORGANIZATION_ID, 0) AS ORGANIZATION_ID,
    COALESCE(MIC.CATEGORY_SET_ID, 0) AS CATEGORY_SET_ID,
    COALESCE(MC.STRUCTURE_ID, 0) AS STRUCTURE_ID,
    COALESCE(MIC.CATEGORY_ID, 0) AS CATEGORY_ID,
    MSI.SEGMENT1 AS `ITEM_NAME`,
    MSI.DESCRIPTION AS `ITEM_DESCRIPTION`,
    CSET.CATEGORY_SET_NAME,
    CSET.DESCRIPTION AS `CATEGORY_SET_DESC`,
    MC.SEGMENT1 AS `ITEM_CATEGORY_SEGMENT1`,
    COALESCE(MC.SEGMENT2, 'LEVEL2') AS `ITEM_CATEGORY_SEGMENT2`,
    COALESCE(MC.SEGMENT3, 'LEVEL3') AS `ITEM_CATEGORY_SEGMENT3`,
    COALESCE(MC.SEGMENT4, 'LEVEL4') AS `ITEM_CATEGORY_SEGMENT4`,
    MC.DESCRIPTION AS `ITEM_CATEGORY_DESC`,
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
    ) || '-' || COALESCE(MIC.INVENTORY_ITEM_ID, 0) || '-' || COALESCE(MIC.ORGANIZATION_ID, 0) || '-' || COALESCE(MC.CATEGORY_ID, 0) || '-' || COALESCE(MIC.CATEGORY_SET_ID, 0) AS dw_load_id,
    CURRENT_TIMESTAMP() AS dw_insert_date,
    CURRENT_TIMESTAMP() AS dw_update_date
  FROM silver_bec_ods.MTL_ITEM_CATEGORIES AS MIC, silver_bec_ods.MTL_CATEGORY_SETS_TL AS CSET, silver_bec_ods.MTL_SYSTEM_ITEMS_B AS MSI, silver_bec_ods.MTL_CATEGORIES_B AS MC
  WHERE
    1 = 1
    AND CSET.LANGUAGE = 'US'
    AND (
      MIC.kca_seq_date > (
        SELECT
          (
            executebegints - prune_days
          )
        FROM bec_etl_ctrl.batch_dw_info
        WHERE
          dw_table_name = 'dim_inv_item_category_set' AND batch_name = 'ap'
      )
      OR MC.kca_seq_date > (
        SELECT
          (
            executebegints - prune_days
          )
        FROM bec_etl_ctrl.batch_dw_info
        WHERE
          dw_table_name = 'dim_inv_item_category_set' AND batch_name = 'ap'
      )
    )
    AND MIC.INVENTORY_ITEM_ID = MSI.INVENTORY_ITEM_ID
    AND MIC.ORGANIZATION_ID = MSI.ORGANIZATION_ID
    AND MIC.CATEGORY_SET_ID = CSET.CATEGORY_SET_ID
    AND MIC.CATEGORY_ID = MC.CATEGORY_ID
);
/* soft Delete Records */
UPDATE gold_bec_dwh.dim_inv_item_category_set SET is_deleted_flg = 'Y'
WHERE
  NOT EXISTS(
    SELECT
      1
    FROM silver_bec_ods.MTL_ITEM_CATEGORIES AS MIC
    INNER JOIN silver_bec_ods.MTL_CATEGORY_SETS_TL AS CSET
      ON MIC.CATEGORY_SET_ID = CSET.CATEGORY_SET_ID AND CSET.LANGUAGE = 'US'
    INNER JOIN silver_bec_ods.MTL_SYSTEM_ITEMS_B AS MSI
      ON MIC.INVENTORY_ITEM_ID = MSI.INVENTORY_ITEM_ID
      AND MIC.ORGANIZATION_ID = MSI.ORGANIZATION_ID
    INNER JOIN silver_bec_ods.MTL_CATEGORIES_B AS MC
      ON MIC.CATEGORY_ID = MC.CATEGORY_ID
    WHERE
      COALESCE(MIC.INVENTORY_ITEM_ID, 0) = COALESCE(dim_inv_item_category_set.INVENTORY_ITEM_ID, 0)
      AND COALESCE(MIC.ORGANIZATION_ID, 0) = COALESCE(dim_inv_item_category_set.ORGANIZATION_ID, 0)
      AND COALESCE(MIC.CATEGORY_ID, 0) = COALESCE(dim_inv_item_category_set.CATEGORY_ID, 0)
      AND COALESCE(MIC.CATEGORY_SET_ID, 0) = COALESCE(dim_inv_item_category_set.CATEGORY_SET_ID, 0)
      AND (
        MIC.is_deleted_flg <> 'Y' OR MC.is_deleted_flg <> 'Y' OR MSI.is_deleted_flg <> 'Y'
      )
  );
UPDATE bec_etl_ctrl.batch_dw_info SET last_refresh_date = CURRENT_TIMESTAMP()
WHERE
  dw_table_name = 'dim_inv_item_category_set' AND batch_name = 'ap';