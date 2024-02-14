/* Delete Records */
DELETE FROM gold_bec_dwh.FACT_INV_RELATED_ITEMS
WHERE
  (
    (
      COALESCE(RELATED_ITEM_ID, 0)
    )
  ) IN (
    SELECT
      COALESCE(ODS.RELATED_ITEM_ID, 0) AS RELATED_ITEM_ID
    FROM gold_bec_dwh.FACT_INV_RELATED_ITEMS AS dw, (
      SELECT
        RELATED_ITEM_ID
      FROM silver_bec_ods.mtl_related_items
      WHERE
        (
          kca_seq_date > (
            SELECT
              (
                executebegints - prune_days
              )
            FROM bec_etl_ctrl.batch_dw_info
            WHERE
              dw_table_name = 'fact_inv_related_items' AND batch_name = 'inv'
          )
          OR is_deleted_flg = 'Y'
        )
    ) AS ODS
    WHERE
      dw.dw_load_id = (
        SELECT
          system_id
        FROM bec_etl_ctrl.etlsourceappid
        WHERE
          source_system = 'EBS'
      ) || '-' || COALESCE(ods.RELATED_ITEM_ID, 0)
  );
/* Insert records */
INSERT INTO gold_bec_dwh.FACT_INV_RELATED_ITEMS (
  inventory_item_id,
  organization_id,
  related_item_id,
  relationship_type_id,
  reciprocal_flag,
  planning_enabled_flag,
  start_date,
  end_date,
  creation_date,
  created_by,
  last_update_date,
  last_updated_by,
  inventory_item_id_key,
  organization_id_key,
  related_item_id_key,
  relationship_type_id_key,
  source_app_id,
  dw_load_id,
  dw_insert_date,
  dw_update_date
)
(
  SELECT
    INVENTORY_ITEM_ID,
    ORGANIZATION_ID,
    RELATED_ITEM_ID,
    relationship_type_id,
    reciprocal_flag,
    PLANNING_ENABLED_FLAG,
    START_DATE,
    END_DATE,
    CREATION_DATE,
    CREATED_BY,
    LAST_UPDATE_DATE,
    LAST_UPDATED_BY,
    (
      SELECT
        SYSTEM_ID
      FROM BEC_ETL_CTRL.ETLSOURCEAPPID
      WHERE
        SOURCE_SYSTEM = 'EBS'
    ) || '-' || INVENTORY_ITEM_ID AS INVENTORY_ITEM_ID_KEY,
    (
      SELECT
        SYSTEM_ID
      FROM BEC_ETL_CTRL.ETLSOURCEAPPID
      WHERE
        SOURCE_SYSTEM = 'EBS'
    ) || '-' || ORGANIZATION_ID AS ORGANIZATION_ID_KEY,
    (
      SELECT
        SYSTEM_ID
      FROM BEC_ETL_CTRL.ETLSOURCEAPPID
      WHERE
        SOURCE_SYSTEM = 'EBS'
    ) || '-' || RELATED_ITEM_ID AS RELATED_ITEM_ID_KEY,
    (
      SELECT
        SYSTEM_ID
      FROM BEC_ETL_CTRL.ETLSOURCEAPPID
      WHERE
        SOURCE_SYSTEM = 'EBS'
    ) || '-' || RELATIONSHIP_TYPE_ID AS RELATIONSHIP_TYPE_ID_KEY,
    (
      SELECT
        SYSTEM_ID
      FROM BEC_ETL_CTRL.ETLSOURCEAPPID
      WHERE
        SOURCE_SYSTEM = 'EBS'
    ) AS SOURCE_APP_ID,
    (
      SELECT
        SYSTEM_ID
      FROM BEC_ETL_CTRL.ETLSOURCEAPPID
      WHERE
        SOURCE_SYSTEM = 'EBS'
    ) || '-' || COALESCE(RELATED_ITEM_ID, 0) AS DW_LOAD_ID,
    CURRENT_TIMESTAMP() AS DW_INSERT_DATE,
    CURRENT_TIMESTAMP() AS DW_UPDATE_DATE
  FROM (
    SELECT
      *
    FROM silver_bec_ods.mtl_related_items
    WHERE
      is_deleted_flg <> 'Y'
  )
  WHERE
    (
      kca_seq_date > (
        SELECT
          (
            executebegints - prune_days
          )
        FROM bec_etl_ctrl.batch_dw_info
        WHERE
          dw_table_name = 'fact_inv_related_items' AND batch_name = 'inv'
      )
    )
);
UPDATE bec_etl_ctrl.batch_dw_info SET load_type = 'I', last_refresh_date = CURRENT_TIMESTAMP()
WHERE
  dw_table_name = 'fact_inv_related_items' AND batch_name = 'inv';