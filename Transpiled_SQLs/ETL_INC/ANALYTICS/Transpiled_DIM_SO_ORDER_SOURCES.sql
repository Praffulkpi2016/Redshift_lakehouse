/* Delete Records */
DELETE FROM gold_bec_dwh.DIM_SO_ORDER_SOURCES
WHERE
  COALESCE(ORDER_SOURCE_ID, 0) IN (
    SELECT
      COALESCE(ods.ORDER_SOURCE_ID, 0) AS ORDER_SOURCE_ID
    FROM gold_bec_dwh.DIM_SO_ORDER_SOURCES AS dw, silver_bec_ods.OE_ORDER_SOURCES AS ods
    WHERE
      dw.dw_load_id = (
        SELECT
          system_id
        FROM bec_etl_ctrl.etlsourceappid
        WHERE
          source_system = 'EBS'
      ) || '-' || COALESCE(ods.ORDER_SOURCE_ID, 0)
      AND (
        ods.kca_seq_date > (
          SELECT
            (
              executebegints - prune_days
            )
          FROM bec_etl_ctrl.batch_dw_info
          WHERE
            dw_table_name = 'dim_so_order_sources' AND batch_name = 'om'
        )
      )
  );
/* Insert records */
INSERT INTO gold_bec_dwh.DIM_SO_ORDER_SOURCES (
  ORDER_SOURCE_ID,
  CREATION_DATE,
  CREATED_BY,
  LAST_UPDATE_DATE,
  LAST_UPDATED_BY,
  LAST_UPDATE_LOGIN,
  NAME,
  DESCRIPTION,
  ENABLED_FLAG,
  CREATE_CUSTOMERS_FLAG,
  USE_IDS_FLAG,
  AIA_ENABLED_FLAG,
  ZD_EDITION_NAME,
  is_deleted_flg,
  source_app_id,
  dw_load_id,
  dw_insert_date,
  dw_update_date
)
(
  SELECT
    ORDER_SOURCE_ID,
    CREATION_DATE,
    CREATED_BY,
    LAST_UPDATE_DATE,
    LAST_UPDATED_BY,
    LAST_UPDATE_LOGIN,
    NAME,
    DESCRIPTION,
    ENABLED_FLAG,
    CREATE_CUSTOMERS_FLAG,
    USE_IDS_FLAG,
    AIA_ENABLED_FLAG,
    ZD_EDITION_NAME,
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
    ) || '-' || COALESCE(ORDER_SOURCE_ID, 0) AS dw_load_id,
    CURRENT_TIMESTAMP() AS dw_insert_date,
    CURRENT_TIMESTAMP() AS dw_update_date
  FROM silver_bec_ods.OE_ORDER_SOURCES
  WHERE
    (
      kca_seq_date > (
        SELECT
          (
            executebegints - prune_days
          )
        FROM bec_etl_ctrl.batch_dw_info
        WHERE
          dw_table_name = 'dim_so_order_sources' AND batch_name = 'om'
      )
    )
);
/* Soft delete */
UPDATE gold_bec_dwh.DIM_SO_ORDER_SOURCES SET is_deleted_flg = 'Y'
WHERE
  NOT COALESCE(ORDER_SOURCE_ID, 0) IN (
    SELECT
      COALESCE(ods.ORDER_SOURCE_ID, 0) AS ORDER_SOURCE_ID
    FROM gold_bec_dwh.DIM_SO_ORDER_SOURCES AS dw, (
      SELECT
        *
      FROM silver_bec_ods.OE_ORDER_SOURCES
      WHERE
        is_deleted_flg <> 'Y'
    ) AS ods
    WHERE
      dw.dw_load_id = (
        SELECT
          system_id
        FROM bec_etl_ctrl.etlsourceappid
        WHERE
          source_system = 'EBS'
      ) || '-' || COALESCE(ods.ORDER_SOURCE_ID, 0)
  );
UPDATE bec_etl_ctrl.batch_dw_info SET load_type = 'I', last_refresh_date = CURRENT_TIMESTAMP()
WHERE
  dw_table_name = 'dim_so_order_sources' AND batch_name = 'om';