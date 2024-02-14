DROP table IF EXISTS gold_bec_dwh.DIM_SO_ORDER_SOURCES;
CREATE TABLE gold_bec_dwh.DIM_SO_ORDER_SOURCES AS
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
);
UPDATE bec_etl_ctrl.batch_dw_info SET load_type = 'I', last_refresh_date = CURRENT_TIMESTAMP()
WHERE
  dw_table_name = 'dim_so_order_sources' AND batch_name = 'om';