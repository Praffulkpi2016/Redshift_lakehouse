DROP table IF EXISTS gold_bec_dwh.DIM_ASCP_HP_SOURCE_TYPES;
CREATE TABLE gold_bec_dwh.DIM_ASCP_HP_SOURCE_TYPES AS
(
  SELECT
    order_type_entity,
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
    ) || '-' || COALESCE(order_type_entity, 'NA') AS dw_load_id,
    CURRENT_TIMESTAMP() AS dw_insert_date,
    CURRENT_TIMESTAMP() AS dw_update_date
  FROM (
    SELECT
      'Gross Requirements' AS order_type_entity
    UNION ALL
    SELECT
      'Total Supply'
    UNION ALL
    SELECT
      'Current Scheduled Receipts'
    UNION ALL
    SELECT
      'Projected Available Balance'
    UNION ALL
    SELECT
      'Projected On Hand'
    UNION ALL
    SELECT
      'Sales Orders'
    UNION ALL
    SELECT
      'Forecast'
    UNION ALL
    SELECT
      'Dependent Demand'
    UNION ALL
    SELECT
      'Expected Scrap'
    UNION ALL
    SELECT
      'Work Orders'
    UNION ALL
    SELECT
      'Purchase order'
    UNION ALL
    SELECT
      'Requisition'
    UNION ALL
    SELECT
      'In transit'
    UNION ALL
    SELECT
      'In receiving'
    UNION ALL
    SELECT
      'Planned order'
    UNION ALL
    SELECT
      'On Hand'
    UNION ALL
    SELECT
      'Safety Stock'
  )
);
UPDATE bec_etl_ctrl.batch_dw_info SET load_type = 'I', last_refresh_date = CURRENT_TIMESTAMP()
WHERE
  dw_table_name = 'dim_ascp_hp_source_types' AND batch_name = 'ascp';