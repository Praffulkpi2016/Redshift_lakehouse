DROP table IF EXISTS gold_bec_dwh.DIM_EXPENDITURE_ITEMS;
CREATE TABLE gold_bec_dwh.DIM_EXPENDITURE_ITEMS AS
(
  SELECT
    EXPENDITURE_ITEM_ID,
    EXPENDITURE_ID,
    EXPENDITURE_TYPE,
    orig_transaction_reference,
    transaction_source,
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
    ) || '-' || COALESCE(EXPENDITURE_ITEM_ID, 0) AS dw_load_id,
    CURRENT_TIMESTAMP() AS dw_insert_date,
    CURRENT_TIMESTAMP() AS dw_update_date
  FROM silver_bec_ods.pa_expenditure_items_all
);
UPDATE bec_etl_ctrl.batch_dw_info SET load_type = 'I', last_refresh_date = CURRENT_TIMESTAMP()
WHERE
  dw_table_name = 'dim_expenditure_items' AND batch_name = 'gl';