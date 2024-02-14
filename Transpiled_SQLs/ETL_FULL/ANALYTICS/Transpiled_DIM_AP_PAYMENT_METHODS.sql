DROP table IF EXISTS gold_bec_dwh.DIM_AP_PAYMENT_METHODS;
CREATE TABLE gold_bec_dwh.dim_ap_payment_methods AS
(
  SELECT
    PAYMENT_METHOD_CODE,
    PAYMENT_METHOD_NAME,
    DESCRIPTION,
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
    ) || '-' || COALESCE(PAYMENT_METHOD_CODE, 'NA') AS dw_load_id,
    CURRENT_TIMESTAMP() AS dw_insert_date,
    CURRENT_TIMESTAMP() AS dw_update_date
  FROM silver_bec_ods.iby_payment_methods_tl
  WHERE
    language = 'US'
);
UPDATE bec_etl_ctrl.batch_dw_info SET load_type = 'I', last_refresh_date = CURRENT_TIMESTAMP()
WHERE
  dw_table_name = 'dim_ap_payment_methods' AND batch_name = 'ap';