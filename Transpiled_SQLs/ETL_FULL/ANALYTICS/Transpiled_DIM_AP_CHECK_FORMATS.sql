DROP table IF EXISTS gold_bec_dwh.DIM_AP_CHECK_FORMATS;
CREATE TABLE gold_bec_dwh.DIM_AP_CHECK_FORMATS AS
(
  SELECT
    check_format_id,
    name AS `CHECK_FORMAT`,
    invoices_per_stub,
    payment_method_lookup_code AS `PAYMENT_METHOD`,
    currency_code,
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
    ) || '-' || COALESCE(check_format_id, 0) AS dw_load_id,
    CURRENT_TIMESTAMP() AS dw_insert_date,
    CURRENT_TIMESTAMP() AS dw_update_date
  FROM silver_bec_ods.ap_check_formats
);
UPDATE bec_etl_ctrl.batch_dw_info SET load_type = 'I', last_refresh_date = CURRENT_TIMESTAMP()
WHERE
  dw_table_name = 'dim_ap_check_formats' AND batch_name = 'ap';