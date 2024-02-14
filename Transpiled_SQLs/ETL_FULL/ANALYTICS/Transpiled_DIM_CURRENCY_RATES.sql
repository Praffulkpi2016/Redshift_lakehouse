DROP table IF EXISTS gold_bec_dwh.DIM_CURRENCY_RATES;
CREATE TABLE gold_bec_dwh.DIM_CURRENCY_RATES AS
(
  SELECT
    FROM_CURRENCY,
    TO_CURRENCY,
    CONVERSION_DATE,
    CONVERSION_TYPE,
    RATE_SOURCE_CODE,
    ROUND(CONVERSION_RATE, 5) AS CONVERSION_RATE,
    ROUND(1 / (
      CONVERSION_RATE
    ), 5) AS INVERSE_CONVERSION_RATE,
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
    ) || '-' || COALESCE(FROM_CURRENCY, 'NA') || '-' || COALESCE(TO_CURRENCY, 'NA') || '-' || COALESCE(CONVERSION_DATE, '1900-01-01') || '-' || COALESCE(CONVERSION_TYPE, 'NA') AS dw_load_id,
    CURRENT_TIMESTAMP() AS dw_insert_date,
    CURRENT_TIMESTAMP() AS dw_update_date
  FROM silver_bec_ods.GL_DAILY_RATES
);
UPDATE bec_etl_ctrl.batch_dw_info SET load_type = 'I', last_refresh_date = CURRENT_TIMESTAMP()
WHERE
  dw_table_name = 'dim_currency_rates' AND batch_name = 'ap';