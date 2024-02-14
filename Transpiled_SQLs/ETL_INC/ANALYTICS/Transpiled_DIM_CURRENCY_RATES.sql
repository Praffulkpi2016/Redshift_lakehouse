/* Delete Records */
DELETE FROM gold_bec_dwh.dim_currency_rates
WHERE
  (COALESCE(FROM_CURRENCY, 'NA'), COALESCE(TO_CURRENCY, 'NA'), COALESCE(CONVERSION_DATE, '1900-01-01'), COALESCE(CONVERSION_TYPE, 'NA')) IN (
    SELECT
      COALESCE(ods.FROM_CURRENCY, 'NA') AS FROM_CURRENCY,
      COALESCE(ods.TO_CURRENCY, 'NA') AS TO_CURRENCY,
      COALESCE(ods.CONVERSION_DATE, '1900-01-01') AS CONVERSION_DATE,
      COALESCE(ods.CONVERSION_TYPE, 'NA') AS CONVERSION_TYPE
    FROM gold_bec_dwh.dim_currency_rates AS dw, silver_bec_ods.GL_DAILY_RATES AS ods
    WHERE
      dw.dw_load_id = (
        SELECT
          system_id
        FROM bec_etl_ctrl.etlsourceappid
        WHERE
          source_system = 'EBS'
      ) || '-' || COALESCE(ods.FROM_CURRENCY, 'NA') || '-' || COALESCE(ods.TO_CURRENCY, 'NA') || '-' || COALESCE(ods.CONVERSION_DATE, '1900-01-01') || '-' || COALESCE(ods.CONVERSION_TYPE, 'NA')
      AND (
        ods.kca_seq_date > (
          SELECT
            (
              executebegints - prune_days
            )
          FROM bec_etl_ctrl.batch_dw_info
          WHERE
            dw_table_name = 'dim_currency_rates' AND batch_name = 'ap'
        )
      )
  );
/* Insert records */
INSERT INTO gold_bec_dwh.DIM_CURRENCY_RATES
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
  WHERE
    1 = 1
    AND (
      kca_seq_date > (
        SELECT
          (
            executebegints - prune_days
          )
        FROM bec_etl_ctrl.batch_dw_info
        WHERE
          dw_table_name = 'dim_currency_rates' AND batch_name = 'ap'
      )
    )
);
/* Soft delete */
UPDATE gold_bec_dwh.dim_currency_rates SET is_deleted_flg = 'Y'
WHERE
  NOT (COALESCE(FROM_CURRENCY, 'NA'), COALESCE(TO_CURRENCY, 'NA'), COALESCE(CONVERSION_DATE, '1900-01-01'), COALESCE(CONVERSION_TYPE, 'NA')) IN (
    SELECT
      COALESCE(ods.FROM_CURRENCY, 'NA') AS FROM_CURRENCY,
      COALESCE(ods.TO_CURRENCY, 'NA') AS TO_CURRENCY,
      COALESCE(ods.CONVERSION_DATE, '1900-01-01') AS CONVERSION_DATE,
      COALESCE(ods.CONVERSION_TYPE, 'NA') AS CONVERSION_TYPE
    FROM gold_bec_dwh.dim_currency_rates AS dw, silver_bec_ods.GL_DAILY_RATES AS ods
    WHERE
      dw.dw_load_id = (
        SELECT
          system_id
        FROM bec_etl_ctrl.etlsourceappid
        WHERE
          source_system = 'EBS'
      ) || '-' || COALESCE(ods.FROM_CURRENCY, 'NA') || '-' || COALESCE(ods.TO_CURRENCY, 'NA') || '-' || COALESCE(ods.CONVERSION_DATE, '1900-01-01') || '-' || COALESCE(ods.CONVERSION_TYPE, 'NA')
      AND ods.is_deleted_flg <> 'Y'
  );
UPDATE bec_etl_ctrl.batch_dw_info SET load_type = 'I', last_refresh_date = CURRENT_TIMESTAMP()
WHERE
  dw_table_name = 'dim_currency_rates' AND batch_name = 'ap';