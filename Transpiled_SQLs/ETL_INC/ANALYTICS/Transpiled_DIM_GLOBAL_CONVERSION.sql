/* Delete Records */
DELETE FROM gold_bec_dwh.DIM_GLOBAL_CONVERSION
WHERE
  (COALESCE(FROM_CURRENCY, 'NA'), COALESCE(TO_CURRENCY, 'NA'), COALESCE(CAST(CONVERSION_DATE AS DATE), '1900-01-01'), COALESCE(CONVERSION_TYPE, 'NA'), COALESCE(CONVERSION_RATE, 0)) IN (
    SELECT
      COALESCE(ods.FROM_CURRENCY, 'NA') AS FROM_CURRENCY,
      COALESCE(ods.TO_CURRENCY, 'NA') AS TO_CURRENCY,
      COALESCE(CAST(ods.CONVERSION_DATE AS DATE), '1900-01-01') AS CONVERSION_DATE,
      COALESCE(ods.CONVERSION_TYPE, 'NA') AS CONVERSION_TYPE,
      COALESCE(ods.CONVERSION_RATE, 0) AS CONVERSION_RATE
    FROM gold_bec_dwh.DIM_GLOBAL_CONVERSION AS dw, (
      SELECT
        GLR.FROM_CURRENCY AS FROM_CURRENCY,
        GLR.TO_CURRENCY AS TO_CURRENCY,
        CAST(GLR.CONVERSION_DATE AS DATE) AS CONVERSION_DATE,
        GLR.CONVERSION_TYPE AS CONVERSION_TYPE,
        GLR.CONVERSION_RATE AS CONVERSION_RATE,
        GLR.LAST_UPDATE_DATE AS LAST_UPDATE_DATE,
        GLR.kca_seq_date AS kca_seq_date
      FROM BEC_ODS.GL_DAILY_RATES AS GLR
    ) AS ods
    WHERE
      dw.dw_load_id = (
        SELECT
          system_id
        FROM bec_etl_ctrl.etlsourceappid
        WHERE
          source_system = 'EBS'
      ) || '-' || COALESCE(ods.FROM_CURRENCY, 'NA') || '-' || COALESCE(ods.TO_CURRENCY, 'NA') || '-' || COALESCE(CAST(ods.CONVERSION_DATE AS DATE), '1900-01-01') || '-' || COALESCE(ods.CONVERSION_TYPE, 'NA') || '-' || COALESCE(ods.CONVERSION_RATE, 0)
      AND (
        ods.kca_seq_date > (
          SELECT
            (
              executebegints - prune_days
            )
          FROM bec_etl_ctrl.batch_dw_info
          WHERE
            dw_table_name = 'dim_global_conversion' AND batch_name = 'gl'
        )
      )
  );
/* Insert records */
INSERT INTO gold_bec_dwh.DIM_GLOBAL_CONVERSION (
  from_currency,
  to_currency,
  conversion_date,
  conversion_type,
  rate_source_code,
  conversion_rate,
  inverse_conversion_rate,
  is_deleted_flg,
  source_app_id,
  dw_load_id,
  dw_insert_date,
  dw_update_date
)
(
  SELECT
    GLR.FROM_CURRENCY AS FROM_CURRENCY,
    GLR.TO_CURRENCY AS TO_CURRENCY,
    CAST(GLR.CONVERSION_DATE AS DATE) AS CONVERSION_DATE,
    GLR.CONVERSION_TYPE AS CONVERSION_TYPE,
    GLR.RATE_SOURCE_CODE AS RATE_SOURCE_CODE,
    GLR.CONVERSION_RATE AS CONVERSION_RATE,
    GLR.CONVERSION_RATE AS INVERSE_CONVERSION_RATE,
    'N' AS is_deleted_flg, /* AUDIT COLUMNS */
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
    ) || '-' || COALESCE(FROM_CURRENCY, 'NA') || '-' || COALESCE(TO_CURRENCY, 'NA') || '-' || COALESCE(CAST(CONVERSION_DATE AS DATE), '1900-01-01') || '-' || COALESCE(CONVERSION_TYPE, 'NA') || '-' || COALESCE(CONVERSION_RATE, 0) AS DW_LOAD_ID,
    CURRENT_TIMESTAMP() AS DW_INSERT_DATE,
    CURRENT_TIMESTAMP() AS DW_UPDATE_DATE
  FROM BEC_ODS.GL_DAILY_RATES AS GLR
  WHERE
    1 = 1
    AND (
      GLR.kca_seq_date > (
        SELECT
          (
            executebegints - prune_days
          )
        FROM bec_etl_ctrl.batch_dw_info
        WHERE
          dw_table_name = 'dim_global_conversion' AND batch_name = 'gl'
      )
    )
);
/* Soft delete */
UPDATE gold_bec_dwh.DIM_GLOBAL_CONVERSION SET is_deleted_flg = 'Y'
WHERE
  NOT (COALESCE(FROM_CURRENCY, 'NA'), COALESCE(TO_CURRENCY, 'NA'), COALESCE(CAST(CONVERSION_DATE AS DATE), '1900-01-01'), COALESCE(CONVERSION_TYPE, 'NA'), COALESCE(CONVERSION_RATE, 0)) IN (
    SELECT
      COALESCE(ods.FROM_CURRENCY, 'NA') AS FROM_CURRENCY,
      COALESCE(ods.TO_CURRENCY, 'NA') AS TO_CURRENCY,
      COALESCE(CAST(ods.CONVERSION_DATE AS DATE), '1900-01-01') AS CONVERSION_DATE,
      COALESCE(ods.CONVERSION_TYPE, 'NA') AS CONVERSION_TYPE,
      COALESCE(ods.CONVERSION_RATE, 0) AS CONVERSION_RATE
    FROM gold_bec_dwh.DIM_GLOBAL_CONVERSION AS dw, (
      SELECT
        GLR.FROM_CURRENCY AS FROM_CURRENCY,
        GLR.TO_CURRENCY AS TO_CURRENCY,
        CAST(GLR.CONVERSION_DATE AS DATE) AS CONVERSION_DATE,
        GLR.CONVERSION_TYPE AS CONVERSION_TYPE,
        GLR.CONVERSION_RATE AS CONVERSION_RATE
      FROM silver_bec_ods.GL_DAILY_RATES AS GLR
      WHERE
        GLR.is_deleted_flg <> 'Y'
    ) AS ods
    WHERE
      dw.dw_load_id = (
        SELECT
          system_id
        FROM bec_etl_ctrl.etlsourceappid
        WHERE
          source_system = 'EBS'
      ) || '-' || COALESCE(ods.FROM_CURRENCY, 'NA') || '-' || COALESCE(ods.TO_CURRENCY, 'NA') || '-' || COALESCE(CAST(ods.CONVERSION_DATE AS DATE), '1900-01-01') || '-' || COALESCE(ods.CONVERSION_TYPE, 'NA') || '-' || COALESCE(ods.CONVERSION_RATE, 0)
  );
UPDATE bec_etl_ctrl.batch_dw_info SET last_refresh_date = CURRENT_TIMESTAMP()
WHERE
  dw_table_name = 'dim_global_conversion' AND batch_name = 'gl';