DROP TABLE IF EXISTS BEC_DWH.DIM_GLOBAL_CONVERSION;
CREATE TABLE BEC_DWH.DIM_GLOBAL_CONVERSION AS
(
  SELECT
    GLR.FROM_CURRENCY AS FROM_CURRENCY,
    GLR.TO_CURRENCY AS TO_CURRENCY,
    CAST(GLR.CONVERSION_DATE AS DATE) AS CONVERSION_DATE,
    GLR.CONVERSION_TYPE AS CONVERSION_TYPE,
    GLR.RATE_SOURCE_CODE AS RATE_SOURCE_CODE,
    GLR.CONVERSION_RATE AS CONVERSION_RATE,
    GLR.CONVERSION_RATE AS INVERSE_CONVERSION_RATE,
    'N' AS IS_DELETED_FLG, /* AUDIT COLUMNS */
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
);
UPDATE bec_etl_ctrl.batch_dw_info SET load_type = 'I', last_refresh_date = CURRENT_TIMESTAMP()
WHERE
  dw_table_name = 'dim_global_conversion' AND batch_name = 'gl';