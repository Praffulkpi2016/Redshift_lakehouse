DROP table IF EXISTS gold_bec_dwh.DIM_PERIODS;
CREATE TABLE gold_bec_dwh.DIM_PERIODS AS
(
  SELECT
    GP.PERIOD_SET_NAME,
    GP.PERIOD_TYPE,
    GP.PERIOD_NAME,
    UPPER(GP.PERIOD_NAME) AS `GL_PERIOD`,
    GP.PERIOD_NAME AS `GL_PERIOD_LINK`,
    GP.PERIOD_NUM AS `PERIOD_NUMBER`,
    GP.PERIOD_YEAR,
    GP.QUARTER_NUM,
    GP.PERIOD_YEAR || 'Q' || GP.QUARTER_NUM || 'M' || GP.PERIOD_NUM AS `PERIOD_YYYY_QQ_MM`,
    GP.ENTERED_PERIOD_NAME,
    GP.YEAR_START_DATE,
    ADD_MONTHS(GP.YEAR_START_DATE, 12) - 1 AS `YEAR_END_DATE`,
    GP.QUARTER_START_DATE,
    ADD_MONTHS(GP.QUARTER_START_DATE, 3) - 1 AS `QUARTER_END_DATE`,
    GP.START_DATE,
    GP.END_DATE,
    UPPER(GP.ENTERED_PERIOD_NAME) || '-' || GP.PERIOD_YEAR AS `PERIOD_YYYY`,
    UPPER(GP.ENTERED_PERIOD_NAME) || '-' || GP.PERIOD_YEAR AS `PERIOD_NAME_YYYY`,
    1 AS `SORT_KEY`,
    ADJUSTMENT_PERIOD_FLAG,
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
    ) || '-' || COALESCE(PERIOD_SET_NAME, 'NA') || '-' || COALESCE(PERIOD_NAME, 'NA') AS dw_load_id,
    CURRENT_TIMESTAMP() AS dw_insert_date,
    CURRENT_TIMESTAMP() AS dw_update_date
  FROM silver_bec_ods.GL_PERIODS AS GP
  WHERE
    gp.is_deleted_flg <> 'Y'
);
UPDATE bec_etl_ctrl.batch_dw_info SET load_type = 'I', last_refresh_date = CURRENT_TIMESTAMP()
WHERE
  dw_table_name = 'dim_periods' AND batch_name = 'ap';