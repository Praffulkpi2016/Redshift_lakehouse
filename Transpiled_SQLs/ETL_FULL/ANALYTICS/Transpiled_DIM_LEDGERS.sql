DROP table IF EXISTS gold_bec_dwh.DIM_LEDGERS;
CREATE TABLE gold_bec_dwh.DIM_LEDGERS AS
(
  SELECT
    LEDGER_ID,
    NAME AS `LEDGER_NAME`,
    SHORT_NAME AS `LEDGER_SHORT_NAME`,
    DESCRIPTION AS `LEDGER_DESCRIPTION`,
    LEDGER_CATEGORY_CODE,
    CHART_OF_ACCOUNTS_ID,
    CURRENCY_CODE,
    ACCOUNTED_PERIOD_TYPE,
    RET_EARN_CODE_COMBINATION_ID,
    DAILY_TRANSLATION_RATE_TYPE,
    BAL_SEG_COLUMN_NAME,
    BAL_SEG_VALUE_SET_ID,
    SLA_DESCRIPTION_LANGUAGE,
    PERIOD_SET_NAME,
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
    ) || '-' || COALESCE(LEDGER_ID, 0) AS dw_load_id,
    CURRENT_TIMESTAMP() AS dw_insert_date,
    CURRENT_TIMESTAMP() AS dw_update_date
  FROM silver_bec_ods.GL_LEDGERS
);
UPDATE bec_etl_ctrl.batch_dw_info SET load_type = 'I', last_refresh_date = CURRENT_TIMESTAMP()
WHERE
  dw_table_name = 'dim_ledgers' AND batch_name = 'ap';