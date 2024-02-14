/* Delete Records */
DELETE FROM gold_bec_dwh.dim_ledgers
WHERE
  (
    COALESCE(LEDGER_ID, 0)
  ) IN (
    SELECT
      COALESCE(ods.LEDGER_ID, 0) AS LEDGER_ID
    FROM gold_bec_dwh.dim_ledgers AS dw, silver_bec_ods.GL_LEDGERS AS ods
    WHERE
      dw.dw_load_id = (
        SELECT
          system_id
        FROM bec_etl_ctrl.etlsourceappid
        WHERE
          source_system = 'EBS'
      ) || '-' || COALESCE(ods.LEDGER_ID, 0)
      AND (
        ods.kca_seq_date > (
          SELECT
            (
              executebegints - prune_days
            )
          FROM bec_etl_ctrl.batch_dw_info
          WHERE
            dw_table_name = 'dim_ledgers' AND batch_name = 'ap'
        )
      )
  );
/* Insert records */
INSERT INTO gold_bec_dwh.DIM_LEDGERS (
  LEDGER_ID,
  LEDGER_NAME,
  LEDGER_SHORT_NAME,
  LEDGER_DESCRIPTION,
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
  is_deleted_flg,
  SOURCE_APP_ID,
  DW_LOAD_ID,
  DW_INSERT_DATE,
  DW_UPDATE_DATE
)
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
          dw_table_name = 'dim_ledgers' AND batch_name = 'ap'
      )
    )
);
/* Soft delete */
UPDATE gold_bec_dwh.dim_ledgers SET is_deleted_flg = 'Y'
WHERE
  NOT (
    COALESCE(LEDGER_ID, 0)
  ) IN (
    SELECT
      COALESCE(ods.LEDGER_ID, 0) AS LEDGER_ID
    FROM gold_bec_dwh.dim_ledgers AS dw, silver_bec_ods.GL_LEDGERS AS ods
    WHERE
      dw.dw_load_id = (
        SELECT
          system_id
        FROM bec_etl_ctrl.etlsourceappid
        WHERE
          source_system = 'EBS'
      ) || '-' || COALESCE(ods.LEDGER_ID, 0)
      AND ods.is_deleted_flg <> 'Y'
  );
UPDATE bec_etl_ctrl.batch_dw_info SET last_refresh_date = CURRENT_TIMESTAMP()
WHERE
  dw_table_name = 'dim_ledgers' AND batch_name = 'ap';