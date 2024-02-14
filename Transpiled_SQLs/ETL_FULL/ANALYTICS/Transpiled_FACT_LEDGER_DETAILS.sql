DROP table IF EXISTS gold_bec_dwh.FACT_LEDGER_DETAILS;
CREATE TABLE gold_bec_dwh.FACT_LEDGER_DETAILS AS
(
  SELECT DISTINCT
    GL.LEDGER_ID,
    (
      SELECT
        system_id
      FROM bec_etl_ctrl.etlsourceappid
      WHERE
        source_system = 'EBS'
    ) || '-' || GL.LEDGER_ID AS LEDGER_ID_KEY,
    CASE
      WHEN GL.LEDGER_CATEGORY_CODE = 'PRIMARY'
      THEN GL.NAME
      ELSE (
        SELECT
          NAME
        FROM (
          SELECT
            *
          FROM silver_bec_ods.GL_LEDGERS
          WHERE
            is_deleted_flg <> 'Y'
        ) AS GL_LEDGERS
        WHERE
          LEDGER_ID = LEDGER_DETAILS.SOURCE_LEDGER_ID
      )
    END AS PRIMARY_LEDGER_NAME,
    CASE WHEN GL.LEDGER_CATEGORY_CODE = 'SECONDARY' THEN GL.NAME END AS SECONDARY_LEDGER_NAME,
    GL.DESCRIPTION AS LEDGER_DESC,
    GL.LEDGER_CATEGORY_CODE AS LEDGER_CATEGORY,
    GL.CHART_OF_ACCOUNTS_ID,
    (
      SELECT
        system_id
      FROM bec_etl_ctrl.etlsourceappid
      WHERE
        source_system = 'EBS'
    ) || '-' || GL.CHART_OF_ACCOUNTS_ID AS CHART_OF_ACCOUNTS_ID_KEY,
    GL.CURRENCY_CODE,
    GL.PERIOD_SET_NAME,
    GL.SLA_ACCOUNTING_METHOD_CODE,
    GL.RET_EARN_CODE_COMBINATION_ID AS RETAINED_EARNINGS_ACCT_ID,
    GL.SLA_LEDGER_CUR_BAL_SUS_CCID AS SUSPENSE_ACCT_ID,
    GL.ROUNDING_CODE_COMBINATION_ID AS ROUNDING_ACCT_ID,
    GL.CUM_TRANS_CODE_COMBINATION_ID AS ADJ_ACCT_ID,
    GL.ENABLE_RECONCILIATION_FLAG AS RECONCILATION_FLAG,
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
    ) || '-' || COALESCE(GL.LEDGER_ID, 0) AS dw_load_id,
    CURRENT_TIMESTAMP() AS dw_insert_date,
    CURRENT_TIMESTAMP() AS dw_update_date
  FROM (
    SELECT
      *
    FROM silver_bec_ods.GL_LEDGERS
    WHERE
      is_deleted_flg <> 'Y'
  ) AS GL, (
    SELECT
      GLR.TARGET_LEDGER_ID,
      GLR.TARGET_LEDGER_CATEGORY_CODE,
      GLR.SOURCE_LEDGER_ID,
      GL1.LEDGER_ID AS LEDGER_ID1
    FROM (
      SELECT
        *
      FROM silver_bec_ods.GL_LEDGER_CONFIG_DETAILS
      WHERE
        is_deleted_flg <> 'Y'
    ) AS GLCD, (
      SELECT
        *
      FROM silver_bec_ods.GL_LEDGERS
      WHERE
        is_deleted_flg <> 'Y'
    ) AS GL1, (
      SELECT
        *
      FROM silver_bec_ods.GL_LEDGER_RELATIONSHIPS
      WHERE
        is_deleted_flg <> 'Y'
    ) AS GLR
    WHERE
      1 = 1
      AND GLCD.SETUP_STEP_CODE = 'NONE'
      AND GLCD.OBJECT_TYPE_CODE = GLR.TARGET_LEDGER_CATEGORY_CODE
      AND GL1.CONFIGURATION_ID = GLCD.CONFIGURATION_ID
      AND GL1.LEDGER_CATEGORY_CODE IN ('PRIMARY', 'SECONDARY')
      AND GL1.LEDGER_ID = GLCD.OBJECT_ID
      AND GLR.TARGET_LEDGER_ID = GLCD.OBJECT_ID
      AND GLR.RELATIONSHIP_ENABLED_FLAG = 'Y'
      AND (
        (
          GLR.TARGET_LEDGER_CATEGORY_CODE IN ('PRIMARY')
        )
        OR (
          GLR.TARGET_LEDGER_CATEGORY_CODE IN ('SECONDARY')
          AND NOT RELATIONSHIP_TYPE_CODE IN ('NONE')
        )
      )
      AND GLR.TARGET_LEDGER_CATEGORY_CODE = GLCD.OBJECT_TYPE_CODE
  ) AS LEDGER_DETAILS
  WHERE
    (
      (
        GL.LEDGER_ID = LEDGER_DETAILS.TARGET_LEDGER_ID
      )
      OR (
        GL.LEDGER_ID = LEDGER_DETAILS.SOURCE_LEDGER_ID
      )
    )
  ORDER BY
    GL.LEDGER_ID NULLS LAST
);
UPDATE bec_etl_ctrl.batch_dw_info SET load_type = 'I', last_refresh_date = CURRENT_TIMESTAMP()
WHERE
  dw_table_name = 'fact_ledger_details' AND batch_name = 'gl';