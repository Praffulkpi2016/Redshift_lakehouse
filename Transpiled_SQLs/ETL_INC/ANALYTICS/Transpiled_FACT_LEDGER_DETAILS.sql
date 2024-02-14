/* Delete Records */
DELETE FROM gold_bec_dwh.FACT_LEDGER_DETAILS
WHERE
  COALESCE(LEDGER_ID, 0) IN (
    SELECT
      COALESCE(ods.LEDGER_ID, 0) AS LEDGER_ID
    FROM gold_bec_dwh.FACT_LEDGER_DETAILS AS dw, (
      SELECT DISTINCT
        GL.LEDGER_ID,
        CASE
          WHEN GL.LEDGER_CATEGORY_CODE = 'PRIMARY'
          THEN GL.NAME
          ELSE (
            SELECT
              NAME
            FROM silver_bec_ods.GL_LEDGERS AS GL_LEDGERS
            WHERE
              LEDGER_ID = LEDGER_DETAILS.SOURCE_LEDGER_ID
          )
        END AS PRIMARY_LEDGER_NAME,
        CASE WHEN GL.LEDGER_CATEGORY_CODE = 'SECONDARY' THEN GL.NAME END AS SECONDARY_LEDGER_NAME,
        GL.DESCRIPTION AS LEDGER_DESC,
        GL.LEDGER_CATEGORY_CODE AS LEDGER_CATEGORY,
        GL.CHART_OF_ACCOUNTS_ID,
        GL.CURRENCY_CODE,
        GL.PERIOD_SET_NAME,
        GL.SLA_ACCOUNTING_METHOD_CODE,
        GL.RET_EARN_CODE_COMBINATION_ID AS RETAINED_EARNINGS_ACCT_ID,
        GL.SLA_LEDGER_CUR_BAL_SUS_CCID AS SUSPENSE_ACCT_ID,
        GL.ROUNDING_CODE_COMBINATION_ID AS ROUNDING_ACCT_ID,
        GL.CUM_TRANS_CODE_COMBINATION_ID AS ADJ_ACCT_ID,
        GL.ENABLE_RECONCILIATION_FLAG AS RECONCILATION_FLAG
      FROM silver_bec_ods.GL_LEDGERS AS GL, (
        SELECT
          GLR.TARGET_LEDGER_ID,
          GLR.TARGET_LEDGER_CATEGORY_CODE,
          GLR.SOURCE_LEDGER_ID,
          GL1.LEDGER_ID AS LEDGER_ID1,
          GLCD.is_deleted_flg AS is_deleted_flg1,
          GL1.is_deleted_flg AS is_deleted_flg2,
          GLR.is_deleted_flg AS is_deleted_flg3
        FROM silver_bec_ods.GL_LEDGER_CONFIG_DETAILS AS GLCD, silver_bec_ods.GL_LEDGERS AS GL1, silver_bec_ods.GL_LEDGER_RELATIONSHIPS AS GLR
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
        AND GL.kca_seq_date > (
          SELECT
            (
              executebegints - prune_days
            )
          FROM bec_etl_ctrl.batch_dw_info
          WHERE
            dw_table_name = 'fact_ledger_details' AND batch_name = 'gl'
        )
        OR GL.is_deleted_flg = 'Y'
        OR LEDGER_DETAILS.is_deleted_flg1 = 'Y'
        OR LEDGER_DETAILS.is_deleted_flg2 = 'Y'
        OR LEDGER_DETAILS.is_deleted_flg3 = 'Y'
      ORDER BY
        GL.LEDGER_ID NULLS LAST
    ) AS ods
    WHERE
      dw.dw_load_id = (
        SELECT
          system_id
        FROM bec_etl_ctrl.etlsourceappid
        WHERE
          source_system = 'EBS'
      ) || '-' || COALESCE(ods.LEDGER_ID, 0)
  );
/* Insert records */
INSERT INTO gold_bec_dwh.FACT_LEDGER_DETAILS (
  ledger_id,
  ledger_id_key,
  primary_ledger_name,
  secondary_ledger_name,
  ledger_desc,
  ledger_category,
  chart_of_accounts_id,
  chart_of_accounts_id_key,
  currency_code,
  period_set_name,
  sla_accounting_method_code,
  retained_earnings_acct_id,
  suspense_acct_id,
  rounding_acct_id,
  adj_acct_id,
  reconcilation_flag,
  is_deleted_flg,
  source_app_id,
  dw_load_id,
  dw_insert_date,
  dw_update_date
)
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
    AND GL.kca_seq_date > (
      SELECT
        (
          executebegints - prune_days
        )
      FROM bec_etl_ctrl.batch_dw_info
      WHERE
        dw_table_name = 'fact_ledger_details' AND batch_name = 'gl'
    )
  ORDER BY
    GL.LEDGER_ID NULLS LAST
);
UPDATE bec_etl_ctrl.batch_dw_info SET last_refresh_date = CURRENT_TIMESTAMP()
WHERE
  dw_table_name = 'fact_ledger_details' AND batch_name = 'gl';