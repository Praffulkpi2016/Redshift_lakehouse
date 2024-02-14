/* Delete Records */
DELETE FROM gold_bec_dwh.fact_gl_journals
WHERE
  EXISTS(
    SELECT
      1
    FROM silver_bec_ods.GL_JE_LINES AS JL, silver_bec_ods.GL_JE_HEADERS AS JH, silver_bec_ods.GL_JE_BATCHES AS JB, silver_bec_ods.GL_CODE_COMBINATIONS AS GCC, silver_bec_ods.GL_LEDGERS AS GL
    WHERE
      1 = 1
      AND GCC.CODE_COMBINATION_ID = JL.CODE_COMBINATION_ID
      AND JH.JE_HEADER_ID = JL.JE_HEADER_ID
      AND JH.JE_BATCH_ID = JB.JE_BATCH_ID
      AND JL.LEDGER_ID = GL.LEDGER_ID
      AND GCC.SUMMARY_FLAG = 'N'
      AND (
        JL.kca_seq_date > (
          SELECT
            (
              executebegints - prune_days
            )
          FROM bec_etl_ctrl.batch_dw_info
          WHERE
            dw_table_name = 'fact_gl_journals' AND batch_name = 'gl'
        )
        OR GCC.kca_seq_date > (
          SELECT
            (
              executebegints - prune_days
            )
          FROM bec_etl_ctrl.batch_dw_info
          WHERE
            dw_table_name = 'fact_gl_journals' AND batch_name = 'gl'
        )
      )
      AND fact_gl_journals.JE_HEADER_ID = JL.JE_HEADER_ID
      AND fact_gl_journals.JE_LINE_NUM = JL.JE_LINE_NUM
      AND fact_gl_journals.CODE_COMBINATION_ID = GCC.CODE_COMBINATION_ID
  );
/* Insert records */
INSERT INTO gold_bec_dwh.fact_gl_journals (
  JE_HEADER_ID,
  JE_LINE_NUM,
  LEDGER_ID,
  LEDGER_ID_KEY,
  PERIOD_SET_NAME,
  CHART_OF_ACCOUNTS_ID,
  PERIOD_NAME,
  CODE_COMBINATION_ID,
  CODE_COMBINATION_ID_KEY,
  ACCOUNT_TYPE,
  company,
  department,
  Account,
  budget_id,
  intercompany,
  `LOCATION`,
  `JOURNAL_LINE_STATUS`,
  `JOURNAL_HEADER_STATUS`,
  `JOURNAL_BATCH_STATUS`,
  EFFECTIVE_DATE,
  LINE_DESCRIPTION,
  HEADER_DESCRIPTION,
  JE_CATEGORY,
  `JOURNAL_SOURCE`,
  `JOURNAL_NAME`,
  ACTUAL_FLAG,
  `BATCH_NAME`,
  batch_description,
  posted_by,
  ORG_ID,
  ORG_ID_KEY,
  JE_BATCH_ID,
  `POSTED_DATE`,
  LAST_UPDATED_BY,
  CREATED_BY,
  `LINE_LAST_UPDATE`,
  `LINE_CREATION_DATE`,
  `DATE_CREATED`,
  JE_FROM_SLA_FLAG,
  TAX_STATUS_CODE,
  CURRENCY_CONVERSION_RATE,
  CURRENCY_CODE,
  external_reference,
  running_total_accounted_cr,
  running_total_accounted_dr,
  ENTERED_DR,
  ENTERED_CR,
  ACCOUNTED_DR,
  ACCOUNTED_CR,
  REFERENCE_1,
  REFERENCE_2,
  REFERENCE_3,
  REFERENCE_4,
  REFERENCE_5,
  REFERENCE_6,
  REFERENCE_7,
  REFERENCE_8,
  REFERENCE_9,
  REFERENCE_10,
  GL_SL_LINK_ID,
  GL_SL_LINK_TABLE,
  project_number,
  task_number,
  expnd_type,
  IS_DELETED_FLG,
  orig_transaction_reference,
  source_app_id,
  dw_load_id,
  dw_insert_date,
  dw_update_date
)
(
  SELECT
    JL.JE_HEADER_ID,
    JL.JE_LINE_NUM,
    JL.LEDGER_ID,
    (
      SELECT
        system_id
      FROM bec_etl_ctrl.etlsourceappid
      WHERE
        source_system = 'EBS'
    ) || '-' || JL.LEDGER_ID AS LEDGER_ID_KEY,
    GL.PERIOD_SET_NAME,
    GCC.CHART_OF_ACCOUNTS_ID,
    JL.PERIOD_NAME AS `PERIOD_NAME`,
    GCC.CODE_COMBINATION_ID,
    (
      SELECT
        system_id
      FROM bec_etl_ctrl.etlsourceappid
      WHERE
        source_system = 'EBS'
    ) || '-' || GCC.CODE_COMBINATION_ID AS CODE_COMBINATION_ID_KEY,
    GCC.ACCOUNT_TYPE,
    gcc.segment1 AS company,
    gcc.segment2 AS department,
    gcc.segment3 AS Account,
    gcc.segment5 AS budget_id,
    gcc.segment4 AS intercompany,
    gcc.segment6 AS `LOCATION`,
    JL.STATUS AS `JOURNAL_LINE_STATUS`,
    JH.STATUS AS `JOURNAL_HEADER_STATUS`,
    JB.STATUS AS `JOURNAL_BATCH_STATUS`,
    JL.EFFECTIVE_DATE,
    JL.DESCRIPTION AS LINE_DESCRIPTION,
    JH.DESCRIPTION AS HEADER_DESCRIPTION,
    JH.JE_CATEGORY,
    JH.JE_SOURCE AS `JOURNAL_SOURCE`,
    JH.NAME AS `JOURNAL_NAME`,
    JH.ACTUAL_FLAG,
    JB.NAME AS `BATCH_NAME`,
    jb.description AS batch_description,
    jb.posted_by,
    JB.ORG_ID AS ORG_ID,
    (
      SELECT
        system_id
      FROM bec_etl_ctrl.etlsourceappid
      WHERE
        source_system = 'EBS'
    ) || '-' || JB.ORG_ID AS ORG_ID_KEY,
    JH.JE_BATCH_ID,
    JH.POSTED_DATE AS `POSTED_DATE`,
    JL.LAST_UPDATED_BY,
    JL.CREATED_BY,
    JL.LAST_UPDATE_DATE AS `LINE_LAST_UPDATE`,
    JL.CREATION_DATE AS `LINE_CREATION_DATE`,
    JH.DATE_CREATED AS `DATE_CREATED`,
    JH.JE_FROM_SLA_FLAG,
    JH.TAX_STATUS_CODE,
    JH.CURRENCY_CONVERSION_RATE,
    JH.CURRENCY_CODE,
    jh.external_reference,
    jh.running_total_accounted_cr,
    jh.running_total_accounted_dr,
    JL.ENTERED_DR,
    JL.ENTERED_CR,
    JL.ACCOUNTED_DR,
    JL.ACCOUNTED_CR,
    JL.REFERENCE_1,
    JL.REFERENCE_2,
    JL.REFERENCE_3,
    JL.REFERENCE_4,
    JL.REFERENCE_5,
    JL.REFERENCE_6,
    JL.REFERENCE_7,
    JL.REFERENCE_8,
    JL.REFERENCE_9,
    JL.REFERENCE_10,
    JL.GL_SL_LINK_ID,
    JL.GL_SL_LINK_TABLE,
    JL.ATTRIBUTE1 AS project_number,
    JL.ATTRIBUTE2 AS task_number,
    JL.ATTRIBUTE3 AS expnd_type,
    'N' AS IS_DELETED_FLG,
    JL.PERIOD_NAME || JH.NAME || JL.JE_LINE_NUM AS orig_transaction_reference,
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
    ) || '-' || COALESCE(GL.LEDGER_ID, 0) || '-' || COALESCE(JL.JE_HEADER_ID, 0) || '-' || COALESCE(JL.JE_LINE_NUM, 0) || '-' || COALESCE(JB.JE_BATCH_ID, 0) || '-' || COALESCE(GCC.CODE_COMBINATION_ID, 0) AS dw_load_id,
    CURRENT_TIMESTAMP() AS dw_insert_date,
    CURRENT_TIMESTAMP() AS dw_update_date
  FROM (
    SELECT
      *
    FROM silver_bec_ods.GL_JE_LINES
    WHERE
      is_deleted_flg <> 'Y'
  ) AS JL, (
    SELECT
      *
    FROM silver_bec_ods.GL_JE_HEADERS
    WHERE
      is_deleted_flg <> 'Y'
  ) AS JH, (
    SELECT
      *
    FROM silver_bec_ods.GL_JE_BATCHES
    WHERE
      is_deleted_flg <> 'Y'
  ) AS JB, (
    SELECT
      *
    FROM silver_bec_ods.GL_CODE_COMBINATIONS
    WHERE
      is_deleted_flg <> 'Y'
  ) AS GCC, (
    SELECT
      *
    FROM silver_bec_ods.GL_LEDGERS
    WHERE
      is_deleted_flg <> 'Y'
  ) AS GL
  WHERE
    1 = 1
    AND GCC.CODE_COMBINATION_ID = JL.CODE_COMBINATION_ID
    AND JH.JE_HEADER_ID = JL.JE_HEADER_ID
    AND JH.JE_BATCH_ID = JB.JE_BATCH_ID
    AND JL.LEDGER_ID = GL.LEDGER_ID
    AND GCC.SUMMARY_FLAG = 'N'
    AND (
      JL.kca_seq_date > (
        SELECT
          (
            executebegints - prune_days
          )
        FROM bec_etl_ctrl.batch_dw_info
        WHERE
          dw_table_name = 'fact_gl_journals' AND batch_name = 'gl'
      )
      OR GCC.kca_seq_date > (
        SELECT
          (
            executebegints - prune_days
          )
        FROM bec_etl_ctrl.batch_dw_info
        WHERE
          dw_table_name = 'fact_gl_journals' AND batch_name = 'gl'
      )
    )
);
UPDATE bec_etl_ctrl.batch_dw_info SET last_refresh_date = CURRENT_TIMESTAMP()
WHERE
  dw_table_name = 'fact_gl_journals' AND batch_name = 'gl';