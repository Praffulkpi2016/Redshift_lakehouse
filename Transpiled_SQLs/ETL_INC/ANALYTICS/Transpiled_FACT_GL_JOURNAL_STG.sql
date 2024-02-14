TRUNCATE table gold_bec_dwh.FACT_GL_JOURNAL_STG;
/* Insert records */
WITH CTE_gl_je_sources_tl AS (
  SELECT
    gjst.user_je_source_name,
    gjst.user_je_source_name AS JE_SOURCE_NAME
  FROM (
    SELECT
      *
    FROM silver_bec_ods.gl_je_sources_tl
    WHERE
      is_deleted_flg <> 'Y'
  ) AS gjst
  WHERE
    gjst.LANGUAGE = 'US'
), CTE_gl_import_references AS (
  SELECT
    gir.je_line_num,
    gir.je_header_id,
    gir.je_batch_id,
    gir.gl_sl_link_id,
    gir.gl_sl_link_table
  FROM (
    SELECT
      *
    FROM silver_bec_ods.gl_import_references
    WHERE
      is_deleted_flg <> 'Y'
  ) AS gir
  WHERE
    (
      gir.kca_seq_date >= (
        SELECT
          (
            executebegints - prune_days
          )
        FROM bec_etl_ctrl.batch_dw_info
        WHERE
          dw_table_name = 'fact_gl_journal_stg' AND batch_name = 'gl'
      )
    )
), CTE_gl_je_lines AS (
  SELECT
    gjl.ledger_id,
    gjl.code_combination_id,
    gjl.effective_date,
    gjl.period_name,
    gjl.je_header_id,
    gjl.je_line_num AS GL_LINE_NUMBER,
    gjl.created_by AS created_by_id
  FROM (
    SELECT
      *
    FROM silver_bec_ods.gl_je_lines
    WHERE
      is_deleted_flg <> 'Y'
  ) AS gjl
  WHERE
    (
      gjl.last_update_date >= TO_DATE('2020-01-01 12:00:00', 'YYYY-MM-DD HH:MI:SS')
      AND gjl.kca_seq_date >= (
        SELECT
          (
            executebegints - prune_days
          )
        FROM bec_etl_ctrl.batch_dw_info
        WHERE
          dw_table_name = 'fact_gl_journal_stg' AND batch_name = 'gl'
      )
    )
), CTE_gl_je_headers AS (
  SELECT
    gjh.je_header_id,
    gjh.period_name,
    gjh.je_source,
    gjh.NAME AS GL_JE_NAME,
    gjh.ACCRUAL_REV_PERIOD_NAME AS REVERSAL_PERIOD,
    gjh.ACCRUAL_REV_STATUS AS REVERSAL_STATUS,
    gjh.external_reference AS EXTERNAL_REFERENCE
  FROM (
    SELECT
      *
    FROM silver_bec_ods.gl_je_headers
    WHERE
      is_deleted_flg <> 'Y'
  ) AS gjh
  WHERE
    gjh.status = 'P'
    AND COALESCE(gjh.je_from_sla_flag, 'N') IN ('Y', 'U')
    AND (
      gjh.last_update_date >= TO_DATE('2020-01-01 12:00:00', 'YYYY-MM-DD HH:MI:SS')
      AND gjh.kca_seq_date >= (
        SELECT
          (
            executebegints - prune_days
          )
        FROM bec_etl_ctrl.batch_dw_info
        WHERE
          dw_table_name = 'fact_gl_journal_stg' AND batch_name = 'gl'
      )
    )
), CTE_gl_je_batches AS (
  SELECT
    gjb.je_batch_id,
    gjb.APPROVER_EMPLOYEE_ID,
    gjb.creation_date,
    gjb.NAME AS GL_BATCH_NAME,
    gjb.posted_date
  FROM (
    SELECT
      *
    FROM silver_bec_ods.gl_je_batches
    WHERE
      is_deleted_flg <> 'Y'
  ) AS gjb
  WHERE
    gjb.status = 'P'
    AND (
      gjb.kca_seq_date >= (
        SELECT
          (
            executebegints - prune_days
          )
        FROM bec_etl_ctrl.batch_dw_info
        WHERE
          dw_table_name = 'fact_gl_journal_stg' AND batch_name = 'gl'
      )
    )
), CTE_FACT_XLA_REPORT_BALANCES_STG AS (
  SELECT
    glbgt.ledger_id,
    glbgt.code_combination_id,
    glbgt.period_start_date AS glbgt_period_start_date,
    glbgt.period_end_date AS glbgt_period_end_date,
    glbgt.period_name,
    glbgt.balance_type_code,
    glbgt.budget_version_id,
    glbgt.encumbrance_type_id,
    glbgt.ledger_short_name AS LEDGER_SHORT_NAME,
    glbgt.ledger_description AS LEDGER_DESCRIPTION,
    glbgt.ledger_name AS LEDGER_NAME,
    glbgt.ledger_currency AS LEDGER_CURRENCY,
    glbgt.period_year AS PERIOD_YEAR,
    glbgt.period_number AS PERIOD_NUMBER,
    glbgt.period_start_date,
    glbgt.period_end_date,
    glbgt.balance_type AS BALANCE_TYPE,
    glbgt.budget_name AS BUDGET_NAME,
    glbgt.encumbrance_type AS ENCUMBRANCE_TYPE,
    glbgt.begin_balance_dr AS BEGIN_BALANCE_DR,
    glbgt.begin_balance_cr AS BEGIN_BALANCE_CR,
    glbgt.period_net_dr AS PERIOD_NET_DR,
    glbgt.period_net_cr AS PERIOD_NET_CR,
    CAST('NA' AS STRING) AS ACCOUNTING_CODE_COMBINATION,
    CAST('NA' AS STRING) AS CODE_COMBINATION_DESCRIPTION,
    glbgt.control_account_flag AS CONTROL_ACCOUNT_FLAG,
    glbgt.control_account AS CONTROL_ACCOUNT,
    CAST('NA' AS STRING) AS BALANCING_SEGMENT,
    CAST('NA' AS STRING) AS NATURAL_ACCOUNT_SEGMENT,
    CAST('NA' AS STRING) AS COST_CENTER_SEGMENT,
    CAST('NA' AS STRING) AS MANAGEMENT_SEGMENT,
    CAST('NA' AS STRING) AS INTERCOMPANY_SEGMENT,
    CAST('NA' AS STRING) AS BALANCING_SEGMENT_DESC,
    CAST('NA' AS STRING) AS NATURAL_ACCOUNT_DESC,
    CAST('NA' AS STRING) AS COST_CENTER_DESC,
    CAST('NA' AS STRING) AS MANAGEMENT_SEGMENT_DESC,
    CAST('NA' AS STRING) AS INTERCOMPANY_SEGMENT_DESC,
    glbgt.segment1 AS SEGMENT1,
    glbgt.segment2 AS SEGMENT2,
    glbgt.segment3 AS SEGMENT3,
    glbgt.segment4 AS SEGMENT4,
    glbgt.segment5 AS SEGMENT5,
    glbgt.segment6 AS SEGMENT6,
    glbgt.segment7 AS SEGMENT7,
    glbgt.segment8 AS SEGMENT8,
    glbgt.segment9 AS SEGMENT9,
    glbgt.segment10 AS SEGMENT10,
    CAST('NA' AS STRING) AS BEGIN_RUNNING_TOTAL_CR,
    CAST('NA' AS STRING) AS BEGIN_RUNNING_TOTAL_DR,
    CAST('NA' AS STRING) AS END_RUNNING_TOTAL_CR,
    CAST('NA' AS STRING) AS END_RUNNING_TOTAL_DR,
    CAST('NA' AS STRING) AS LEGAL_ENTITY_ID,
    CAST('NA' AS STRING) AS LEGAL_ENTITY_NAME,
    CAST('NA' AS STRING) AS LE_ADDRESS_LINE_1,
    CAST('NA' AS STRING) AS LE_ADDRESS_LINE_2,
    CAST('NA' AS STRING) AS LE_ADDRESS_LINE_3,
    CAST('NA' AS STRING) AS LE_CITY,
    CAST('NA' AS STRING) AS LE_REGION_1,
    CAST('NA' AS STRING) AS LE_REGION_2,
    CAST('NA' AS STRING) AS LE_REGION_3,
    CAST('NA' AS STRING) AS LE_POSTAL_CODE,
    CAST('NA' AS STRING) AS LE_COUNTRY,
    CAST('NA' AS STRING) AS LE_REGISTRATION_NUMBER,
    CAST('NA' AS STRING) AS LE_REGISTRATION_EFFECTIVE_FROM,
    CAST('NA' AS STRING) AS LE_BR_DAILY_INSCRIPTION_NUMBER,
    CAST(NULL AS TIMESTAMP) AS LE_BR_DAILY_INSCRIPTION_DATE,
    CAST('NA' AS STRING) AS LE_BR_DAILY_ENTITY,
    CAST('NA' AS STRING) AS LE_BR_DAILY_LOCATION,
    CAST('NA' AS STRING) AS LE_BR_DIRECTOR_NUMBER,
    CAST('NA' AS STRING) AS LE_BR_ACCOUNTANT_NUMBER,
    CAST('NA' AS STRING) AS LE_BR_ACCOUNTANT_NAME,
    glbgt.TRANSLATED_FLAG
  FROM gold_bec_dwh.FACT_XLA_REPORT_BALANCES_STG AS glbgt
), CTE_PER_ALL_PEOPLE_F AS (
  SELECT
    papf.person_id,
    papf.effective_start_date,
    papf.effective_end_date,
    papf.full_name AS approver_name
  FROM (
    SELECT
      *
    FROM silver_bec_ods.PER_ALL_PEOPLE_F
    WHERE
      is_deleted_flg <> 'Y'
  ) AS papf
)
INSERT INTO gold_bec_dwh.FACT_GL_JOURNAL_STG (
  created_by_id,
  JE_SOURCE_NAME,
  GL_BATCH_NAME,
  POSTED_DATE,
  je_header_id,
  GL_JE_NAME,
  REVERSAL_PERIOD,
  REVERSAL_STATUS,
  EXTERNAL_REFERENCE,
  GL_LINE_NUMBER,
  effective_date,
  LEDGER_ID,
  LEDGER_SHORT_NAME,
  LEDGER_DESCRIPTION,
  LEDGER_NAME,
  LEDGER_CURRENCY,
  PERIOD_YEAR,
  PERIOD_NUMBER,
  PERIOD_NAME,
  PERIOD_START_DATE,
  PERIOD_END_DATE,
  BALANCE_TYPE_CODE,
  BALANCE_TYPE,
  BUDGET_NAME,
  ENCUMBRANCE_TYPE,
  BEGIN_BALANCE_DR,
  BEGIN_BALANCE_CR,
  PERIOD_NET_DR,
  PERIOD_NET_CR,
  CODE_COMBINATION_ID,
  ACCOUNTING_CODE_COMBINATION,
  CODE_COMBINATION_DESCRIPTION,
  CONTROL_ACCOUNT_FLAG,
  CONTROL_ACCOUNT,
  BALANCING_SEGMENT,
  NATURAL_ACCOUNT_SEGMENT,
  COST_CENTER_SEGMENT,
  MANAGEMENT_SEGMENT,
  INTERCOMPANY_SEGMENT,
  BALANCING_SEGMENT_DESC,
  NATURAL_ACCOUNT_DESC,
  COST_CENTER_DESC,
  MANAGEMENT_SEGMENT_DESC,
  INTERCOMPANY_SEGMENT_DESC,
  segment1,
  segment2,
  segment3,
  segment4,
  segment5,
  segment6,
  segment7,
  segment8,
  segment9,
  segment10,
  BEGIN_RUNNING_TOTAL_CR,
  BEGIN_RUNNING_TOTAL_DR,
  END_RUNNING_TOTAL_CR,
  END_RUNNING_TOTAL_DR,
  LEGAL_ENTITY_ID,
  LEGAL_ENTITY_NAME,
  LE_ADDRESS_LINE_1,
  LE_ADDRESS_LINE_2,
  LE_ADDRESS_LINE_3,
  LE_CITY,
  LE_REGION_1,
  LE_REGION_2,
  LE_REGION_3,
  LE_POSTAL_CODE,
  LE_COUNTRY,
  LE_REGISTRATION_NUMBER,
  LE_REGISTRATION_EFFECTIVE_FROM,
  LE_BR_DAILY_INSCRIPTION_NUMBER,
  LE_BR_DAILY_INSCRIPTION_DATE,
  LE_BR_DAILY_ENTITY,
  LE_BR_DAILY_LOCATION,
  LE_BR_DIRECTOR_NUMBER,
  LE_BR_ACCOUNTANT_NUMBER,
  LE_BR_ACCOUNTANT_NAME,
  user_je_source_name,
  gl_sl_link_id,
  gl_sl_link_table,
  budget_version_id,
  encumbrance_type_id,
  approver_name,
  TRANSLATED_FLAG,
  is_deleted_flg,
  source_app_id,
  dw_load_id,
  dw_insert_date,
  dw_update_date
)
(
  SELECT
    gjl.created_by_id,
    gjst.JE_SOURCE_NAME,
    gjb.GL_BATCH_NAME,
    gjb.POSTED_DATE,
    gjh.je_header_id,
    gjh.GL_JE_NAME,
    gjh.REVERSAL_PERIOD,
    gjh.REVERSAL_STATUS,
    gjh.EXTERNAL_REFERENCE,
    gjl.GL_LINE_NUMBER,
    gjl.effective_date,
    glbgt.LEDGER_ID,
    glbgt.LEDGER_SHORT_NAME,
    glbgt.LEDGER_DESCRIPTION,
    glbgt.LEDGER_NAME,
    glbgt.LEDGER_CURRENCY,
    glbgt.PERIOD_YEAR,
    glbgt.PERIOD_NUMBER,
    glbgt.PERIOD_NAME,
    glbgt.PERIOD_START_DATE,
    glbgt.PERIOD_END_DATE,
    glbgt.BALANCE_TYPE_CODE,
    glbgt.BALANCE_TYPE,
    glbgt.BUDGET_NAME,
    glbgt.ENCUMBRANCE_TYPE,
    glbgt.BEGIN_BALANCE_DR,
    glbgt.BEGIN_BALANCE_CR,
    glbgt.PERIOD_NET_DR,
    glbgt.PERIOD_NET_CR,
    glbgt.CODE_COMBINATION_ID,
    glbgt.ACCOUNTING_CODE_COMBINATION,
    glbgt.CODE_COMBINATION_DESCRIPTION,
    glbgt.CONTROL_ACCOUNT_FLAG,
    glbgt.CONTROL_ACCOUNT,
    glbgt.BALANCING_SEGMENT,
    glbgt.NATURAL_ACCOUNT_SEGMENT,
    glbgt.COST_CENTER_SEGMENT,
    glbgt.MANAGEMENT_SEGMENT,
    glbgt.INTERCOMPANY_SEGMENT,
    glbgt.BALANCING_SEGMENT_DESC,
    glbgt.NATURAL_ACCOUNT_DESC,
    glbgt.COST_CENTER_DESC,
    glbgt.MANAGEMENT_SEGMENT_DESC,
    glbgt.INTERCOMPANY_SEGMENT_DESC,
    glbgt.segment1 AS SEGMENT1,
    glbgt.segment2 AS SEGMENT2,
    glbgt.segment3 AS SEGMENT3,
    glbgt.segment4 AS SEGMENT4,
    glbgt.segment5 AS SEGMENT5,
    glbgt.segment6 AS SEGMENT6,
    glbgt.segment7 AS SEGMENT7,
    glbgt.segment8 AS SEGMENT8,
    glbgt.segment9 AS SEGMENT9,
    glbgt.segment10 AS SEGMENT10,
    glbgt.BEGIN_RUNNING_TOTAL_CR,
    glbgt.BEGIN_RUNNING_TOTAL_DR,
    glbgt.END_RUNNING_TOTAL_CR,
    glbgt.END_RUNNING_TOTAL_DR,
    glbgt.LEGAL_ENTITY_ID,
    glbgt.LEGAL_ENTITY_NAME,
    glbgt.LE_ADDRESS_LINE_1,
    glbgt.LE_ADDRESS_LINE_2,
    glbgt.LE_ADDRESS_LINE_3,
    glbgt.LE_CITY,
    glbgt.LE_REGION_1,
    glbgt.LE_REGION_2,
    glbgt.LE_REGION_3,
    glbgt.LE_POSTAL_CODE,
    glbgt.LE_COUNTRY,
    glbgt.LE_REGISTRATION_NUMBER,
    glbgt.LE_REGISTRATION_EFFECTIVE_FROM,
    glbgt.LE_BR_DAILY_INSCRIPTION_NUMBER,
    glbgt.LE_BR_DAILY_INSCRIPTION_DATE,
    glbgt.LE_BR_DAILY_ENTITY,
    glbgt.LE_BR_DAILY_LOCATION,
    glbgt.LE_BR_DIRECTOR_NUMBER,
    glbgt.LE_BR_ACCOUNTANT_NUMBER,
    glbgt.LE_BR_ACCOUNTANT_NAME,
    gjst.user_je_source_name,
    gir.gl_sl_link_id,
    gir.gl_sl_link_table,
    glbgt.budget_version_id,
    glbgt.encumbrance_type_id,
    papf.approver_name,
    glbgt.TRANSLATED_FLAG, /* audit columns */
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
    ) || '-' || COALESCE(gir.gl_sl_link_id, 0) || '-' || COALESCE(glbgt.LEDGER_CURRENCY, 'NA') || '-' || COALESCE(glbgt.TRANSLATED_FLAG, 'NA') AS dw_load_id,
    CURRENT_TIMESTAMP() AS dw_insert_date,
    CURRENT_TIMESTAMP() AS dw_update_date
  FROM CTE_gl_je_sources_tl AS gjst, CTE_gl_import_references AS gir, CTE_gl_je_lines AS gjl, CTE_gl_je_headers AS gjh, CTE_gl_je_batches AS gjb, CTE_FACT_XLA_REPORT_BALANCES_STG AS glbgt, CTE_PER_ALL_PEOPLE_F AS papf
  WHERE
    1 = 1
    AND gjl.ledger_id = glbgt.ledger_id
    AND gjl.code_combination_id = glbgt.code_combination_id
    AND gjl.effective_date BETWEEN glbgt.glbgt_period_start_date AND glbgt.glbgt_period_end_date
    AND gjl.period_name = glbgt.period_name
    AND gjl.je_header_id = gjh.je_header_id
    AND gjl.period_name = gjh.period_name
    AND gjl.je_header_id = gir.je_header_id
    AND gjl.GL_LINE_NUMBER = gir.je_line_num
    AND gjh.je_header_id = gir.je_header_id
    AND gjb.je_batch_id = gir.je_batch_id
    AND gjb.APPROVER_EMPLOYEE_ID = papf.PERSON_ID()
    AND gjb.creation_date BETWEEN papf.EFFECTIVE_START_DATE() AND COALESCE(papf.EFFECTIVE_END_DATE(), CURRENT_TIMESTAMP() + 1)
    AND gjst.je_source_name = gjh.je_source
);
UPDATE bec_etl_ctrl.batch_dw_info SET last_refresh_date = CURRENT_TIMESTAMP()
WHERE
  dw_table_name = 'fact_gl_journal_stg' AND batch_name = 'gl';