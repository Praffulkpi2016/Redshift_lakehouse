/* Delete Records */
DELETE FROM silver_bec_ods.GL_BALANCES
WHERE
  (LEDGER_ID, CODE_COMBINATION_ID, CURRENCY_CODE, PERIOD_NAME, ACTUAL_FLAG, COALESCE(BUDGET_VERSION_ID, 0), COALESCE(ENCUMBRANCE_TYPE_ID, 0), COALESCE(TRANSLATED_FLAG, '0')) IN (
    SELECT
      stg.LEDGER_ID,
      stg.CODE_COMBINATION_ID,
      stg.CURRENCY_CODE,
      stg.PERIOD_NAME,
      stg.ACTUAL_FLAG,
      COALESCE(stg.BUDGET_VERSION_ID, 0) AS BUDGET_VERSION_ID,
      COALESCE(stg.ENCUMBRANCE_TYPE_ID, 0) AS ENCUMBRANCE_TYPE_ID,
      COALESCE(stg.TRANSLATED_FLAG, '0') AS TRANSLATED_FLAG
    FROM silver_bec_ods.GL_BALANCES AS ods, bronze_bec_ods_stg.GL_BALANCES AS stg
    WHERE
      ods.LEDGER_ID = stg.LEDGER_ID
      AND ods.CODE_COMBINATION_ID = stg.CODE_COMBINATION_ID
      AND ods.CURRENCY_CODE = stg.CURRENCY_CODE
      AND ods.PERIOD_NAME = stg.PERIOD_NAME
      AND ods.ACTUAL_FLAG = stg.ACTUAL_FLAG
      AND COALESCE(ods.BUDGET_VERSION_ID, 0) = COALESCE(stg.BUDGET_VERSION_ID, 0)
      AND COALESCE(ods.ENCUMBRANCE_TYPE_ID, 0) = COALESCE(stg.ENCUMBRANCE_TYPE_ID, 0)
      AND COALESCE(ods.TRANSLATED_FLAG, '0') = COALESCE(stg.TRANSLATED_FLAG, '0')
      AND stg.kca_operation IN ('INSERT', 'UPDATE')
  );
/* Insert records */
INSERT INTO silver_bec_ods.GL_BALANCES (
  ledger_id,
  code_combination_id,
  currency_code,
  period_name,
  actual_flag,
  last_update_date,
  last_updated_by,
  budget_version_id,
  encumbrance_type_id,
  translated_flag,
  revaluation_status,
  period_type,
  period_year,
  period_num,
  period_net_dr,
  period_net_cr,
  period_to_date_adb,
  quarter_to_date_dr,
  quarter_to_date_cr,
  quarter_to_date_adb,
  year_to_date_adb,
  project_to_date_dr,
  project_to_date_cr,
  project_to_date_adb,
  begin_balance_dr,
  begin_balance_cr,
  period_net_dr_beq,
  period_net_cr_beq,
  begin_balance_dr_beq,
  begin_balance_cr_beq,
  template_id,
  encumbrance_doc_id,
  encumbrance_line_num,
  quarter_to_date_dr_beq,
  quarter_to_date_cr_beq,
  project_to_date_dr_beq,
  project_to_date_cr_beq,
  KCA_OPERATION,
  IS_DELETED_FLG,
  kca_seq_id,
  kca_seq_date
)
(
  SELECT
    ledger_id,
    code_combination_id,
    currency_code,
    period_name,
    actual_flag,
    last_update_date,
    last_updated_by,
    budget_version_id,
    encumbrance_type_id,
    translated_flag,
    revaluation_status,
    period_type,
    period_year,
    period_num,
    period_net_dr,
    period_net_cr,
    period_to_date_adb,
    quarter_to_date_dr,
    quarter_to_date_cr,
    quarter_to_date_adb,
    year_to_date_adb,
    project_to_date_dr,
    project_to_date_cr,
    project_to_date_adb,
    begin_balance_dr,
    begin_balance_cr,
    period_net_dr_beq,
    period_net_cr_beq,
    begin_balance_dr_beq,
    begin_balance_cr_beq,
    template_id,
    encumbrance_doc_id,
    encumbrance_line_num,
    quarter_to_date_dr_beq,
    quarter_to_date_cr_beq,
    project_to_date_dr_beq,
    project_to_date_cr_beq,
    KCA_OPERATION,
    'N' AS IS_DELETED_FLG,
    CAST(NULLIF(KCA_SEQ_ID, '') AS DECIMAL(36, 0)) AS KCA_SEQ_ID,
    kca_seq_date
  FROM bronze_bec_ods_stg.GL_BALANCES
  WHERE
    kca_operation IN ('INSERT', 'UPDATE')
    AND (LEDGER_ID, CODE_COMBINATION_ID, CURRENCY_CODE, PERIOD_NAME, ACTUAL_FLAG, COALESCE(BUDGET_VERSION_ID, 0), COALESCE(ENCUMBRANCE_TYPE_ID, 0), COALESCE(TRANSLATED_FLAG, '0'), KCA_SEQ_ID) IN (
      SELECT
        LEDGER_ID,
        CODE_COMBINATION_ID,
        CURRENCY_CODE,
        PERIOD_NAME,
        ACTUAL_FLAG,
        COALESCE(BUDGET_VERSION_ID, 0) AS BUDGET_VERSION_ID,
        COALESCE(ENCUMBRANCE_TYPE_ID, 0) AS ENCUMBRANCE_TYPE_ID,
        COALESCE(TRANSLATED_FLAG, '0') AS TRANSLATED_FLAG,
        MAX(KCA_SEQ_ID)
      FROM bronze_bec_ods_stg.GL_BALANCES
      WHERE
        kca_operation IN ('INSERT', 'UPDATE')
      GROUP BY
        LEDGER_ID,
        CODE_COMBINATION_ID,
        CURRENCY_CODE,
        PERIOD_NAME,
        ACTUAL_FLAG,
        COALESCE(BUDGET_VERSION_ID, 0),
        COALESCE(ENCUMBRANCE_TYPE_ID, 0),
        COALESCE(TRANSLATED_FLAG, '0')
    )
);
/* Soft delete */
UPDATE silver_bec_ods.GL_BALANCES SET IS_DELETED_FLG = 'N';
UPDATE silver_bec_ods.GL_BALANCES SET IS_DELETED_FLG = 'Y'
WHERE
  (LEDGER_ID, CODE_COMBINATION_ID, CURRENCY_CODE, PERIOD_NAME, ACTUAL_FLAG, COALESCE(BUDGET_VERSION_ID, 0), COALESCE(ENCUMBRANCE_TYPE_ID, 0), COALESCE(TRANSLATED_FLAG, '0')) IN (
    SELECT
      LEDGER_ID,
      CODE_COMBINATION_ID,
      CURRENCY_CODE,
      PERIOD_NAME,
      ACTUAL_FLAG,
      COALESCE(BUDGET_VERSION_ID, 0),
      COALESCE(ENCUMBRANCE_TYPE_ID, 0),
      COALESCE(TRANSLATED_FLAG, '0')
    FROM bec_raw_dl_ext.GL_BALANCES
    WHERE
      (LEDGER_ID, CODE_COMBINATION_ID, CURRENCY_CODE, PERIOD_NAME, ACTUAL_FLAG, COALESCE(BUDGET_VERSION_ID, 0), COALESCE(ENCUMBRANCE_TYPE_ID, 0), COALESCE(TRANSLATED_FLAG, '0'), KCA_SEQ_ID) IN (
        SELECT
          LEDGER_ID,
          CODE_COMBINATION_ID,
          CURRENCY_CODE,
          PERIOD_NAME,
          ACTUAL_FLAG,
          COALESCE(BUDGET_VERSION_ID, 0),
          COALESCE(ENCUMBRANCE_TYPE_ID, 0),
          COALESCE(TRANSLATED_FLAG, '0'),
          MAX(KCA_SEQ_ID) AS KCA_SEQ_ID
        FROM bec_raw_dl_ext.GL_BALANCES
        GROUP BY
          LEDGER_ID,
          CODE_COMBINATION_ID,
          CURRENCY_CODE,
          PERIOD_NAME,
          ACTUAL_FLAG,
          COALESCE(BUDGET_VERSION_ID, 0),
          COALESCE(ENCUMBRANCE_TYPE_ID, 0),
          COALESCE(TRANSLATED_FLAG, '0')
      )
      AND kca_operation = 'DELETE'
  );
UPDATE bec_etl_ctrl.batch_ods_info SET last_refresh_date = CURRENT_TIMESTAMP()
WHERE
  ods_table_name = 'gl_balances';