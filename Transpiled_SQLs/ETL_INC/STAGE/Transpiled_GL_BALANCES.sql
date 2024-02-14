TRUNCATE table bronze_bec_ods_stg.GL_BALANCES;
INSERT INTO bronze_bec_ods_stg.GL_BALANCES (
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
  KCA_SEQ_ID,
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
    KCA_SEQ_ID,
    kca_seq_date
  FROM bec_raw_dl_ext.GL_BALANCES
  WHERE
    kca_operation <> 'DELETE'
    AND COALESCE(kca_seq_id, '') <> ''
    AND (LEDGER_ID, CODE_COMBINATION_ID, CURRENCY_CODE, PERIOD_NAME, ACTUAL_FLAG, COALESCE(BUDGET_VERSION_ID, 0), COALESCE(ENCUMBRANCE_TYPE_ID, 0), COALESCE(TRANSLATED_FLAG, '0'), KCA_SEQ_ID) IN (
      SELECT
        LEDGER_ID,
        CODE_COMBINATION_ID,
        CURRENCY_CODE,
        PERIOD_NAME,
        ACTUAL_FLAG,
        COALESCE(BUDGET_VERSION_ID, 0),
        COALESCE(ENCUMBRANCE_TYPE_ID, 0),
        COALESCE(TRANSLATED_FLAG, '0'),
        MAX(KCA_SEQ_ID)
      FROM bec_raw_dl_ext.GL_BALANCES
      WHERE
        kca_operation <> 'DELETE' AND COALESCE(kca_seq_id, '') <> ''
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
    AND (
      kca_seq_date > (
        SELECT
          (
            executebegints - prune_days
          )
        FROM bec_etl_ctrl.batch_ods_info
        WHERE
          ods_table_name = 'gl_balances'
      )
    )
);