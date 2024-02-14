TRUNCATE table gold_bec_dwh_rpt.fact_journal_entries_rt;
INSERT INTO gold_bec_dwh_rpt.fact_journal_entries_rt
(
  SELECT
    gj.je_header_id,
    gj.period_name,
    gj.JOURNAL_SOURCE,
    gj.je_line_num,
    gj.project_number,
    gj.task_number,
    NEXT_DAY(gj.effective_date, 'SUNDAY') AS expnd_ending_date,
    gj.effective_date,
    gj.expnd_type,
    gj.orig_transaction_reference AS original_trans_ref,
    CASE WHEN gj.accounted_dr IS NULL THEN gj.accounted_cr * -1 ELSE gj.accounted_dr END AS quantity,
    NULL AS expnd_type_class,
    NULL AS business_group,
    NULL AS employee_number,
    'SUNNVYVALE OU' AS organization_name,
    NULL AS trans_curr,
    CASE WHEN gj.accounted_dr IS NULL THEN gj.accounted_cr * -1 ELSE gj.accounted_dr END AS trans_raw_cost,
    NULL AS trans_burdened_cost,
    CASE WHEN gj.accounted_dr IS NULL THEN gj.accounted_cr * -1 ELSE gj.accounted_dr END AS functional_raw_cost,
    gj.effective_date AS gl_date,
    gj.code_combination_id AS debit_acct_ccid,
    gj.JOURNAL_LINE_STATUS,
    gj.company,
    gj.company || '.' || gj.department || '.' || gj.Account || '.' || gj.intercompany || '.' || gj.budget_id || '.' || gj.LOCATION AS `Debit Account`,
    gj.code_combination_id AS credit_acct_ccid,
    CASE WHEN gj.accounted_dr IS NULL THEN 'Y' END AS negative_txn_flag
  FROM gold_bec_dwh.FACT_GL_JOURNALS AS gj
  WHERE
    NOT gj.orig_transaction_reference IN (
      SELECT
        dei.orig_transaction_reference
      FROM bec_Dwh.dim_expenditure_items AS dei
      WHERE
        dei.transaction_source = 'BE Project Journal'
    )
    AND NOT gj.posted_date IS NULL
);
UPDATE bec_etl_ctrl.batch_dw_info SET load_type = 'I', last_refresh_date = CURRENT_TIMESTAMP()
WHERE
  dw_table_name = 'fact_journal_entries_rt' AND batch_name = 'gl';