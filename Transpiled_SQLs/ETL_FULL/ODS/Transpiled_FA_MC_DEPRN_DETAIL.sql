DROP TABLE IF EXISTS silver_bec_ods.FA_MC_DEPRN_DETAIL;
CREATE TABLE IF NOT EXISTS silver_bec_ods.FA_MC_DEPRN_DETAIL (
  set_of_books_id DECIMAL(15, 0),
  book_type_code STRING,
  asset_id DECIMAL(15, 0),
  period_counter DECIMAL(15, 0),
  distribution_id DECIMAL(15, 0),
  deprn_source_code STRING,
  deprn_run_date TIMESTAMP,
  deprn_amount DECIMAL(28, 10),
  ytd_deprn DECIMAL(28, 10),
  deprn_reserve DECIMAL(28, 10),
  addition_cost_to_clear DECIMAL(28, 10),
  cost DECIMAL(28, 10),
  deprn_adjustment_amount DECIMAL(28, 10),
  deprn_expense_je_line_num DECIMAL(15, 0),
  deprn_reserve_je_line_num DECIMAL(15, 0),
  reval_amort_je_line_num DECIMAL(15, 0),
  reval_reserve_je_line_num DECIMAL(15, 0),
  je_header_id DECIMAL(15, 0),
  reval_amortization DECIMAL(28, 10),
  reval_deprn_expense DECIMAL(28, 10),
  reval_reserve DECIMAL(28, 10),
  ytd_reval_deprn_expense DECIMAL(28, 10),
  source_deprn_amount DECIMAL(28, 10),
  source_ytd_deprn DECIMAL(28, 10),
  source_deprn_reserve DECIMAL(28, 10),
  source_addition_cost_to_clear DECIMAL(28, 10),
  source_deprn_adjustment_amount DECIMAL(28, 10),
  source_reval_amortization DECIMAL(28, 10),
  source_reval_deprn_expense DECIMAL(28, 10),
  source_reval_reserve DECIMAL(28, 10),
  source_ytd_reval_deprn_expense DECIMAL(28, 10),
  converted_flag STRING,
  bonus_deprn_amount DECIMAL(28, 10),
  bonus_ytd_deprn DECIMAL(28, 10),
  bonus_deprn_reserve DECIMAL(28, 10),
  bonus_deprn_adjustment_amount DECIMAL(28, 10),
  bonus_deprn_exp_je_line_num DECIMAL(15, 0),
  bonus_deprn_rsv_je_line_num DECIMAL(15, 0),
  deprn_expense_ccid DECIMAL(15, 0),
  deprn_reserve_ccid DECIMAL(15, 0),
  bonus_deprn_expense_ccid DECIMAL(15, 0),
  bonus_deprn_reserve_ccid DECIMAL(15, 0),
  reval_amort_ccid DECIMAL(15, 0),
  reval_reserve_ccid DECIMAL(15, 0),
  event_id DECIMAL(15, 0),
  deprn_run_id DECIMAL(15, 0),
  impairment_amount DECIMAL(28, 10),
  ytd_impairment DECIMAL(28, 10),
  impairment_reserve DECIMAL(15, 0),
  capital_adjustment DECIMAL(28, 10),
  general_fund DECIMAL(28, 10),
  reval_loss_balance DECIMAL(28, 10),
  KCA_OPERATION STRING,
  IS_DELETED_FLG STRING,
  kca_seq_id DECIMAL(36, 0),
  kca_seq_date TIMESTAMP
);
INSERT INTO silver_bec_ods.FA_MC_DEPRN_DETAIL (
  set_of_books_id,
  book_type_code,
  asset_id,
  period_counter,
  distribution_id,
  deprn_source_code,
  deprn_run_date,
  deprn_amount,
  ytd_deprn,
  deprn_reserve,
  addition_cost_to_clear,
  cost,
  deprn_adjustment_amount,
  deprn_expense_je_line_num,
  deprn_reserve_je_line_num,
  reval_amort_je_line_num,
  reval_reserve_je_line_num,
  je_header_id,
  reval_amortization,
  reval_deprn_expense,
  reval_reserve,
  ytd_reval_deprn_expense,
  source_deprn_amount,
  source_ytd_deprn,
  source_deprn_reserve,
  source_addition_cost_to_clear,
  source_deprn_adjustment_amount,
  source_reval_amortization,
  source_reval_deprn_expense,
  source_reval_reserve,
  source_ytd_reval_deprn_expense,
  converted_flag,
  bonus_deprn_amount,
  bonus_ytd_deprn,
  bonus_deprn_reserve,
  bonus_deprn_adjustment_amount,
  bonus_deprn_exp_je_line_num,
  bonus_deprn_rsv_je_line_num,
  deprn_expense_ccid,
  deprn_reserve_ccid,
  bonus_deprn_expense_ccid,
  bonus_deprn_reserve_ccid,
  reval_amort_ccid,
  reval_reserve_ccid,
  event_id,
  deprn_run_id,
  impairment_amount,
  ytd_impairment,
  impairment_reserve,
  capital_adjustment,
  general_fund,
  reval_loss_balance,
  kca_operation,
  IS_DELETED_FLG,
  kca_seq_id,
  kca_seq_date
)
SELECT
  set_of_books_id,
  book_type_code,
  asset_id,
  period_counter,
  distribution_id,
  deprn_source_code,
  deprn_run_date,
  deprn_amount,
  ytd_deprn,
  deprn_reserve,
  addition_cost_to_clear,
  cost,
  deprn_adjustment_amount,
  deprn_expense_je_line_num,
  deprn_reserve_je_line_num,
  reval_amort_je_line_num,
  reval_reserve_je_line_num,
  je_header_id,
  reval_amortization,
  reval_deprn_expense,
  reval_reserve,
  ytd_reval_deprn_expense,
  source_deprn_amount,
  source_ytd_deprn,
  source_deprn_reserve,
  source_addition_cost_to_clear,
  source_deprn_adjustment_amount,
  source_reval_amortization,
  source_reval_deprn_expense,
  source_reval_reserve,
  source_ytd_reval_deprn_expense,
  converted_flag,
  bonus_deprn_amount,
  bonus_ytd_deprn,
  bonus_deprn_reserve,
  bonus_deprn_adjustment_amount,
  bonus_deprn_exp_je_line_num,
  bonus_deprn_rsv_je_line_num,
  deprn_expense_ccid,
  deprn_reserve_ccid,
  bonus_deprn_expense_ccid,
  bonus_deprn_reserve_ccid,
  reval_amort_ccid,
  reval_reserve_ccid,
  event_id,
  deprn_run_id,
  impairment_amount,
  ytd_impairment,
  impairment_reserve,
  capital_adjustment,
  general_fund,
  reval_loss_balance,
  KCA_OPERATION,
  'N' AS IS_DELETED_FLG,
  CAST(NULLIF(KCA_SEQ_ID, '') AS DECIMAL(36, 0)) AS KCA_SEQ_ID,
  kca_seq_date
FROM bronze_bec_ods_stg.FA_MC_DEPRN_DETAIL;
UPDATE bec_etl_ctrl.batch_ods_info SET load_type = 'I', last_refresh_date = CURRENT_TIMESTAMP()
WHERE
  ods_table_name = 'fa_mc_deprn_detail';