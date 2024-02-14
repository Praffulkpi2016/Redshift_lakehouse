TRUNCATE table
	table bronze_bec_ods_stg.FA_MC_DEPRN_DETAIL;
INSERT INTO bronze_bec_ods_stg.FA_MC_DEPRN_DETAIL (
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
  kca_seq_id,
  kca_seq_date
)
(
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
    kca_seq_id,
    kca_seq_date
  FROM bec_raw_dl_ext.FA_MC_DEPRN_DETAIL
  WHERE
    kca_operation <> 'DELETE'
    AND COALESCE(kca_seq_id, '') <> ''
    AND (COALESCE(BOOK_TYPE_CODE, 'NA'), COALESCE(ASSET_ID, 0), COALESCE(PERIOD_COUNTER, 0), COALESCE(DISTRIBUTION_ID, 0), COALESCE(SET_OF_BOOKS_ID, 0), KCA_SEQ_ID) IN (
      SELECT
        COALESCE(BOOK_TYPE_CODE, 'NA') AS BOOK_TYPE_CODE,
        COALESCE(ASSET_ID, 0) AS ASSET_ID,
        COALESCE(PERIOD_COUNTER, 0) AS PERIOD_COUNTER,
        COALESCE(DISTRIBUTION_ID, 0) AS DISTRIBUTION_ID,
        COALESCE(SET_OF_BOOKS_ID, 0) AS SET_OF_BOOKS_ID,
        MAX(kca_seq_id)
      FROM bec_raw_dl_ext.FA_MC_DEPRN_DETAIL
      WHERE
        kca_operation <> 'DELETE' AND COALESCE(kca_seq_id, '') <> ''
      GROUP BY
        COALESCE(BOOK_TYPE_CODE, 'NA'),
        COALESCE(ASSET_ID, 0),
        COALESCE(PERIOD_COUNTER, 0),
        COALESCE(DISTRIBUTION_ID, 0),
        COALESCE(SET_OF_BOOKS_ID, 0)
    )
    AND kca_seq_date > (
      SELECT
        (
          executebegints - prune_days
        )
      FROM bec_etl_ctrl.batch_ods_info
      WHERE
        ods_table_name = 'fa_mc_deprn_detail'
    )
);