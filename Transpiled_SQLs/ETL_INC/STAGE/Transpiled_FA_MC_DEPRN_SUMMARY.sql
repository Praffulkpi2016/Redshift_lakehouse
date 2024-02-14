TRUNCATE table
	table bronze_bec_ods_stg.FA_MC_DEPRN_SUMMARY;
INSERT INTO bronze_bec_ods_stg.FA_MC_DEPRN_SUMMARY (
  set_of_books_id,
  book_type_code,
  asset_id,
  deprn_run_date,
  deprn_amount,
  ytd_deprn,
  deprn_reserve,
  deprn_source_code,
  adjusted_cost,
  bonus_rate,
  ltd_production,
  period_counter,
  production,
  reval_amortization,
  reval_amortization_basis,
  reval_deprn_expense,
  reval_reserve,
  ytd_production,
  ytd_reval_deprn_expense,
  prior_fy_expense,
  converted_flag,
  bonus_deprn_amount,
  bonus_ytd_deprn,
  bonus_deprn_reserve,
  prior_fy_bonus_expense,
  deprn_override_flag,
  system_deprn_amount,
  system_bonus_deprn_amount,
  event_id,
  deprn_run_id,
  deprn_adjustment_amount,
  bonus_deprn_adjustment_amount,
  impairment_amount,
  ytd_impairment,
  impairment_reserve,
  capital_adjustment,
  general_fund,
  reval_loss_balance,
  unrevalued_cost,
  historical_nbv,
  KCA_OPERATION,
  kca_seq_id,
  kca_seq_date
)
(
  SELECT
    set_of_books_id,
    book_type_code,
    asset_id,
    deprn_run_date,
    deprn_amount,
    ytd_deprn,
    deprn_reserve,
    deprn_source_code,
    adjusted_cost,
    bonus_rate,
    ltd_production,
    period_counter,
    production,
    reval_amortization,
    reval_amortization_basis,
    reval_deprn_expense,
    reval_reserve,
    ytd_production,
    ytd_reval_deprn_expense,
    prior_fy_expense,
    converted_flag,
    bonus_deprn_amount,
    bonus_ytd_deprn,
    bonus_deprn_reserve,
    prior_fy_bonus_expense,
    deprn_override_flag,
    system_deprn_amount,
    system_bonus_deprn_amount,
    event_id,
    deprn_run_id,
    deprn_adjustment_amount,
    bonus_deprn_adjustment_amount,
    impairment_amount,
    ytd_impairment,
    impairment_reserve,
    capital_adjustment,
    general_fund,
    reval_loss_balance,
    unrevalued_cost,
    historical_nbv,
    KCA_OPERATION,
    kca_seq_id,
    kca_seq_date
  FROM bec_raw_dl_ext.FA_MC_DEPRN_SUMMARY
  WHERE
    kca_operation <> 'DELETE'
    AND COALESCE(kca_seq_id, '') <> ''
    AND (COALESCE(BOOK_TYPE_CODE, 'NA'), COALESCE(ASSET_ID, 0), COALESCE(PERIOD_COUNTER, 0), COALESCE(SET_OF_BOOKS_ID, 0), KCA_SEQ_ID) IN (
      SELECT
        COALESCE(BOOK_TYPE_CODE, 'NA') AS BOOK_TYPE_CODE,
        COALESCE(ASSET_ID, 0) AS ASSET_ID,
        COALESCE(PERIOD_COUNTER, 0) AS PERIOD_COUNTER,
        COALESCE(SET_OF_BOOKS_ID, 0) AS SET_OF_BOOKS_ID,
        MAX(kca_seq_id)
      FROM bec_raw_dl_ext.FA_MC_DEPRN_SUMMARY
      WHERE
        kca_operation <> 'DELETE' AND COALESCE(kca_seq_id, '') <> ''
      GROUP BY
        COALESCE(BOOK_TYPE_CODE, 'NA'),
        COALESCE(ASSET_ID, 0),
        COALESCE(PERIOD_COUNTER, 0),
        COALESCE(SET_OF_BOOKS_ID, 0)
    )
    AND kca_seq_date > (
      SELECT
        (
          executebegints - prune_days
        )
      FROM bec_etl_ctrl.batch_ods_info
      WHERE
        ods_table_name = 'fa_mc_deprn_summary'
    )
);