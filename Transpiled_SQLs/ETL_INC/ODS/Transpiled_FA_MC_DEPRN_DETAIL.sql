/* Delete Records */
DELETE FROM silver_bec_ods.FA_MC_DEPRN_DETAIL
WHERE
  (COALESCE(BOOK_TYPE_CODE, 'NA'), COALESCE(ASSET_ID, 0), COALESCE(PERIOD_COUNTER, 0), COALESCE(DISTRIBUTION_ID, 0), COALESCE(SET_OF_BOOKS_ID, 0)) IN (
    SELECT
      COALESCE(stg.BOOK_TYPE_CODE, 'NA') AS BOOK_TYPE_CODE,
      COALESCE(stg.ASSET_ID, 0) AS ASSET_ID,
      COALESCE(stg.PERIOD_COUNTER, 0) AS PERIOD_COUNTER,
      COALESCE(stg.DISTRIBUTION_ID, 0) AS DISTRIBUTION_ID,
      COALESCE(stg.SET_OF_BOOKS_ID, 0) AS SET_OF_BOOKS_ID
    FROM silver_bec_ods.FA_MC_DEPRN_DETAIL AS ods, bronze_bec_ods_stg.FA_MC_DEPRN_DETAIL AS stg
    WHERE
      COALESCE(ods.BOOK_TYPE_CODE, 'NA') = COALESCE(stg.BOOK_TYPE_CODE, 'NA')
      AND COALESCE(ods.ASSET_ID, 0) = COALESCE(stg.ASSET_ID, 0)
      AND COALESCE(ods.PERIOD_COUNTER, 0) = COALESCE(stg.PERIOD_COUNTER, 0)
      AND COALESCE(ods.DISTRIBUTION_ID, 0) = COALESCE(stg.DISTRIBUTION_ID, 0)
      AND COALESCE(ods.SET_OF_BOOKS_ID, 0) = COALESCE(stg.SET_OF_BOOKS_ID, 0)
      AND stg.kca_operation IN ('INSERT', 'UPDATE')
  );
/* Insert records */
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
    kca_operation,
    'N' AS IS_DELETED_FLG,
    CAST(NULLIF(KCA_SEQ_ID, '') AS DECIMAL(36, 0)) AS KCA_SEQ_ID,
    kca_seq_date
  FROM bronze_bec_ods_stg.FA_MC_DEPRN_DETAIL
  WHERE
    kca_operation IN ('INSERT', 'UPDATE')
    AND (COALESCE(BOOK_TYPE_CODE, 'NA'), COALESCE(ASSET_ID, 0), COALESCE(PERIOD_COUNTER, 0), COALESCE(DISTRIBUTION_ID, 0), COALESCE(SET_OF_BOOKS_ID, 0), KCA_SEQ_ID) IN (
      SELECT
        COALESCE(BOOK_TYPE_CODE, 'NA') AS BOOK_TYPE_CODE,
        COALESCE(ASSET_ID, 0) AS ASSET_ID,
        COALESCE(PERIOD_COUNTER, 0) AS PERIOD_COUNTER,
        COALESCE(DISTRIBUTION_ID, 0) AS DISTRIBUTION_ID,
        COALESCE(SET_OF_BOOKS_ID, 0) AS SET_OF_BOOKS_ID,
        MAX(KCA_SEQ_ID)
      FROM bronze_bec_ods_stg.FA_MC_DEPRN_DETAIL
      WHERE
        kca_operation IN ('INSERT', 'UPDATE')
      GROUP BY
        COALESCE(BOOK_TYPE_CODE, 'NA'),
        COALESCE(ASSET_ID, 0),
        COALESCE(PERIOD_COUNTER, 0),
        COALESCE(DISTRIBUTION_ID, 0),
        COALESCE(SET_OF_BOOKS_ID, 0)
    )
);
/* Soft delete */
UPDATE silver_bec_ods.FA_MC_DEPRN_DETAIL SET IS_DELETED_FLG = 'N';
UPDATE silver_bec_ods.FA_MC_DEPRN_DETAIL SET IS_DELETED_FLG = 'Y'
WHERE
  (COALESCE(BOOK_TYPE_CODE, 'NA'), COALESCE(ASSET_ID, 0), COALESCE(PERIOD_COUNTER, 0), COALESCE(DISTRIBUTION_ID, 0), COALESCE(SET_OF_BOOKS_ID, 0)) IN (
    SELECT
      COALESCE(BOOK_TYPE_CODE, 'NA'),
      COALESCE(ASSET_ID, 0),
      COALESCE(PERIOD_COUNTER, 0),
      COALESCE(DISTRIBUTION_ID, 0),
      COALESCE(SET_OF_BOOKS_ID, 0)
    FROM bec_raw_dl_ext.FA_MC_DEPRN_DETAIL
    WHERE
      (COALESCE(BOOK_TYPE_CODE, 'NA'), COALESCE(ASSET_ID, 0), COALESCE(PERIOD_COUNTER, 0), COALESCE(DISTRIBUTION_ID, 0), COALESCE(SET_OF_BOOKS_ID, 0), KCA_SEQ_ID) IN (
        SELECT
          COALESCE(BOOK_TYPE_CODE, 'NA'),
          COALESCE(ASSET_ID, 0),
          COALESCE(PERIOD_COUNTER, 0),
          COALESCE(DISTRIBUTION_ID, 0),
          COALESCE(SET_OF_BOOKS_ID, 0),
          MAX(KCA_SEQ_ID) AS KCA_SEQ_ID
        FROM bec_raw_dl_ext.FA_MC_DEPRN_DETAIL
        GROUP BY
          COALESCE(BOOK_TYPE_CODE, 'NA'),
          COALESCE(ASSET_ID, 0),
          COALESCE(PERIOD_COUNTER, 0),
          COALESCE(DISTRIBUTION_ID, 0),
          COALESCE(SET_OF_BOOKS_ID, 0)
      )
      AND kca_operation = 'DELETE'
  );
UPDATE bec_etl_ctrl.batch_ods_info SET last_refresh_date = CURRENT_TIMESTAMP()
WHERE
  ods_table_name = 'fa_mc_deprn_detail';