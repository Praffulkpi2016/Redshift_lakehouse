/* Delete Records */
DELETE FROM silver_bec_ods.FA_MC_DEPRN_SUMMARY
WHERE
  (COALESCE(BOOK_TYPE_CODE, 'NA'), COALESCE(ASSET_ID, 0), COALESCE(PERIOD_COUNTER, 0), COALESCE(SET_OF_BOOKS_ID, 0)) IN (
    SELECT
      COALESCE(stg.BOOK_TYPE_CODE, 'NA') AS BOOK_TYPE_CODE,
      COALESCE(stg.ASSET_ID, 0) AS ASSET_ID,
      COALESCE(stg.PERIOD_COUNTER, 0) AS PERIOD_COUNTER,
      COALESCE(stg.SET_OF_BOOKS_ID, 0) AS SET_OF_BOOKS_ID
    FROM silver_bec_ods.FA_MC_DEPRN_SUMMARY AS ods, bronze_bec_ods_stg.FA_MC_DEPRN_SUMMARY AS stg
    WHERE
      COALESCE(ods.BOOK_TYPE_CODE, 'NA') = COALESCE(stg.BOOK_TYPE_CODE, 'NA')
      AND COALESCE(ods.ASSET_ID, 0) = COALESCE(stg.ASSET_ID, 0)
      AND COALESCE(ods.PERIOD_COUNTER, 0) = COALESCE(stg.PERIOD_COUNTER, 0)
      AND COALESCE(ods.SET_OF_BOOKS_ID, 0) = COALESCE(stg.SET_OF_BOOKS_ID, 0)
      AND stg.kca_operation IN ('INSERT', 'UPDATE')
  );
/* Insert records */
INSERT INTO silver_bec_ods.FA_MC_DEPRN_SUMMARY (
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
    kca_operation,
    'N' AS IS_DELETED_FLG,
    CAST(NULLIF(KCA_SEQ_ID, '') AS DECIMAL(36, 0)) AS KCA_SEQ_ID,
    kca_seq_date
  FROM bronze_bec_ods_stg.FA_MC_DEPRN_SUMMARY
  WHERE
    kca_operation IN ('INSERT', 'UPDATE')
    AND (COALESCE(BOOK_TYPE_CODE, 'NA'), COALESCE(ASSET_ID, 0), COALESCE(PERIOD_COUNTER, 0), COALESCE(SET_OF_BOOKS_ID, 0), KCA_SEQ_ID) IN (
      SELECT
        COALESCE(BOOK_TYPE_CODE, 'NA') AS BOOK_TYPE_CODE,
        COALESCE(ASSET_ID, 0) AS ASSET_ID,
        COALESCE(PERIOD_COUNTER, 0) AS PERIOD_COUNTER,
        COALESCE(SET_OF_BOOKS_ID, 0) AS SET_OF_BOOKS_ID,
        MAX(KCA_SEQ_ID)
      FROM bronze_bec_ods_stg.FA_MC_DEPRN_SUMMARY
      WHERE
        kca_operation IN ('INSERT', 'UPDATE')
      GROUP BY
        COALESCE(BOOK_TYPE_CODE, 'NA'),
        COALESCE(ASSET_ID, 0),
        COALESCE(PERIOD_COUNTER, 0),
        COALESCE(SET_OF_BOOKS_ID, 0)
    )
);
/* Soft delete */
UPDATE silver_bec_ods.FA_MC_DEPRN_SUMMARY SET IS_DELETED_FLG = 'N';
UPDATE silver_bec_ods.FA_MC_DEPRN_SUMMARY SET IS_DELETED_FLG = 'Y'
WHERE
  (COALESCE(BOOK_TYPE_CODE, 'NA'), COALESCE(ASSET_ID, 0), COALESCE(PERIOD_COUNTER, 0), COALESCE(SET_OF_BOOKS_ID, 0)) IN (
    SELECT
      COALESCE(BOOK_TYPE_CODE, 'NA'),
      COALESCE(ASSET_ID, 0),
      COALESCE(PERIOD_COUNTER, 0),
      COALESCE(SET_OF_BOOKS_ID, 0)
    FROM bec_raw_dl_ext.FA_MC_DEPRN_SUMMARY
    WHERE
      (COALESCE(BOOK_TYPE_CODE, 'NA'), COALESCE(ASSET_ID, 0), COALESCE(PERIOD_COUNTER, 0), COALESCE(SET_OF_BOOKS_ID, 0), KCA_SEQ_ID) IN (
        SELECT
          COALESCE(BOOK_TYPE_CODE, 'NA'),
          COALESCE(ASSET_ID, 0),
          COALESCE(PERIOD_COUNTER, 0),
          COALESCE(SET_OF_BOOKS_ID, 0),
          MAX(KCA_SEQ_ID) AS KCA_SEQ_ID
        FROM bec_raw_dl_ext.FA_MC_DEPRN_SUMMARY
        GROUP BY
          COALESCE(BOOK_TYPE_CODE, 'NA'),
          COALESCE(ASSET_ID, 0),
          COALESCE(PERIOD_COUNTER, 0),
          COALESCE(SET_OF_BOOKS_ID, 0)
      )
      AND kca_operation = 'DELETE'
  );
UPDATE bec_etl_ctrl.batch_ods_info SET last_refresh_date = CURRENT_TIMESTAMP()
WHERE
  ods_table_name = 'fa_mc_deprn_summary';