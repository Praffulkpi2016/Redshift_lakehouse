/* Delete Records */
DELETE FROM silver_bec_ods.FA_MC_BOOKS
WHERE
  (COALESCE(SET_OF_BOOKS_ID, 0), COALESCE(TRANSACTION_HEADER_ID_IN, 0)) IN (
    SELECT
      COALESCE(stg.SET_OF_BOOKS_ID, 0) AS SET_OF_BOOKS_ID,
      COALESCE(stg.TRANSACTION_HEADER_ID_IN, 0) AS TRANSACTION_HEADER_ID_IN
    FROM silver_bec_ods.FA_MC_BOOKS AS ods, bronze_bec_ods_stg.FA_MC_BOOKS AS stg
    WHERE
      COALESCE(ods.SET_OF_BOOKS_ID, 0) = COALESCE(stg.SET_OF_BOOKS_ID, 0)
      AND COALESCE(ods.TRANSACTION_HEADER_ID_IN, 0) = COALESCE(stg.TRANSACTION_HEADER_ID_IN, 0)
      AND stg.kca_operation IN ('INSERT', 'UPDATE')
  );
/* Insert records */
INSERT INTO silver_bec_ods.FA_MC_BOOKS (
  set_of_books_id,
  asset_id,
  book_type_code,
  transaction_header_id_in,
  transaction_header_id_out,
  adjusted_cost,
  cost,
  source_cost,
  original_cost,
  source_original_cost,
  salvage_value,
  adjustment_required_status,
  retirement_pending_flag,
  last_update_date,
  last_updated_by,
  itc_amount,
  itc_basis,
  recoverable_cost,
  last_update_login,
  reval_ceiling,
  period_counter_fully_reserved,
  unrevalued_cost,
  allowed_deprn_limit_amount,
  period_counter_life_complete,
  adjusted_recoverable_cost,
  converted_flag,
  annual_deprn_rounding_flag,
  eofy_adj_cost,
  old_adjusted_cost,
  eofy_formula_factor,
  formula_factor,
  remaining_life1,
  remaining_life2,
  short_fiscal_year_flag,
  group_asset_id,
  reval_amortization_basis,
  itc_amount_id,
  retirement_id,
  tax_request_id,
  basic_rate,
  adjusted_rate,
  bonus_rule,
  ceiling_name,
  adjusted_capacity,
  fully_rsvd_revals_counter,
  idled_flag,
  period_counter_capitalized,
  period_counter_fully_retired,
  production_capacity,
  unit_of_measure,
  percent_salvage_value,
  allowed_deprn_limit,
  annual_rounding_flag,
  global_attribute1,
  global_attribute2,
  global_attribute3,
  global_attribute4,
  global_attribute5,
  global_attribute6,
  global_attribute7,
  global_attribute8,
  global_attribute9,
  global_attribute10,
  global_attribute11,
  global_attribute12,
  global_attribute13,
  global_attribute14,
  global_attribute15,
  global_attribute16,
  global_attribute17,
  global_attribute18,
  global_attribute19,
  global_attribute20,
  global_attribute_category,
  date_placed_in_service,
  date_effective,
  deprn_start_date,
  deprn_method_code,
  life_in_months,
  rate_adjustment_factor,
  prorate_convention_code,
  prorate_date,
  cost_change_flag,
  capitalize_flag,
  depreciate_flag,
  date_ineffective,
  conversion_date,
  original_deprn_start_date,
  salvage_type,
  deprn_limit_type,
  reduction_rate,
  reduce_addition_flag,
  reduce_adjustment_flag,
  reduce_retirement_flag,
  recognize_gain_loss,
  recapture_reserve_flag,
  limit_proceeds_flag,
  terminal_gain_loss,
  tracking_method,
  exclude_fully_rsv_flag,
  excess_allocation_option,
  depreciation_option,
  member_rollup_flag,
  allocate_to_fully_rsv_flag,
  allocate_to_fully_ret_flag,
  terminal_gain_loss_amount,
  cip_cost,
  ytd_proceeds,
  ltd_proceeds,
  ltd_cost_of_removal,
  eofy_reserve,
  prior_eofy_reserve,
  eop_adj_cost,
  eop_formula_factor,
  exclude_proceeds_from_basis,
  retirement_deprn_option,
  terminal_gain_loss_flag,
  super_group_id,
  over_depreciate_option,
  disabled_flag,
  old_adjusted_capacity,
  dry_hole_flag,
  cash_generating_unit_id,
  contract_id,
  rate_in_use,
  kca_operation,
  IS_DELETED_FLG,
  kca_seq_id,
  kca_seq_date
)
(
  SELECT
    set_of_books_id,
    asset_id,
    book_type_code,
    transaction_header_id_in,
    transaction_header_id_out,
    adjusted_cost,
    cost,
    source_cost,
    original_cost,
    source_original_cost,
    salvage_value,
    adjustment_required_status,
    retirement_pending_flag,
    last_update_date,
    last_updated_by,
    itc_amount,
    itc_basis,
    recoverable_cost,
    last_update_login,
    reval_ceiling,
    period_counter_fully_reserved,
    unrevalued_cost,
    allowed_deprn_limit_amount,
    period_counter_life_complete,
    adjusted_recoverable_cost,
    converted_flag,
    annual_deprn_rounding_flag,
    eofy_adj_cost,
    old_adjusted_cost,
    eofy_formula_factor,
    formula_factor,
    remaining_life1,
    remaining_life2,
    short_fiscal_year_flag,
    group_asset_id,
    reval_amortization_basis,
    itc_amount_id,
    retirement_id,
    tax_request_id,
    basic_rate,
    adjusted_rate,
    bonus_rule,
    ceiling_name,
    adjusted_capacity,
    fully_rsvd_revals_counter,
    idled_flag,
    period_counter_capitalized,
    period_counter_fully_retired,
    production_capacity,
    unit_of_measure,
    percent_salvage_value,
    allowed_deprn_limit,
    annual_rounding_flag,
    global_attribute1,
    global_attribute2,
    global_attribute3,
    global_attribute4,
    global_attribute5,
    global_attribute6,
    global_attribute7,
    global_attribute8,
    global_attribute9,
    global_attribute10,
    global_attribute11,
    global_attribute12,
    global_attribute13,
    global_attribute14,
    global_attribute15,
    global_attribute16,
    global_attribute17,
    global_attribute18,
    global_attribute19,
    global_attribute20,
    global_attribute_category,
    date_placed_in_service,
    date_effective,
    deprn_start_date,
    deprn_method_code,
    life_in_months,
    rate_adjustment_factor,
    prorate_convention_code,
    prorate_date,
    cost_change_flag,
    capitalize_flag,
    depreciate_flag,
    date_ineffective,
    conversion_date,
    original_deprn_start_date,
    salvage_type,
    deprn_limit_type,
    reduction_rate,
    reduce_addition_flag,
    reduce_adjustment_flag,
    reduce_retirement_flag,
    recognize_gain_loss,
    recapture_reserve_flag,
    limit_proceeds_flag,
    terminal_gain_loss,
    tracking_method,
    exclude_fully_rsv_flag,
    excess_allocation_option,
    depreciation_option,
    member_rollup_flag,
    allocate_to_fully_rsv_flag,
    allocate_to_fully_ret_flag,
    terminal_gain_loss_amount,
    cip_cost,
    ytd_proceeds,
    ltd_proceeds,
    ltd_cost_of_removal,
    eofy_reserve,
    prior_eofy_reserve,
    eop_adj_cost,
    eop_formula_factor,
    exclude_proceeds_from_basis,
    retirement_deprn_option,
    terminal_gain_loss_flag,
    super_group_id,
    over_depreciate_option,
    disabled_flag,
    old_adjusted_capacity,
    dry_hole_flag,
    cash_generating_unit_id,
    contract_id,
    rate_in_use,
    kca_operation,
    'N' AS IS_DELETED_FLG,
    CAST(NULLIF(KCA_SEQ_ID, '') AS DECIMAL(36, 0)) AS KCA_SEQ_ID,
    kca_seq_date
  FROM bronze_bec_ods_stg.FA_MC_BOOKS
  WHERE
    kca_operation IN ('INSERT', 'UPDATE')
    AND (COALESCE(SET_OF_BOOKS_ID, 0), COALESCE(TRANSACTION_HEADER_ID_IN, 0), KCA_SEQ_ID) IN (
      SELECT
        COALESCE(SET_OF_BOOKS_ID, 0) AS SET_OF_BOOKS_ID,
        COALESCE(TRANSACTION_HEADER_ID_IN, 0) AS TRANSACTION_HEADER_ID_IN,
        MAX(KCA_SEQ_ID)
      FROM bronze_bec_ods_stg.FA_MC_BOOKS
      WHERE
        kca_operation IN ('INSERT', 'UPDATE')
      GROUP BY
        COALESCE(SET_OF_BOOKS_ID, 0),
        COALESCE(TRANSACTION_HEADER_ID_IN, 0)
    )
);
/* Soft delete */
UPDATE silver_bec_ods.FA_MC_BOOKS SET IS_DELETED_FLG = 'N';
UPDATE silver_bec_ods.FA_MC_BOOKS SET IS_DELETED_FLG = 'Y'
WHERE
  (COALESCE(SET_OF_BOOKS_ID, 0), COALESCE(TRANSACTION_HEADER_ID_IN, 0)) IN (
    SELECT
      COALESCE(SET_OF_BOOKS_ID, 0),
      COALESCE(TRANSACTION_HEADER_ID_IN, 0)
    FROM bec_raw_dl_ext.FA_MC_BOOKS
    WHERE
      (COALESCE(SET_OF_BOOKS_ID, 0), COALESCE(TRANSACTION_HEADER_ID_IN, 0), KCA_SEQ_ID) IN (
        SELECT
          COALESCE(SET_OF_BOOKS_ID, 0),
          COALESCE(TRANSACTION_HEADER_ID_IN, 0),
          MAX(KCA_SEQ_ID) AS KCA_SEQ_ID
        FROM bec_raw_dl_ext.FA_MC_BOOKS
        GROUP BY
          COALESCE(SET_OF_BOOKS_ID, 0),
          COALESCE(TRANSACTION_HEADER_ID_IN, 0)
      )
      AND kca_operation = 'DELETE'
  );
UPDATE bec_etl_ctrl.batch_ods_info SET last_refresh_date = CURRENT_TIMESTAMP()
WHERE
  ods_table_name = 'fa_mc_books';