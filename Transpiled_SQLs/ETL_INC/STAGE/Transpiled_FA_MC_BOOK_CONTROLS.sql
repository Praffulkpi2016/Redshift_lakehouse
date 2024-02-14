TRUNCATE table
	table bronze_bec_ods_stg.FA_MC_BOOK_CONTROLS;
INSERT INTO bronze_bec_ods_stg.FA_MC_BOOK_CONTROLS (
  set_of_books_id,
  book_type_code,
  currency_code,
  deprn_status,
  deprn_request_id,
  last_period_counter,
  last_deprn_run_date,
  current_fiscal_year,
  retired_status,
  retired_request_id,
  primary_set_of_books_id,
  primary_currency_code,
  source_retired_status,
  source_retired_request_id,
  mrc_converted_flag,
  enabled_flag,
  nbv_amount_threshold,
  conversion_status,
  last_updated_by,
  last_update_date,
  last_update_login,
  mass_request_id,
  allow_impairment_flag,
  gl_posting_allowed_flag,
  KCA_OPERATION,
  kca_seq_id,
  kca_seq_date
)
(
  SELECT
    set_of_books_id,
    book_type_code,
    currency_code,
    deprn_status,
    deprn_request_id,
    last_period_counter,
    last_deprn_run_date,
    current_fiscal_year,
    retired_status,
    retired_request_id,
    primary_set_of_books_id,
    primary_currency_code,
    source_retired_status,
    source_retired_request_id,
    mrc_converted_flag,
    enabled_flag,
    nbv_amount_threshold,
    conversion_status,
    last_updated_by,
    last_update_date,
    last_update_login,
    mass_request_id,
    allow_impairment_flag,
    gl_posting_allowed_flag,
    KCA_OPERATION,
    kca_seq_id,
    kca_seq_date
  FROM bec_raw_dl_ext.FA_MC_BOOK_CONTROLS
  WHERE
    kca_operation <> 'DELETE'
    AND COALESCE(kca_seq_id, '') <> ''
    AND (COALESCE(BOOK_TYPE_CODE, 'NA'), COALESCE(SET_OF_BOOKS_ID, 0), kca_seq_id) IN (
      SELECT
        COALESCE(BOOK_TYPE_CODE, 'NA') AS BOOK_TYPE_CODE,
        COALESCE(SET_OF_BOOKS_ID, 0) AS SET_OF_BOOKS_ID,
        MAX(kca_seq_id)
      FROM bec_raw_dl_ext.FA_MC_BOOK_CONTROLS
      WHERE
        kca_operation <> 'DELETE' AND COALESCE(kca_seq_id, '') <> ''
      GROUP BY
        COALESCE(BOOK_TYPE_CODE, 'NA'),
        COALESCE(SET_OF_BOOKS_ID, 0)
    )
    AND kca_seq_date > (
      SELECT
        (
          executebegints - prune_days
        )
      FROM bec_etl_ctrl.batch_ods_info
      WHERE
        ods_table_name = 'fa_mc_book_controls'
    )
);