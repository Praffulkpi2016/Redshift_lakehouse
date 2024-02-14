DROP TABLE IF EXISTS silver_bec_ods.FA_MC_BOOK_CONTROLS;
CREATE TABLE IF NOT EXISTS silver_bec_ods.FA_MC_BOOK_CONTROLS (
  set_of_books_id DECIMAL(15, 0),
  book_type_code STRING,
  currency_code STRING,
  deprn_status STRING,
  deprn_request_id DECIMAL(15, 0),
  last_period_counter DECIMAL(15, 0),
  last_deprn_run_date TIMESTAMP,
  current_fiscal_year SMALLINT,
  retired_status STRING,
  retired_request_id DECIMAL(15, 0),
  primary_set_of_books_id DECIMAL(15, 0),
  primary_currency_code STRING,
  source_retired_status STRING,
  source_retired_request_id DECIMAL(15, 0),
  mrc_converted_flag STRING,
  enabled_flag STRING,
  nbv_amount_threshold DECIMAL(28, 10),
  conversion_status STRING,
  last_updated_by DECIMAL(15, 0),
  last_update_date TIMESTAMP,
  last_update_login DECIMAL(15, 0),
  mass_request_id DECIMAL(15, 0),
  allow_impairment_flag STRING,
  gl_posting_allowed_flag STRING,
  kca_operation STRING,
  is_deleted_flg STRING,
  kca_seq_id DECIMAL(36, 0),
  kca_seq_date TIMESTAMP
);
INSERT INTO silver_bec_ods.FA_MC_BOOK_CONTROLS (
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
  kca_operation,
  is_deleted_flg,
  kca_seq_id,
  kca_seq_date
)
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
  kca_operation,
  'N' AS IS_DELETED_FLG,
  CAST(NULLIF(KCA_SEQ_ID, '') AS DECIMAL(36, 0)) AS KCA_SEQ_ID,
  kca_seq_date
FROM bronze_bec_ods_stg.FA_MC_BOOK_CONTROLS;
UPDATE bec_etl_ctrl.batch_ods_info SET load_type = 'I', last_refresh_date = CURRENT_TIMESTAMP()
WHERE
  ods_table_name = 'fa_mc_book_controls';