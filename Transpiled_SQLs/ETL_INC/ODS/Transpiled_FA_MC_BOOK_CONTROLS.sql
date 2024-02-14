/* Delete Records */
DELETE FROM silver_bec_ods.FA_MC_BOOK_CONTROLS
WHERE
  (COALESCE(BOOK_TYPE_CODE, 'NA'), COALESCE(SET_OF_BOOKS_ID, 0)) IN (
    SELECT
      COALESCE(stg.BOOK_TYPE_CODE, 'NA') AS BOOK_TYPE_CODE,
      COALESCE(stg.SET_OF_BOOKS_ID, 0) AS SET_OF_BOOKS_ID
    FROM silver_bec_ods.FA_MC_BOOK_CONTROLS AS ods, bronze_bec_ods_stg.FA_MC_BOOK_CONTROLS AS stg
    WHERE
      COALESCE(ods.BOOK_TYPE_CODE, 'NA') = COALESCE(stg.BOOK_TYPE_CODE, 'NA')
      AND COALESCE(ods.SET_OF_BOOKS_ID, 0) = COALESCE(stg.SET_OF_BOOKS_ID, 0)
      AND stg.kca_operation IN ('INSERT', 'UPDATE')
  );
/* Insert records */
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
  IS_DELETED_FLG,
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
    kca_operation,
    'N' AS IS_DELETED_FLG,
    CAST(NULLIF(KCA_SEQ_ID, '') AS DECIMAL(36, 0)) AS KCA_SEQ_ID,
    kca_seq_date
  FROM bronze_bec_ods_stg.FA_MC_BOOK_CONTROLS
  WHERE
    kca_operation IN ('INSERT', 'UPDATE')
    AND (COALESCE(BOOK_TYPE_CODE, 'NA'), COALESCE(SET_OF_BOOKS_ID, 0), kca_seq_id) IN (
      SELECT
        COALESCE(BOOK_TYPE_CODE, 'NA') AS BOOK_TYPE_CODE,
        COALESCE(SET_OF_BOOKS_ID, 0) AS SET_OF_BOOKS_ID,
        MAX(kca_seq_id)
      FROM bronze_bec_ods_stg.FA_MC_BOOK_CONTROLS
      WHERE
        kca_operation IN ('INSERT', 'UPDATE')
      GROUP BY
        COALESCE(BOOK_TYPE_CODE, 'NA'),
        COALESCE(SET_OF_BOOKS_ID, 0)
    )
);
/* Soft delete */
UPDATE silver_bec_ods.FA_MC_BOOK_CONTROLS SET IS_DELETED_FLG = 'N';
UPDATE silver_bec_ods.FA_MC_BOOK_CONTROLS SET IS_DELETED_FLG = 'Y'
WHERE
  (COALESCE(BOOK_TYPE_CODE, 'NA'), COALESCE(SET_OF_BOOKS_ID, 0)) IN (
    SELECT
      COALESCE(BOOK_TYPE_CODE, 'NA'),
      COALESCE(SET_OF_BOOKS_ID, 0)
    FROM bec_raw_dl_ext.FA_MC_BOOK_CONTROLS
    WHERE
      (COALESCE(BOOK_TYPE_CODE, 'NA'), COALESCE(SET_OF_BOOKS_ID, 0), KCA_SEQ_ID) IN (
        SELECT
          COALESCE(BOOK_TYPE_CODE, 'NA'),
          COALESCE(SET_OF_BOOKS_ID, 0),
          MAX(KCA_SEQ_ID) AS KCA_SEQ_ID
        FROM bec_raw_dl_ext.FA_MC_BOOK_CONTROLS
        GROUP BY
          COALESCE(BOOK_TYPE_CODE, 'NA'),
          COALESCE(SET_OF_BOOKS_ID, 0)
      )
      AND kca_operation = 'DELETE'
  );
UPDATE bec_etl_ctrl.batch_ods_info SET last_refresh_date = CURRENT_TIMESTAMP()
WHERE
  ods_table_name = 'fa_mc_book_controls';