/* Delete Records */
DELETE FROM silver_bec_ods.FA_DEPRN_PERIODS
WHERE
  (COALESCE(BOOK_TYPE_CODE, 'NA'), COALESCE(PERIOD_COUNTER, 0)) IN (
    SELECT
      COALESCE(stg.BOOK_TYPE_CODE, 'NA') AS BOOK_TYPE_CODE,
      COALESCE(stg.PERIOD_COUNTER, 0) AS PERIOD_COUNTER
    FROM silver_bec_ods.FA_DEPRN_PERIODS AS ods, bronze_bec_ods_stg.FA_DEPRN_PERIODS AS stg
    WHERE
      COALESCE(ods.BOOK_TYPE_CODE, 'NA') = COALESCE(stg.BOOK_TYPE_CODE, 'NA')
      AND COALESCE(ods.PERIOD_COUNTER, 0) = COALESCE(stg.PERIOD_COUNTER, 0)
      AND stg.kca_operation IN ('INSERT', 'UPDATE')
  );
/* Insert records */
INSERT INTO silver_bec_ods.FA_DEPRN_PERIODS (
  book_type_code,
  period_name,
  period_counter,
  fiscal_year,
  period_num,
  period_open_date,
  period_close_date,
  depreciation_batch_id,
  retirement_batch_id,
  reclass_batch_id,
  transfer_batch_id,
  addition_batch_id,
  adjustment_batch_id,
  deferred_deprn_batch_id,
  calendar_period_open_date,
  calendar_period_close_date,
  cip_addition_batch_id,
  cip_adjustment_batch_id,
  cip_reclass_batch_id,
  cip_retirement_batch_id,
  cip_reval_batch_id,
  cip_transfer_batch_id,
  reval_batch_id,
  deprn_adjustment_batch_id,
  deprn_run,
  xla_conversion_status,
  gl_transfer_flag,
  kca_operation,
  IS_DELETED_FLG,
  kca_seq_id,
  kca_seq_date
)
(
  SELECT
    book_type_code,
    period_name,
    period_counter,
    fiscal_year,
    period_num,
    period_open_date,
    period_close_date,
    depreciation_batch_id,
    retirement_batch_id,
    reclass_batch_id,
    transfer_batch_id,
    addition_batch_id,
    adjustment_batch_id,
    deferred_deprn_batch_id,
    calendar_period_open_date,
    calendar_period_close_date,
    cip_addition_batch_id,
    cip_adjustment_batch_id,
    cip_reclass_batch_id,
    cip_retirement_batch_id,
    cip_reval_batch_id,
    cip_transfer_batch_id,
    reval_batch_id,
    deprn_adjustment_batch_id,
    deprn_run,
    xla_conversion_status,
    gl_transfer_flag,
    kca_operation,
    'N' AS IS_DELETED_FLG,
    CAST(NULLIF(KCA_SEQ_ID, '') AS DECIMAL(36, 0)) AS KCA_SEQ_ID,
    kca_seq_date
  FROM bronze_bec_ods_stg.FA_DEPRN_PERIODS
  WHERE
    kca_operation IN ('INSERT', 'UPDATE')
    AND (COALESCE(BOOK_TYPE_CODE, 'NA'), COALESCE(PERIOD_COUNTER, 0), kca_seq_id) IN (
      SELECT
        COALESCE(BOOK_TYPE_CODE, 'NA') AS BOOK_TYPE_CODE,
        COALESCE(PERIOD_COUNTER, 0) AS PERIOD_COUNTER,
        MAX(kca_seq_id)
      FROM bronze_bec_ods_stg.FA_DEPRN_PERIODS
      WHERE
        kca_operation IN ('INSERT', 'UPDATE')
      GROUP BY
        COALESCE(BOOK_TYPE_CODE, 'NA'),
        COALESCE(PERIOD_COUNTER, 0)
    )
);
/* Soft delete */
UPDATE silver_bec_ods.FA_DEPRN_PERIODS SET IS_DELETED_FLG = 'N';
UPDATE silver_bec_ods.FA_DEPRN_PERIODS SET IS_DELETED_FLG = 'Y'
WHERE
  (COALESCE(BOOK_TYPE_CODE, 'NA'), COALESCE(PERIOD_COUNTER, 0)) IN (
    SELECT
      COALESCE(BOOK_TYPE_CODE, 'NA'),
      COALESCE(PERIOD_COUNTER, 0)
    FROM bec_raw_dl_ext.FA_DEPRN_PERIODS
    WHERE
      (COALESCE(BOOK_TYPE_CODE, 'NA'), COALESCE(PERIOD_COUNTER, 0), KCA_SEQ_ID) IN (
        SELECT
          COALESCE(BOOK_TYPE_CODE, 'NA'),
          COALESCE(PERIOD_COUNTER, 0),
          MAX(KCA_SEQ_ID) AS KCA_SEQ_ID
        FROM bec_raw_dl_ext.FA_DEPRN_PERIODS
        GROUP BY
          COALESCE(BOOK_TYPE_CODE, 'NA'),
          COALESCE(PERIOD_COUNTER, 0)
      )
      AND kca_operation = 'DELETE'
  );
UPDATE bec_etl_ctrl.batch_ods_info SET last_refresh_date = CURRENT_TIMESTAMP()
WHERE
  ods_table_name = 'fa_deprn_periods';