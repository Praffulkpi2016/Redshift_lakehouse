TRUNCATE table
	table bronze_bec_ods_stg.FA_DEPRN_PERIODS;
INSERT INTO bronze_bec_ods_stg.FA_DEPRN_PERIODS (
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
  KCA_OPERATION,
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
    KCA_OPERATION,
    kca_seq_id,
    kca_seq_date
  FROM bec_raw_dl_ext.FA_DEPRN_PERIODS
  WHERE
    kca_operation <> 'DELETE'
    AND COALESCE(kca_seq_id, '') <> ''
    AND (COALESCE(BOOK_TYPE_CODE, 'NA'), COALESCE(PERIOD_COUNTER, 0), kca_seq_id) IN (
      SELECT
        COALESCE(BOOK_TYPE_CODE, 'NA') AS BOOK_TYPE_CODE,
        COALESCE(PERIOD_COUNTER, 0) AS PERIOD_COUNTER,
        MAX(kca_seq_id)
      FROM bec_raw_dl_ext.FA_DEPRN_PERIODS
      WHERE
        kca_operation <> 'DELETE' AND COALESCE(kca_seq_id, '') <> ''
      GROUP BY
        COALESCE(BOOK_TYPE_CODE, 'NA'),
        COALESCE(PERIOD_COUNTER, 0)
    )
    AND kca_seq_date > (
      SELECT
        (
          executebegints - prune_days
        )
      FROM bec_etl_ctrl.batch_ods_info
      WHERE
        ods_table_name = 'fa_deprn_periods'
    )
);