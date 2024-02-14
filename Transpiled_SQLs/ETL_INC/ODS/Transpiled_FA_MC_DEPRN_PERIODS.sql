/* Delete Records */
DELETE FROM silver_bec_ods.FA_MC_DEPRN_PERIODS
WHERE
  (COALESCE(BOOK_TYPE_CODE, 'NA'), COALESCE(PERIOD_COUNTER, 0), COALESCE(SET_OF_BOOKS_ID, 0)) IN (
    SELECT
      COALESCE(stg.BOOK_TYPE_CODE, 'NA') AS BOOK_TYPE_CODE,
      COALESCE(stg.PERIOD_COUNTER, 0) AS PERIOD_COUNTER,
      COALESCE(stg.SET_OF_BOOKS_ID, 0) AS SET_OF_BOOKS_ID
    FROM silver_bec_ods.FA_MC_DEPRN_PERIODS AS ods, bronze_bec_ods_stg.FA_MC_DEPRN_PERIODS AS stg
    WHERE
      COALESCE(ods.BOOK_TYPE_CODE, 'NA') = COALESCE(stg.BOOK_TYPE_CODE, 'NA')
      AND COALESCE(ods.PERIOD_COUNTER, 0) = COALESCE(stg.PERIOD_COUNTER, 0)
      AND COALESCE(ods.SET_OF_BOOKS_ID, 0) = COALESCE(stg.SET_OF_BOOKS_ID, 0)
      AND stg.kca_operation IN ('INSERT', 'UPDATE')
  );
/* Insert records */
INSERT INTO silver_bec_ods.FA_MC_DEPRN_PERIODS (
  SET_OF_BOOKS_ID,
  BOOK_TYPE_CODE,
  PERIOD_NAME,
  PERIOD_COUNTER,
  FISCAL_YEAR,
  PERIOD_NUM,
  PERIOD_OPEN_DATE,
  PERIOD_CLOSE_DATE,
  DEPRECIATION_BATCH_ID,
  RETIREMENT_BATCH_ID,
  RECLASS_BATCH_ID,
  TRANSFER_BATCH_ID,
  ADDITION_BATCH_ID,
  ADJUSTMENT_BATCH_ID,
  DEFERRED_DEPRN_BATCH_ID,
  CALENDAR_PERIOD_OPEN_DATE,
  CALENDAR_PERIOD_CLOSE_DATE,
  CIP_ADDITION_BATCH_ID,
  CIP_ADJUSTMENT_BATCH_ID,
  CIP_RECLASS_BATCH_ID,
  CIP_RETIREMENT_BATCH_ID,
  CIP_REVAL_BATCH_ID,
  CIP_TRANSFER_BATCH_ID,
  REVAL_BATCH_ID,
  DEPRN_ADJUSTMENT_BATCH_ID,
  DEPRN_RUN,
  KCA_OPERATION,
  IS_DELETED_FLG,
  kca_seq_id,
  kca_seq_date
)
(
  SELECT
    SET_OF_BOOKS_ID,
    BOOK_TYPE_CODE,
    PERIOD_NAME,
    PERIOD_COUNTER,
    FISCAL_YEAR,
    PERIOD_NUM,
    PERIOD_OPEN_DATE,
    PERIOD_CLOSE_DATE,
    DEPRECIATION_BATCH_ID,
    RETIREMENT_BATCH_ID,
    RECLASS_BATCH_ID,
    TRANSFER_BATCH_ID,
    ADDITION_BATCH_ID,
    ADJUSTMENT_BATCH_ID,
    DEFERRED_DEPRN_BATCH_ID,
    CALENDAR_PERIOD_OPEN_DATE,
    CALENDAR_PERIOD_CLOSE_DATE,
    CIP_ADDITION_BATCH_ID,
    CIP_ADJUSTMENT_BATCH_ID,
    CIP_RECLASS_BATCH_ID,
    CIP_RETIREMENT_BATCH_ID,
    CIP_REVAL_BATCH_ID,
    CIP_TRANSFER_BATCH_ID,
    REVAL_BATCH_ID,
    DEPRN_ADJUSTMENT_BATCH_ID,
    DEPRN_RUN,
    KCA_OPERATION,
    'N' AS IS_DELETED_FLG,
    CAST(NULLIF(KCA_SEQ_ID, '') AS DECIMAL(36, 0)) AS KCA_SEQ_ID,
    kca_seq_date
  FROM bronze_bec_ods_stg.FA_MC_DEPRN_PERIODS
  WHERE
    kca_operation IN ('INSERT', 'UPDATE')
    AND (COALESCE(BOOK_TYPE_CODE, 'NA'), COALESCE(PERIOD_COUNTER, 0), COALESCE(SET_OF_BOOKS_ID, 0), kca_seq_id) IN (
      SELECT
        COALESCE(BOOK_TYPE_CODE, 'NA') AS BOOK_TYPE_CODE,
        COALESCE(PERIOD_COUNTER, 0) AS PERIOD_COUNTER,
        COALESCE(SET_OF_BOOKS_ID, 0) AS SET_OF_BOOKS_ID,
        MAX(kca_seq_id)
      FROM bronze_bec_ods_stg.FA_MC_DEPRN_PERIODS
      WHERE
        kca_operation IN ('INSERT', 'UPDATE')
      GROUP BY
        COALESCE(BOOK_TYPE_CODE, 'NA'),
        COALESCE(PERIOD_COUNTER, 0),
        COALESCE(SET_OF_BOOKS_ID, 0)
    )
);
/* Soft delete */
UPDATE silver_bec_ods.FA_MC_DEPRN_PERIODS SET IS_DELETED_FLG = 'N';
UPDATE silver_bec_ods.FA_MC_DEPRN_PERIODS SET IS_DELETED_FLG = 'Y'
WHERE
  (COALESCE(BOOK_TYPE_CODE, 'NA'), COALESCE(PERIOD_COUNTER, 0), COALESCE(SET_OF_BOOKS_ID, 0)) IN (
    SELECT
      COALESCE(BOOK_TYPE_CODE, 'NA'),
      COALESCE(PERIOD_COUNTER, 0),
      COALESCE(SET_OF_BOOKS_ID, 0)
    FROM bec_raw_dl_ext.FA_MC_DEPRN_PERIODS
    WHERE
      (COALESCE(BOOK_TYPE_CODE, 'NA'), COALESCE(PERIOD_COUNTER, 0), COALESCE(SET_OF_BOOKS_ID, 0), KCA_SEQ_ID) IN (
        SELECT
          COALESCE(BOOK_TYPE_CODE, 'NA'),
          COALESCE(PERIOD_COUNTER, 0),
          COALESCE(SET_OF_BOOKS_ID, 0),
          MAX(KCA_SEQ_ID) AS KCA_SEQ_ID
        FROM bec_raw_dl_ext.FA_MC_DEPRN_PERIODS
        GROUP BY
          COALESCE(BOOK_TYPE_CODE, 'NA'),
          COALESCE(PERIOD_COUNTER, 0),
          COALESCE(SET_OF_BOOKS_ID, 0)
      )
      AND kca_operation = 'DELETE'
  );
UPDATE bec_etl_ctrl.batch_ods_info SET last_refresh_date = CURRENT_TIMESTAMP()
WHERE
  ods_table_name = 'fa_mc_deprn_periods';