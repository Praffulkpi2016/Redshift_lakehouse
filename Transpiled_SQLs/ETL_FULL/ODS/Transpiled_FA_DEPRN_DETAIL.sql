DROP TABLE IF EXISTS silver_bec_ods.fa_deprn_detail;
CREATE TABLE IF NOT EXISTS silver_bec_ods.fa_deprn_detail (
  BOOK_TYPE_CODE STRING,
  ASSET_ID DECIMAL(15, 0),
  PERIOD_COUNTER DECIMAL(15, 0),
  DISTRIBUTION_ID DECIMAL(15, 0),
  DEPRN_SOURCE_CODE STRING,
  DEPRN_RUN_DATE TIMESTAMP,
  DEPRN_AMOUNT DECIMAL(28, 10),
  YTD_DEPRN DECIMAL(28, 10),
  DEPRN_RESERVE DECIMAL(28, 10),
  ADDITION_COST_TO_CLEAR DECIMAL(28, 10),
  COST DECIMAL(28, 10),
  DEPRN_ADJUSTMENT_AMOUNT DECIMAL(28, 10),
  DEPRN_EXPENSE_JE_LINE_NUM DECIMAL(15, 0),
  DEPRN_RESERVE_JE_LINE_NUM DECIMAL(15, 0),
  REVAL_AMORT_JE_LINE_NUM DECIMAL(15, 0),
  REVAL_RESERVE_JE_LINE_NUM DECIMAL(15, 0),
  JE_HEADER_ID DECIMAL(15, 0),
  REVAL_AMORTIZATION DECIMAL(28, 10),
  REVAL_DEPRN_EXPENSE DECIMAL(28, 10),
  REVAL_RESERVE DECIMAL(28, 10),
  YTD_REVAL_DEPRN_EXPENSE DECIMAL(28, 10),
  BONUS_DEPRN_AMOUNT DECIMAL(28, 10),
  BONUS_YTD_DEPRN DECIMAL(28, 10),
  BONUS_DEPRN_RESERVE DECIMAL(28, 10),
  BONUS_DEPRN_ADJUSTMENT_AMOUNT DECIMAL(28, 10),
  BONUS_DEPRN_EXP_JE_LINE_NUM DECIMAL(15, 0),
  BONUS_DEPRN_RSV_JE_LINE_NUM DECIMAL(15, 0),
  DEPRN_EXPENSE_CCID DECIMAL(15, 0),
  DEPRN_RESERVE_CCID DECIMAL(15, 0),
  BONUS_DEPRN_EXPENSE_CCID DECIMAL(15, 0),
  BONUS_DEPRN_RESERVE_CCID DECIMAL(15, 0),
  REVAL_AMORT_CCID DECIMAL(15, 0),
  REVAL_RESERVE_CCID DECIMAL(15, 0),
  EVENT_ID DECIMAL(38, 0),
  DEPRN_RUN_ID DECIMAL(15, 0),
  IMPAIRMENT_AMOUNT DECIMAL(28, 10),
  YTD_IMPAIRMENT DECIMAL(28, 10),
  IMPAIRMENT_RESERVE DECIMAL(28, 10),
  CAPITAL_ADJUSTMENT DECIMAL(28, 10),
  GENERAL_FUND DECIMAL(28, 10),
  REVAL_LOSS_BALANCE DECIMAL(28, 10),
  KCA_OPERATION STRING,
  IS_DELETED_FLG STRING,
  KCA_SEQ_ID DECIMAL(36, 0),
  kca_seq_date TIMESTAMP
);
INSERT INTO silver_bec_ods.fa_deprn_detail (
  BOOK_TYPE_CODE,
  ASSET_ID,
  PERIOD_COUNTER,
  DISTRIBUTION_ID,
  DEPRN_SOURCE_CODE,
  DEPRN_RUN_DATE,
  DEPRN_AMOUNT,
  YTD_DEPRN,
  DEPRN_RESERVE,
  ADDITION_COST_TO_CLEAR,
  COST,
  DEPRN_ADJUSTMENT_AMOUNT,
  DEPRN_EXPENSE_JE_LINE_NUM,
  DEPRN_RESERVE_JE_LINE_NUM,
  REVAL_AMORT_JE_LINE_NUM,
  REVAL_RESERVE_JE_LINE_NUM,
  JE_HEADER_ID,
  REVAL_AMORTIZATION,
  REVAL_DEPRN_EXPENSE,
  REVAL_RESERVE,
  YTD_REVAL_DEPRN_EXPENSE,
  BONUS_DEPRN_AMOUNT,
  BONUS_YTD_DEPRN,
  BONUS_DEPRN_RESERVE,
  BONUS_DEPRN_ADJUSTMENT_AMOUNT,
  BONUS_DEPRN_EXP_JE_LINE_NUM,
  BONUS_DEPRN_RSV_JE_LINE_NUM,
  DEPRN_EXPENSE_CCID,
  DEPRN_RESERVE_CCID,
  BONUS_DEPRN_EXPENSE_CCID,
  BONUS_DEPRN_RESERVE_CCID,
  REVAL_AMORT_CCID,
  REVAL_RESERVE_CCID,
  EVENT_ID,
  DEPRN_RUN_ID,
  IMPAIRMENT_AMOUNT,
  YTD_IMPAIRMENT,
  IMPAIRMENT_RESERVE,
  CAPITAL_ADJUSTMENT,
  GENERAL_FUND,
  REVAL_LOSS_BALANCE,
  KCA_OPERATION,
  IS_DELETED_FLG,
  KCA_SEQ_ID,
  kca_seq_date
)
SELECT
  BOOK_TYPE_CODE,
  ASSET_ID,
  PERIOD_COUNTER,
  DISTRIBUTION_ID,
  DEPRN_SOURCE_CODE,
  DEPRN_RUN_DATE,
  DEPRN_AMOUNT,
  YTD_DEPRN,
  DEPRN_RESERVE,
  ADDITION_COST_TO_CLEAR,
  COST,
  DEPRN_ADJUSTMENT_AMOUNT,
  DEPRN_EXPENSE_JE_LINE_NUM,
  DEPRN_RESERVE_JE_LINE_NUM,
  REVAL_AMORT_JE_LINE_NUM,
  REVAL_RESERVE_JE_LINE_NUM,
  JE_HEADER_ID,
  REVAL_AMORTIZATION,
  REVAL_DEPRN_EXPENSE,
  REVAL_RESERVE,
  YTD_REVAL_DEPRN_EXPENSE,
  BONUS_DEPRN_AMOUNT,
  BONUS_YTD_DEPRN,
  BONUS_DEPRN_RESERVE,
  BONUS_DEPRN_ADJUSTMENT_AMOUNT,
  BONUS_DEPRN_EXP_JE_LINE_NUM,
  BONUS_DEPRN_RSV_JE_LINE_NUM,
  DEPRN_EXPENSE_CCID,
  DEPRN_RESERVE_CCID,
  BONUS_DEPRN_EXPENSE_CCID,
  BONUS_DEPRN_RESERVE_CCID,
  REVAL_AMORT_CCID,
  REVAL_RESERVE_CCID,
  EVENT_ID,
  DEPRN_RUN_ID,
  IMPAIRMENT_AMOUNT,
  YTD_IMPAIRMENT,
  IMPAIRMENT_RESERVE,
  CAPITAL_ADJUSTMENT,
  GENERAL_FUND,
  REVAL_LOSS_BALANCE,
  KCA_OPERATION,
  'N' AS IS_DELETED_FLG,
  CAST(NULLIF(KCA_SEQ_ID, '') AS DECIMAL(36, 0)) AS KCA_SEQ_ID,
  kca_seq_date
FROM bronze_bec_ods_stg.fa_deprn_detail;
UPDATE bec_etl_ctrl.batch_ods_info SET load_type = 'I', last_refresh_date = CURRENT_TIMESTAMP()
WHERE
  ods_table_name = 'fa_deprn_detail';