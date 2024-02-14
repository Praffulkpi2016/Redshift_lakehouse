DROP TABLE IF EXISTS silver_bec_ods.FA_RETIREMENTS;
CREATE TABLE IF NOT EXISTS silver_bec_ods.FA_RETIREMENTS (
  RETIREMENT_ID DECIMAL(15, 0),
  BOOK_TYPE_CODE STRING,
  ASSET_ID DECIMAL(15, 0),
  TRANSACTION_HEADER_ID_IN DECIMAL(15, 0),
  DATE_RETIRED TIMESTAMP,
  DATE_EFFECTIVE TIMESTAMP,
  COST_RETIRED DECIMAL(28, 10),
  STATUS STRING,
  LAST_UPDATE_DATE TIMESTAMP,
  LAST_UPDATED_BY DECIMAL(15, 0),
  RETIREMENT_PRORATE_CONVENTION STRING,
  TRANSACTION_HEADER_ID_OUT DECIMAL(15, 0),
  UNITS DECIMAL(15, 0),
  COST_OF_REMOVAL DECIMAL(28, 10),
  NBV_RETIRED DECIMAL(28, 10),
  GAIN_LOSS_AMOUNT DECIMAL(28, 10),
  PROCEEDS_OF_SALE DECIMAL(28, 10),
  GAIN_LOSS_TYPE_CODE STRING,
  RETIREMENT_TYPE_CODE STRING,
  ITC_RECAPTURED DECIMAL(28, 10),
  ITC_RECAPTURE_ID DECIMAL(15, 0),
  REFERENCE_NUM STRING,
  SOLD_TO STRING,
  TRADE_IN_ASSET_ID DECIMAL(15, 0),
  STL_METHOD_CODE STRING,
  STL_LIFE_IN_MONTHS DECIMAL(4, 0),
  STL_DEPRN_AMOUNT DECIMAL(28, 10),
  CREATED_BY DECIMAL(15, 0),
  CREATION_DATE TIMESTAMP,
  LAST_UPDATE_LOGIN DECIMAL(15, 0),
  ATTRIBUTE1 STRING,
  ATTRIBUTE2 STRING,
  ATTRIBUTE3 STRING,
  ATTRIBUTE4 STRING,
  ATTRIBUTE5 STRING,
  ATTRIBUTE6 STRING,
  ATTRIBUTE7 STRING,
  ATTRIBUTE8 STRING,
  ATTRIBUTE9 STRING,
  ATTRIBUTE10 STRING,
  ATTRIBUTE11 STRING,
  ATTRIBUTE12 STRING,
  ATTRIBUTE13 STRING,
  ATTRIBUTE14 STRING,
  ATTRIBUTE15 STRING,
  ATTRIBUTE_CATEGORY_CODE STRING,
  REVAL_RESERVE_RETIRED DECIMAL(28, 10),
  UNREVALUED_COST_RETIRED DECIMAL(28, 10),
  BONUS_RESERVE_RETIRED DECIMAL(28, 0),
  RECOGNIZE_GAIN_LOSS STRING,
  REDUCTION_RATE DECIMAL(28, 10),
  RECAPTURE_RESERVE_FLAG STRING,
  LIMIT_PROCEEDS_FLAG STRING,
  TERMINAL_GAIN_LOSS STRING,
  RESERVE_RETIRED DECIMAL(28, 10),
  RECAPTURE_AMOUNT DECIMAL(28, 10),
  EOFY_RESERVE DECIMAL(28, 10),
  IMPAIR_RESERVE_RETIRED DECIMAL(28, 10),
  KCA_OPERATION STRING,
  IS_DELETED_FLG STRING,
  kca_seq_id DECIMAL(36, 0),
  kca_seq_date TIMESTAMP
);
INSERT INTO silver_bec_ods.FA_RETIREMENTS (
  RETIREMENT_ID,
  BOOK_TYPE_CODE,
  ASSET_ID,
  TRANSACTION_HEADER_ID_IN,
  DATE_RETIRED,
  DATE_EFFECTIVE,
  COST_RETIRED,
  STATUS,
  LAST_UPDATE_DATE,
  LAST_UPDATED_BY,
  RETIREMENT_PRORATE_CONVENTION,
  TRANSACTION_HEADER_ID_OUT,
  UNITS,
  COST_OF_REMOVAL,
  NBV_RETIRED,
  GAIN_LOSS_AMOUNT,
  PROCEEDS_OF_SALE,
  GAIN_LOSS_TYPE_CODE,
  RETIREMENT_TYPE_CODE,
  ITC_RECAPTURED,
  ITC_RECAPTURE_ID,
  REFERENCE_NUM,
  SOLD_TO,
  TRADE_IN_ASSET_ID,
  STL_METHOD_CODE,
  STL_LIFE_IN_MONTHS,
  STL_DEPRN_AMOUNT,
  CREATED_BY,
  CREATION_DATE,
  LAST_UPDATE_LOGIN,
  ATTRIBUTE1,
  ATTRIBUTE2,
  ATTRIBUTE3,
  ATTRIBUTE4,
  ATTRIBUTE5,
  ATTRIBUTE6,
  ATTRIBUTE7,
  ATTRIBUTE8,
  ATTRIBUTE9,
  ATTRIBUTE10,
  ATTRIBUTE11,
  ATTRIBUTE12,
  ATTRIBUTE13,
  ATTRIBUTE14,
  ATTRIBUTE15,
  ATTRIBUTE_CATEGORY_CODE,
  REVAL_RESERVE_RETIRED,
  UNREVALUED_COST_RETIRED,
  BONUS_RESERVE_RETIRED,
  RECOGNIZE_GAIN_LOSS,
  REDUCTION_RATE,
  RECAPTURE_RESERVE_FLAG,
  LIMIT_PROCEEDS_FLAG,
  TERMINAL_GAIN_LOSS,
  RESERVE_RETIRED,
  RECAPTURE_AMOUNT,
  EOFY_RESERVE,
  IMPAIR_RESERVE_RETIRED,
  KCA_OPERATION,
  IS_DELETED_FLG,
  kca_seq_id,
  kca_seq_date
)
SELECT
  RETIREMENT_ID,
  BOOK_TYPE_CODE,
  ASSET_ID,
  TRANSACTION_HEADER_ID_IN,
  DATE_RETIRED,
  DATE_EFFECTIVE,
  COST_RETIRED,
  STATUS,
  LAST_UPDATE_DATE,
  LAST_UPDATED_BY,
  RETIREMENT_PRORATE_CONVENTION,
  TRANSACTION_HEADER_ID_OUT,
  UNITS,
  COST_OF_REMOVAL,
  NBV_RETIRED,
  GAIN_LOSS_AMOUNT,
  PROCEEDS_OF_SALE,
  GAIN_LOSS_TYPE_CODE,
  RETIREMENT_TYPE_CODE,
  ITC_RECAPTURED,
  ITC_RECAPTURE_ID,
  REFERENCE_NUM,
  SOLD_TO,
  TRADE_IN_ASSET_ID,
  STL_METHOD_CODE,
  STL_LIFE_IN_MONTHS,
  STL_DEPRN_AMOUNT,
  CREATED_BY,
  CREATION_DATE,
  LAST_UPDATE_LOGIN,
  ATTRIBUTE1,
  ATTRIBUTE2,
  ATTRIBUTE3,
  ATTRIBUTE4,
  ATTRIBUTE5,
  ATTRIBUTE6,
  ATTRIBUTE7,
  ATTRIBUTE8,
  ATTRIBUTE9,
  ATTRIBUTE10,
  ATTRIBUTE11,
  ATTRIBUTE12,
  ATTRIBUTE13,
  ATTRIBUTE14,
  ATTRIBUTE15,
  ATTRIBUTE_CATEGORY_CODE,
  REVAL_RESERVE_RETIRED,
  UNREVALUED_COST_RETIRED,
  BONUS_RESERVE_RETIRED,
  RECOGNIZE_GAIN_LOSS,
  REDUCTION_RATE,
  RECAPTURE_RESERVE_FLAG,
  LIMIT_PROCEEDS_FLAG,
  TERMINAL_GAIN_LOSS,
  RESERVE_RETIRED,
  RECAPTURE_AMOUNT,
  EOFY_RESERVE,
  IMPAIR_RESERVE_RETIRED,
  KCA_OPERATION,
  'N' AS IS_DELETED_FLG,
  CAST(NULLIF(KCA_SEQ_ID, '') AS DECIMAL(36, 0)) AS KCA_SEQ_ID,
  kca_seq_date
FROM bronze_bec_ods_stg.FA_RETIREMENTS;
UPDATE bec_etl_ctrl.batch_ods_info SET load_type = 'I', last_refresh_date = CURRENT_TIMESTAMP()
WHERE
  ods_table_name = 'fa_retirements';