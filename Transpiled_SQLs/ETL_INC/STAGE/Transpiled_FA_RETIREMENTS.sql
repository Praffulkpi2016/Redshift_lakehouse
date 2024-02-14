TRUNCATE table bronze_bec_ods_stg.FA_RETIREMENTS;
INSERT INTO bronze_bec_ods_stg.FA_RETIREMENTS (
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
  kca_seq_id,
  kca_seq_date
)
(
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
    kca_seq_id,
    kca_seq_date
  FROM bec_raw_dl_ext.FA_RETIREMENTS
  WHERE
    kca_operation <> 'DELETE'
    AND COALESCE(kca_seq_id, '') <> ''
    AND (COALESCE(RETIREMENT_ID, 0), KCA_SEQ_ID) IN (
      SELECT
        COALESCE(RETIREMENT_ID, 0) AS RETIREMENT_ID,
        MAX(KCA_SEQ_ID)
      FROM bec_raw_dl_ext.FA_RETIREMENTS
      WHERE
        kca_operation <> 'DELETE' AND COALESCE(kca_seq_id, '') <> ''
      GROUP BY
        COALESCE(RETIREMENT_ID, 0)
    )
    AND kca_seq_date > (
      SELECT
        (
          executebegints - prune_days
        )
      FROM bec_etl_ctrl.batch_ods_info
      WHERE
        ods_table_name = 'fa_retirements'
    )
);