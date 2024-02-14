/* Delete Records */
DELETE FROM silver_bec_ods.fa_adjustments
WHERE
  (COALESCE(TRANSACTION_HEADER_ID, 0), COALESCE(ADJUSTMENT_LINE_ID, 0)) IN (
    SELECT
      COALESCE(stg.TRANSACTION_HEADER_ID, 0) AS TRANSACTION_HEADER_ID,
      COALESCE(stg.ADJUSTMENT_LINE_ID, 0) AS ADJUSTMENT_LINE_ID
    FROM silver_bec_ods.fa_adjustments AS ods, bronze_bec_ods_stg.fa_adjustments AS stg
    WHERE
      COALESCE(ods.TRANSACTION_HEADER_ID, 0) = COALESCE(stg.TRANSACTION_HEADER_ID, 0)
      AND COALESCE(ods.ADJUSTMENT_LINE_ID, 0) = COALESCE(stg.ADJUSTMENT_LINE_ID, 0)
      AND stg.kca_operation IN ('INSERT', 'UPDATE')
  );
/* Insert records */
INSERT INTO silver_bec_ods.fa_adjustments (
  TRANSACTION_HEADER_ID,
  SOURCE_TYPE_CODE,
  ADJUSTMENT_TYPE,
  DEBIT_CREDIT_FLAG,
  CODE_COMBINATION_ID,
  BOOK_TYPE_CODE,
  ASSET_ID,
  ADJUSTMENT_AMOUNT,
  DISTRIBUTION_ID,
  LAST_UPDATE_DATE,
  LAST_UPDATED_BY,
  LAST_UPDATE_LOGIN,
  ANNUALIZED_ADJUSTMENT,
  JE_HEADER_ID,
  JE_LINE_NUM,
  PERIOD_COUNTER_ADJUSTED,
  PERIOD_COUNTER_CREATED,
  ASSET_INVOICE_ID,
  GLOBAL_ATTRIBUTE1,
  GLOBAL_ATTRIBUTE2,
  GLOBAL_ATTRIBUTE3,
  GLOBAL_ATTRIBUTE4,
  GLOBAL_ATTRIBUTE5,
  GLOBAL_ATTRIBUTE6,
  GLOBAL_ATTRIBUTE7,
  GLOBAL_ATTRIBUTE8,
  GLOBAL_ATTRIBUTE9,
  GLOBAL_ATTRIBUTE10,
  GLOBAL_ATTRIBUTE11,
  GLOBAL_ATTRIBUTE12,
  GLOBAL_ATTRIBUTE13,
  GLOBAL_ATTRIBUTE14,
  GLOBAL_ATTRIBUTE15,
  GLOBAL_ATTRIBUTE16,
  GLOBAL_ATTRIBUTE17,
  GLOBAL_ATTRIBUTE18,
  GLOBAL_ATTRIBUTE19,
  GLOBAL_ATTRIBUTE20,
  GLOBAL_ATTRIBUTE_CATEGORY,
  DEPRN_OVERRIDE_FLAG,
  TRACK_MEMBER_FLAG,
  ADJUSTMENT_LINE_ID,
  SOURCE_LINE_ID,
  SOURCE_DEST_CODE,
  INSERTION_ORDER,
  KCA_OPERATION,
  IS_DELETED_FLG,
  kca_seq_id,
  kca_seq_date
)
(
  SELECT
    TRANSACTION_HEADER_ID,
    SOURCE_TYPE_CODE,
    ADJUSTMENT_TYPE,
    DEBIT_CREDIT_FLAG,
    CODE_COMBINATION_ID,
    BOOK_TYPE_CODE,
    ASSET_ID,
    ADJUSTMENT_AMOUNT,
    DISTRIBUTION_ID,
    LAST_UPDATE_DATE,
    LAST_UPDATED_BY,
    LAST_UPDATE_LOGIN,
    ANNUALIZED_ADJUSTMENT,
    JE_HEADER_ID,
    JE_LINE_NUM,
    PERIOD_COUNTER_ADJUSTED,
    PERIOD_COUNTER_CREATED,
    ASSET_INVOICE_ID,
    GLOBAL_ATTRIBUTE1,
    GLOBAL_ATTRIBUTE2,
    GLOBAL_ATTRIBUTE3,
    GLOBAL_ATTRIBUTE4,
    GLOBAL_ATTRIBUTE5,
    GLOBAL_ATTRIBUTE6,
    GLOBAL_ATTRIBUTE7,
    GLOBAL_ATTRIBUTE8,
    GLOBAL_ATTRIBUTE9,
    GLOBAL_ATTRIBUTE10,
    GLOBAL_ATTRIBUTE11,
    GLOBAL_ATTRIBUTE12,
    GLOBAL_ATTRIBUTE13,
    GLOBAL_ATTRIBUTE14,
    GLOBAL_ATTRIBUTE15,
    GLOBAL_ATTRIBUTE16,
    GLOBAL_ATTRIBUTE17,
    GLOBAL_ATTRIBUTE18,
    GLOBAL_ATTRIBUTE19,
    GLOBAL_ATTRIBUTE20,
    GLOBAL_ATTRIBUTE_CATEGORY,
    DEPRN_OVERRIDE_FLAG,
    TRACK_MEMBER_FLAG,
    ADJUSTMENT_LINE_ID,
    SOURCE_LINE_ID,
    SOURCE_DEST_CODE,
    INSERTION_ORDER,
    KCA_OPERATION,
    'N' AS IS_DELETED_FLG,
    CAST(NULLIF(KCA_SEQ_ID, '') AS DECIMAL(36, 0)) AS KCA_SEQ_ID,
    kca_seq_date
  FROM bronze_bec_ods_stg.fa_adjustments
  WHERE
    kca_operation IN ('INSERT', 'UPDATE')
    AND (COALESCE(TRANSACTION_HEADER_ID, 0), COALESCE(ADJUSTMENT_LINE_ID, 0), kca_seq_id) IN (
      SELECT
        COALESCE(TRANSACTION_HEADER_ID, 0) AS TRANSACTION_HEADER_ID,
        COALESCE(ADJUSTMENT_LINE_ID, 0) AS ADJUSTMENT_LINE_ID,
        MAX(kca_seq_id)
      FROM bronze_bec_ods_stg.fa_adjustments
      WHERE
        kca_operation IN ('INSERT', 'UPDATE')
      GROUP BY
        COALESCE(TRANSACTION_HEADER_ID, 0),
        COALESCE(ADJUSTMENT_LINE_ID, 0)
    )
);
/* Soft delete */
UPDATE silver_bec_ods.fa_adjustments SET IS_DELETED_FLG = 'N';
UPDATE silver_bec_ods.fa_adjustments SET IS_DELETED_FLG = 'Y'
WHERE
  (COALESCE(TRANSACTION_HEADER_ID, 0), COALESCE(ADJUSTMENT_LINE_ID, 0)) IN (
    SELECT
      COALESCE(TRANSACTION_HEADER_ID, 0),
      COALESCE(ADJUSTMENT_LINE_ID, 0)
    FROM bec_raw_dl_ext.fa_adjustments
    WHERE
      (COALESCE(TRANSACTION_HEADER_ID, 0), COALESCE(ADJUSTMENT_LINE_ID, 0), KCA_SEQ_ID) IN (
        SELECT
          COALESCE(TRANSACTION_HEADER_ID, 0),
          COALESCE(ADJUSTMENT_LINE_ID, 0),
          MAX(KCA_SEQ_ID) AS KCA_SEQ_ID
        FROM bec_raw_dl_ext.fa_adjustments
        GROUP BY
          COALESCE(TRANSACTION_HEADER_ID, 0),
          COALESCE(ADJUSTMENT_LINE_ID, 0)
      )
      AND kca_operation = 'DELETE'
  );
UPDATE bec_etl_ctrl.batch_ods_info SET last_refresh_date = CURRENT_TIMESTAMP()
WHERE
  ods_table_name = 'fa_adjustments';