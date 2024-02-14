/* Delete Records */
DELETE FROM silver_bec_ods.gl_daily_rates
WHERE
  (FROM_CURRENCY, TO_CURRENCY, CONVERSION_DATE, CONVERSION_TYPE) IN (
    SELECT
      stg.FROM_CURRENCY,
      stg.TO_CURRENCY,
      stg.CONVERSION_DATE,
      stg.CONVERSION_TYPE
    FROM silver_bec_ods.gl_daily_rates AS ods, bronze_bec_ods_stg.gl_daily_rates AS stg
    WHERE
      ods.FROM_CURRENCY = stg.FROM_CURRENCY
      AND ods.TO_CURRENCY = stg.TO_CURRENCY
      AND ods.CONVERSION_DATE = stg.CONVERSION_DATE
      AND ods.CONVERSION_TYPE = stg.CONVERSION_TYPE
      AND stg.kca_operation IN ('INSERT', 'UPDATE')
  );
/* Insert records */
INSERT INTO silver_bec_ods.gl_daily_rates (
  FROM_CURRENCY,
  TO_CURRENCY,
  CONVERSION_DATE,
  CONVERSION_TYPE,
  CONVERSION_RATE,
  STATUS_CODE,
  CREATION_DATE,
  CREATED_BY,
  LAST_UPDATE_DATE,
  LAST_UPDATED_BY,
  LAST_UPDATE_LOGIN,
  CONTEXT,
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
  RATE_SOURCE_CODE,
  KCA_OPERATION,
  IS_DELETED_FLG,
  KCA_SEQ_ID,
  kca_seq_date
)
(
  SELECT
    FROM_CURRENCY,
    TO_CURRENCY,
    CONVERSION_DATE,
    CONVERSION_TYPE,
    CONVERSION_RATE,
    STATUS_CODE,
    CREATION_DATE,
    CREATED_BY,
    LAST_UPDATE_DATE,
    LAST_UPDATED_BY,
    LAST_UPDATE_LOGIN,
    CONTEXT,
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
    RATE_SOURCE_CODE,
    KCA_OPERATION,
    'N' AS IS_DELETED_FLG,
    CAST(NULLIF(KCA_SEQ_ID, '') AS DECIMAL(36, 0)) AS KCA_SEQ_ID,
    kca_seq_date
  FROM bronze_bec_ods_stg.gl_daily_rates
  WHERE
    kca_operation IN ('INSERT', 'UPDATE')
    AND (FROM_CURRENCY, TO_CURRENCY, CONVERSION_DATE, CONVERSION_TYPE, kca_seq_id) IN (
      SELECT
        FROM_CURRENCY,
        TO_CURRENCY,
        CONVERSION_DATE,
        CONVERSION_TYPE,
        MAX(kca_seq_id)
      FROM bronze_bec_ods_stg.gl_daily_rates
      WHERE
        kca_operation IN ('INSERT', 'UPDATE')
      GROUP BY
        FROM_CURRENCY,
        TO_CURRENCY,
        CONVERSION_DATE,
        CONVERSION_TYPE
    )
);
/* Soft delete */
UPDATE silver_bec_ods.gl_daily_rates SET IS_DELETED_FLG = 'N';
UPDATE silver_bec_ods.gl_daily_rates SET IS_DELETED_FLG = 'Y'
WHERE
  (FROM_CURRENCY, TO_CURRENCY, CONVERSION_DATE, CONVERSION_TYPE) IN (
    SELECT
      FROM_CURRENCY,
      TO_CURRENCY,
      CONVERSION_DATE,
      CONVERSION_TYPE
    FROM bec_raw_dl_ext.gl_daily_rates
    WHERE
      (FROM_CURRENCY, TO_CURRENCY, CONVERSION_DATE, CONVERSION_TYPE, KCA_SEQ_ID) IN (
        SELECT
          FROM_CURRENCY,
          TO_CURRENCY,
          CONVERSION_DATE,
          CONVERSION_TYPE,
          MAX(KCA_SEQ_ID) AS KCA_SEQ_ID
        FROM bec_raw_dl_ext.gl_daily_rates
        GROUP BY
          FROM_CURRENCY,
          TO_CURRENCY,
          CONVERSION_DATE,
          CONVERSION_TYPE
      )
      AND kca_operation = 'DELETE'
  );
UPDATE bec_etl_ctrl.batch_ods_info SET last_refresh_date = CURRENT_TIMESTAMP()
WHERE
  ods_table_name = 'gl_daily_rates';