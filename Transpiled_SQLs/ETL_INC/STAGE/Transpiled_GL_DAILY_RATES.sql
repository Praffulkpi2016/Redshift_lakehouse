TRUNCATE table
	table bronze_bec_ods_stg.gl_daily_rates;
INSERT INTO bronze_bec_ods_stg.gl_daily_rates (
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
  kca_operation,
  kca_seq_id,
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
    kca_operation,
    kca_seq_id,
    kca_seq_date
  FROM bec_raw_dl_ext.gl_daily_rates
  WHERE
    kca_operation <> 'DELETE'
    AND COALESCE(kca_seq_id, '') <> ''
    AND (FROM_CURRENCY, TO_CURRENCY, CONVERSION_DATE, CONVERSION_TYPE, kca_seq_id) IN (
      SELECT
        FROM_CURRENCY,
        TO_CURRENCY,
        CONVERSION_DATE,
        CONVERSION_TYPE,
        MAX(kca_seq_id)
      FROM bec_raw_dl_ext.GL_DAILY_RATES
      WHERE
        kca_operation <> 'DELETE' AND COALESCE(kca_seq_id, '') <> ''
      GROUP BY
        FROM_CURRENCY,
        TO_CURRENCY,
        CONVERSION_DATE,
        CONVERSION_TYPE
    )
    AND (
      kca_seq_date > (
        SELECT
          (
            executebegints - prune_days
          )
        FROM bec_etl_ctrl.batch_ods_info
        WHERE
          ods_table_name = 'gl_daily_rates'
      )
    )
);