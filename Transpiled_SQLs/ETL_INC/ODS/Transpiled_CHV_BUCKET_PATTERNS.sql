/* Delete Records */
DELETE FROM silver_bec_ods.chv_bucket_patterns
WHERE
  BUCKET_PATTERN_ID IN (
    SELECT
      stg.BUCKET_PATTERN_ID
    FROM silver_bec_ods.chv_bucket_patterns AS ods, bronze_bec_ods_stg.chv_bucket_patterns AS stg
    WHERE
      ods.BUCKET_PATTERN_ID = stg.BUCKET_PATTERN_ID
      AND stg.kca_operation IN ('INSERT', 'UPDATE')
  );
/* Insert records */
INSERT INTO silver_bec_ods.chv_bucket_patterns (
  BUCKET_PATTERN_ID,
  BUCKET_PATTERN_NAME,
  NUMBER_DAILY_BUCKETS,
  NUMBER_WEEKLY_BUCKETS,
  NUMBER_MONTHLY_BUCKETS,
  NUMBER_QUARTERLY_BUCKETS,
  LAST_UPDATE_DATE,
  LAST_UPDATED_BY,
  CREATION_DATE,
  CREATED_BY,
  WEEK_START_DAY,
  DESCRIPTION,
  INACTIVE_DATE,
  ATTRIBUTE_CATEGORY,
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
  LAST_UPDATE_LOGIN,
  REQUEST_ID,
  PROGRAM_APPLICATION_ID,
  PROGRAM_ID,
  PROGRAM_UPDATE_DATE,
  KCA_OPERATION,
  IS_DELETED_FLG,
  kca_seq_id,
  kca_seq_date
)
(
  SELECT
    BUCKET_PATTERN_ID,
    BUCKET_PATTERN_NAME,
    NUMBER_DAILY_BUCKETS,
    NUMBER_WEEKLY_BUCKETS,
    NUMBER_MONTHLY_BUCKETS,
    NUMBER_QUARTERLY_BUCKETS,
    LAST_UPDATE_DATE,
    LAST_UPDATED_BY,
    CREATION_DATE,
    CREATED_BY,
    WEEK_START_DAY,
    DESCRIPTION,
    INACTIVE_DATE,
    ATTRIBUTE_CATEGORY,
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
    LAST_UPDATE_LOGIN,
    REQUEST_ID,
    PROGRAM_APPLICATION_ID,
    PROGRAM_ID,
    PROGRAM_UPDATE_DATE,
    KCA_OPERATION,
    'N' AS IS_DELETED_FLG,
    CAST(NULLIF(KCA_SEQ_ID, '') AS DECIMAL(36, 0)) AS KCA_SEQ_ID,
    kca_seq_date
  FROM bronze_bec_ods_stg.chv_bucket_patterns
  WHERE
    kca_operation IN ('INSERT', 'UPDATE')
    AND (BUCKET_PATTERN_ID, kca_seq_id) IN (
      SELECT
        BUCKET_PATTERN_ID,
        MAX(kca_seq_id)
      FROM bronze_bec_ods_stg.chv_bucket_patterns
      WHERE
        kca_operation IN ('INSERT', 'UPDATE')
      GROUP BY
        BUCKET_PATTERN_ID
    )
);
/* Soft delete */
UPDATE silver_bec_ods.chv_bucket_patterns SET IS_DELETED_FLG = 'N';
UPDATE silver_bec_ods.chv_bucket_patterns SET IS_DELETED_FLG = 'Y'
WHERE
  (
    BUCKET_PATTERN_ID
  ) IN (
    SELECT
      BUCKET_PATTERN_ID
    FROM bec_raw_dl_ext.chv_bucket_patterns
    WHERE
      (BUCKET_PATTERN_ID, KCA_SEQ_ID) IN (
        SELECT
          BUCKET_PATTERN_ID,
          MAX(KCA_SEQ_ID) AS KCA_SEQ_ID
        FROM bec_raw_dl_ext.chv_bucket_patterns
        GROUP BY
          BUCKET_PATTERN_ID
      )
      AND kca_operation = 'DELETE'
  );
UPDATE bec_etl_ctrl.batch_ods_info SET last_refresh_date = CURRENT_TIMESTAMP()
WHERE
  ods_table_name = 'chv_bucket_patterns';