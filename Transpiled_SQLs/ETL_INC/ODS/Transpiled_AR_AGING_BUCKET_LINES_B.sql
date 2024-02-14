/* Delete Records */
DELETE FROM silver_bec_ods.AR_AGING_BUCKET_LINES_B
WHERE
  aging_bucket_line_id IN (
    SELECT
      stg.aging_bucket_line_id
    FROM silver_bec_ods.AR_AGING_BUCKET_LINES_B AS ods, bronze_bec_ods_stg.AR_AGING_BUCKET_LINES_B AS stg
    WHERE
      ods.aging_bucket_line_id = stg.aging_bucket_line_id
      AND stg.kca_operation IN ('INSERT', 'UPDATE')
  );
/* Insert records */
INSERT INTO silver_bec_ods.AR_AGING_BUCKET_LINES_B (
  aging_bucket_line_id,
  last_updated_by,
  last_update_date,
  last_update_login,
  created_by,
  creation_date,
  aging_bucket_id,
  bucket_sequence_num,
  days_start,
  days_to,
  `type`,
  attribute_category,
  attribute1,
  attribute2,
  attribute3,
  attribute4,
  attribute5,
  attribute6,
  attribute7,
  attribute8,
  attribute9,
  attribute10,
  attribute11,
  attribute12,
  attribute13,
  attribute14,
  attribute15,
  zd_edition_name,
  zd_sync,
  kca_operation,
  IS_DELETED_FLG,
  kca_seq_id,
  kca_seq_date
)
(
  SELECT
    aging_bucket_line_id,
    last_updated_by,
    last_update_date,
    last_update_login,
    created_by,
    creation_date,
    aging_bucket_id,
    bucket_sequence_num,
    days_start,
    days_to,
    `type`,
    attribute_category,
    attribute1,
    attribute2,
    attribute3,
    attribute4,
    attribute5,
    attribute6,
    attribute7,
    attribute8,
    attribute9,
    attribute10,
    attribute11,
    attribute12,
    attribute13,
    attribute14,
    attribute15,
    zd_edition_name,
    zd_sync,
    KCA_OPERATION,
    'N' AS IS_DELETED_FLG,
    CAST(NULLIF(KCA_SEQ_ID, '') AS DECIMAL(36, 0)) AS KCA_SEQ_ID,
    kca_seq_date
  FROM bronze_bec_ods_stg.AR_AGING_BUCKET_LINES_B
  WHERE
    kca_operation IN ('INSERT', 'UPDATE')
    AND (aging_bucket_line_id, kca_seq_id) IN (
      SELECT
        aging_bucket_line_id,
        MAX(kca_seq_id)
      FROM bronze_bec_ods_stg.AR_AGING_BUCKET_LINES_B
      WHERE
        kca_operation IN ('INSERT', 'UPDATE')
      GROUP BY
        aging_bucket_line_id
    )
);
/* Soft delete */
UPDATE silver_bec_ods.AR_AGING_BUCKET_LINES_B SET IS_DELETED_FLG = 'N';
UPDATE silver_bec_ods.AR_AGING_BUCKET_LINES_B SET IS_DELETED_FLG = 'Y'
WHERE
  (
    aging_bucket_line_id
  ) IN (
    SELECT
      aging_bucket_line_id
    FROM bec_raw_dl_ext.AR_AGING_BUCKET_LINES_B
    WHERE
      (aging_bucket_line_id, KCA_SEQ_ID) IN (
        SELECT
          aging_bucket_line_id,
          MAX(KCA_SEQ_ID) AS KCA_SEQ_ID
        FROM bec_raw_dl_ext.AR_AGING_BUCKET_LINES_B
        GROUP BY
          aging_bucket_line_id
      )
      AND kca_operation = 'DELETE'
  );
UPDATE bec_etl_ctrl.batch_ods_info SET last_refresh_date = CURRENT_TIMESTAMP()
WHERE
  ods_table_name = 'ar_aging_bucket_lines_b';