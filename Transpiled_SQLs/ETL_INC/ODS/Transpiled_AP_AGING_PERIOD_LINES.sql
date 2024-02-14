/* Delete Records */
DELETE FROM silver_bec_ods.ap_aging_period_lines
WHERE
  aging_period_line_id IN (
    SELECT
      stg.aging_period_line_id
    FROM silver_bec_ods.ap_aging_period_lines AS ods, bronze_bec_ods_stg.ap_aging_period_lines AS stg
    WHERE
      ods.aging_period_line_id = stg.aging_period_line_id
      AND stg.kca_operation IN ('INSERT', 'UPDATE')
  );
/* Insert records */
INSERT INTO silver_bec_ods.ap_aging_period_lines (
  aging_period_line_id,
  last_updated_by,
  last_update_date,
  created_by,
  creation_date,
  aging_period_id,
  period_sequence_num,
  days_start,
  days_to,
  type,
  report_heading1,
  report_heading2,
  report_heading3,
  new_line,
  base_date,
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
  last_update_login,
  KCA_OPERATION,
  IS_DELETED_FLG,
  kca_seq_id,
  kca_seq_date
)
(
  SELECT
    aging_period_line_id,
    last_updated_by,
    last_update_date,
    created_by,
    creation_date,
    aging_period_id,
    period_sequence_num,
    days_start,
    days_to,
    type,
    report_heading1,
    report_heading2,
    report_heading3,
    new_line,
    base_date,
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
    last_update_login,
    KCA_OPERATION,
    'N' AS IS_DELETED_FLG,
    CAST(NULLIF(KCA_SEQ_ID, '') AS DECIMAL(36, 0)) AS KCA_SEQ_ID,
    kca_seq_date
  FROM bronze_bec_ods_stg.ap_aging_period_lines
  WHERE
    kca_operation IN ('INSERT', 'UPDATE')
    AND (aging_period_line_id, kca_seq_id) IN (
      SELECT
        aging_period_line_id,
        MAX(kca_seq_id)
      FROM bronze_bec_ods_stg.ap_aging_period_lines
      WHERE
        kca_operation IN ('INSERT', 'UPDATE')
      GROUP BY
        aging_period_line_id
    )
);
/* Soft delete */
UPDATE silver_bec_ods.ap_aging_period_lines SET IS_DELETED_FLG = 'N';
UPDATE silver_bec_ods.ap_aging_period_lines SET IS_DELETED_FLG = 'Y'
WHERE
  (
    aging_period_line_id
  ) IN (
    SELECT
      aging_period_line_id
    FROM bec_raw_dl_ext.ap_aging_period_lines
    WHERE
      (aging_period_line_id, KCA_SEQ_ID) IN (
        SELECT
          aging_period_line_id,
          MAX(KCA_SEQ_ID) AS KCA_SEQ_ID
        FROM bec_raw_dl_ext.ap_aging_period_lines
        GROUP BY
          aging_period_line_id
      )
      AND kca_operation = 'DELETE'
  );
UPDATE bec_etl_ctrl.batch_ods_info SET last_refresh_date = CURRENT_TIMESTAMP()
WHERE
  ods_table_name = 'ap_aging_period_lines';