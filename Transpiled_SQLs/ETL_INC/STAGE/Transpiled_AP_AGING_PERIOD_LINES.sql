TRUNCATE table bronze_bec_ods_stg.AP_AGING_PERIOD_LINES;
INSERT INTO bronze_bec_ods_stg.ap_aging_period_lines (
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
    kca_seq_id,
    kca_seq_date
  FROM bec_raw_dl_ext.AP_AGING_PERIOD_LINES
  WHERE
    kca_operation <> 'DELETE'
    AND COALESCE(kca_seq_id, '') <> ''
    AND (aging_period_line_id, kca_seq_id) IN (
      SELECT
        aging_period_line_id,
        MAX(kca_seq_id)
      FROM bec_raw_dl_ext.AP_AGING_PERIOD_LINES
      WHERE
        kca_operation <> 'DELETE' AND COALESCE(kca_seq_id, '') <> ''
      GROUP BY
        aging_period_line_id
    )
    AND kca_seq_date > (
      SELECT
        (
          executebegints - prune_days
        )
      FROM bec_etl_ctrl.batch_ods_info
      WHERE
        ods_table_name = 'ap_aging_period_lines'
    )
);