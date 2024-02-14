TRUNCATE table bronze_bec_ods_stg.AP_TERMS_LINES;
INSERT INTO bronze_bec_ods_stg.AP_TERMS_LINES (
  term_id,
  sequence_num,
  last_update_date,
  last_updated_by,
  creation_date,
  created_by,
  last_update_login,
  due_percent,
  due_amount,
  due_days,
  due_day_of_month,
  due_months_forward,
  discount_percent,
  discount_days,
  discount_day_of_month,
  discount_months_forward,
  discount_percent_2,
  discount_days_2,
  discount_day_of_month_2,
  discount_months_forward_2,
  discount_percent_3,
  discount_days_3,
  discount_day_of_month_3,
  discount_months_forward_3,
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
  fixed_date,
  calendar,
  discount_amount,
  discount_criteria,
  discount_amount_2,
  discount_criteria_2,
  discount_amount_3,
  discount_criteria_3,
  KCA_OPERATION,
  kca_seq_id,
  kca_seq_date
)
(
  SELECT
    term_id,
    sequence_num,
    last_update_date,
    last_updated_by,
    creation_date,
    created_by,
    last_update_login,
    due_percent,
    due_amount,
    due_days,
    due_day_of_month,
    due_months_forward,
    discount_percent,
    discount_days,
    discount_day_of_month,
    discount_months_forward,
    discount_percent_2,
    discount_days_2,
    discount_day_of_month_2,
    discount_months_forward_2,
    discount_percent_3,
    discount_days_3,
    discount_day_of_month_3,
    discount_months_forward_3,
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
    fixed_date,
    calendar,
    discount_amount,
    discount_criteria,
    discount_amount_2,
    discount_criteria_2,
    discount_amount_3,
    discount_criteria_3,
    KCA_OPERATION,
    kca_seq_id,
    kca_seq_date
  FROM bec_raw_dl_ext.AP_TERMS_LINES
  WHERE
    kca_operation <> 'DELETE'
    AND COALESCE(kca_seq_id, '') <> ''
    AND (TERM_ID, SEQUENCE_NUM, kca_seq_id) IN (
      SELECT
        TERM_ID,
        SEQUENCE_NUM,
        MAX(kca_seq_id)
      FROM bec_raw_dl_ext.AP_TERMS_LINES
      WHERE
        kca_operation <> 'DELETE' AND COALESCE(kca_seq_id, '') <> ''
      GROUP BY
        TERM_ID,
        SEQUENCE_NUM
    )
    AND kca_seq_date > (
      SELECT
        (
          executebegints - prune_days
        )
      FROM bec_etl_ctrl.batch_ods_info
      WHERE
        ods_table_name = 'ap_terms_lines'
    )
);