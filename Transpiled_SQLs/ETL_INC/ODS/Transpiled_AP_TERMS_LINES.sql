/* Delete Records */
DELETE FROM silver_bec_ods.ap_terms_lines
WHERE
  (term_id, sequence_num) IN (
    SELECT
      stg.term_id,
      stg.sequence_num
    FROM silver_bec_ods.ap_terms_lines AS ods, bronze_bec_ods_stg.ap_terms_lines AS stg
    WHERE
      ods.term_id = stg.term_id
      AND ods.sequence_num = stg.sequence_num
      AND stg.kca_operation IN ('INSERT', 'UPDATE')
  );
INSERT INTO silver_bec_ods.ap_terms_lines (
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
  IS_DELETED_FLG,
  KCA_SEQ_ID,
  kca_seq_date
)
(
  SELECT
    term_id,
    sequence_num,
    TO_DATE(DATE_FORMAT(last_update_date, 'yyyy-MM-dd HH:mm:ss'), 'YYYY-MM-DD HH24:MI:SS'),
    last_updated_by,
    TO_DATE(DATE_FORMAT(creation_date, 'yyyy-MM-dd HH:mm:ss'), 'YYYY-MM-DD HH24:MI:SS'),
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
    TO_DATE(DATE_FORMAT(fixed_date, 'yyyy-MM-dd HH:mm:ss'), 'YYYY-MM-DD HH24:MI:SS') AS fixed_date,
    calendar,
    discount_amount,
    discount_criteria,
    discount_amount_2,
    discount_criteria_2,
    discount_amount_3,
    discount_criteria_3,
    KCA_OPERATION,
    'N' AS IS_DELETED_FLG,
    CAST(NULLIF(KCA_SEQ_ID, '') AS DECIMAL(36, 0)) AS KCA_SEQ_ID,
    kca_seq_date
  FROM bronze_bec_ods_stg.ap_terms_lines
  WHERE
    kca_operation IN ('INSERT', 'UPDATE')
    AND (TERM_ID, sequence_num, kca_seq_id) IN (
      SELECT
        TERM_ID,
        sequence_num,
        MAX(kca_seq_id)
      FROM bronze_bec_ods_stg.ap_terms_lines
      WHERE
        kca_operation IN ('INSERT', 'UPDATE')
      GROUP BY
        TERM_ID,
        sequence_num
    )
);
/* Soft delete */
UPDATE silver_bec_ods.ap_terms_lines SET IS_DELETED_FLG = 'N';
UPDATE silver_bec_ods.ap_terms_lines SET IS_DELETED_FLG = 'Y'
WHERE
  (TERM_ID, sequence_num) IN (
    SELECT
      TERM_ID,
      sequence_num
    FROM bec_raw_dl_ext.ap_terms_lines
    WHERE
      (TERM_ID, sequence_num, KCA_SEQ_ID) IN (
        SELECT
          TERM_ID,
          sequence_num,
          MAX(KCA_SEQ_ID) AS KCA_SEQ_ID
        FROM bec_raw_dl_ext.ap_terms_lines
        GROUP BY
          TERM_ID,
          sequence_num
      )
      AND kca_operation = 'DELETE'
  );
UPDATE bec_etl_ctrl.batch_ods_info SET last_refresh_date = CURRENT_TIMESTAMP()
WHERE
  ods_table_name = 'ap_terms_lines';