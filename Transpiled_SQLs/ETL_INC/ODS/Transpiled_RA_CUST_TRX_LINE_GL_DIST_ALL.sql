/* Delete Records */
DELETE FROM silver_bec_ods.RA_CUST_TRX_LINE_GL_DIST_ALL
WHERE
  CUST_TRX_LINE_GL_DIST_ID IN (
    SELECT
      stg.CUST_TRX_LINE_GL_DIST_ID
    FROM silver_bec_ods.ra_cust_trx_line_gl_dist_all AS ods, bronze_bec_ods_stg.ra_cust_trx_line_gl_dist_all AS stg
    WHERE
      ods.CUST_TRX_LINE_GL_DIST_ID = stg.CUST_TRX_LINE_GL_DIST_ID
      AND stg.kca_operation IN ('INSERT', 'UPDATE')
  );
/* Insert records */
INSERT INTO silver_bec_ods.ra_cust_trx_line_gl_dist_all (
  cust_trx_line_gl_dist_id,
  customer_trx_line_id,
  code_combination_id,
  set_of_books_id,
  last_update_date,
  last_updated_by,
  creation_date,
  created_by,
  last_update_login,
  `percent`,
  amount,
  gl_date,
  gl_posted_date,
  cust_trx_line_salesrep_id,
  comments,
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
  request_id,
  program_application_id,
  program_id,
  program_update_date,
  concatenated_segments,
  original_gl_date,
  post_request_id,
  posting_control_id,
  account_class,
  ra_post_loop_number,
  customer_trx_id,
  account_set_flag,
  acctd_amount,
  ussgl_transaction_code,
  ussgl_transaction_code_context,
  attribute11,
  attribute12,
  attribute13,
  attribute14,
  attribute15,
  latest_rec_flag,
  org_id,
  mrc_account_class,
  mrc_customer_trx_id,
  mrc_amount,
  mrc_gl_posted_date,
  mrc_posting_control_id,
  mrc_acctd_amount,
  collected_tax_ccid,
  collected_tax_concat_seg,
  revenue_adjustment_id,
  rev_adj_class_temp,
  rec_offset_flag,
  event_id,
  user_generated_flag,
  rounding_correction_flag,
  cogs_request_id,
  ccid_change_flag,
  global_attribute1,
  global_attribute2,
  global_attribute3,
  global_attribute4,
  global_attribute5,
  global_attribute6,
  global_attribute7,
  global_attribute8,
  global_attribute9,
  global_attribute10,
  global_attribute11,
  global_attribute12,
  global_attribute13,
  global_attribute14,
  global_attribute15,
  global_attribute16,
  global_attribute17,
  global_attribute18,
  global_attribute19,
  global_attribute20,
  global_attribute21,
  global_attribute22,
  global_attribute23,
  global_attribute24,
  global_attribute25,
  global_attribute26,
  global_attribute27,
  global_attribute28,
  global_attribute29,
  global_attribute30,
  global_attribute_category,
  kca_operation,
  is_deleted_flg,
  kca_seq_id,
  kca_seq_date
)
SELECT
  cust_trx_line_gl_dist_id,
  customer_trx_line_id,
  code_combination_id,
  set_of_books_id,
  last_update_date,
  last_updated_by,
  creation_date,
  created_by,
  last_update_login,
  `percent`,
  amount,
  gl_date,
  gl_posted_date,
  cust_trx_line_salesrep_id,
  comments,
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
  request_id,
  program_application_id,
  program_id,
  program_update_date,
  concatenated_segments,
  original_gl_date,
  post_request_id,
  posting_control_id,
  account_class,
  ra_post_loop_number,
  customer_trx_id,
  account_set_flag,
  acctd_amount,
  ussgl_transaction_code,
  ussgl_transaction_code_context,
  attribute11,
  attribute12,
  attribute13,
  attribute14,
  attribute15,
  latest_rec_flag,
  org_id,
  mrc_account_class,
  mrc_customer_trx_id,
  mrc_amount,
  mrc_gl_posted_date,
  mrc_posting_control_id,
  mrc_acctd_amount,
  collected_tax_ccid,
  collected_tax_concat_seg,
  revenue_adjustment_id,
  rev_adj_class_temp,
  rec_offset_flag,
  event_id,
  user_generated_flag,
  rounding_correction_flag,
  cogs_request_id,
  ccid_change_flag,
  global_attribute1,
  global_attribute2,
  global_attribute3,
  global_attribute4,
  global_attribute5,
  global_attribute6,
  global_attribute7,
  global_attribute8,
  global_attribute9,
  global_attribute10,
  global_attribute11,
  global_attribute12,
  global_attribute13,
  global_attribute14,
  global_attribute15,
  global_attribute16,
  global_attribute17,
  global_attribute18,
  global_attribute19,
  global_attribute20,
  global_attribute21,
  global_attribute22,
  global_attribute23,
  global_attribute24,
  global_attribute25,
  global_attribute26,
  global_attribute27,
  global_attribute28,
  global_attribute29,
  global_attribute30,
  global_attribute_category,
  KCA_OPERATION,
  'N' AS IS_DELETED_FLG,
  CAST(NULLIF(KCA_SEQ_ID, '') AS DECIMAL(36, 0)) AS KCA_SEQ_ID,
  KCA_SEQ_DATE
FROM bronze_bec_ods_stg.ra_cust_trx_line_gl_dist_all
WHERE
  kca_operation IN ('INSERT', 'UPDATE')
  AND (CUST_TRX_LINE_GL_DIST_ID, kca_seq_id) IN (
    SELECT
      CUST_TRX_LINE_GL_DIST_ID,
      MAX(kca_seq_id)
    FROM bronze_bec_ods_stg.ra_cust_trx_line_gl_dist_all
    WHERE
      kca_operation IN ('INSERT', 'UPDATE')
    GROUP BY
      CUST_TRX_LINE_GL_DIST_ID
  );
/* Soft delete */
UPDATE silver_bec_ods.ra_cust_trx_line_gl_dist_all SET IS_DELETED_FLG = 'N';
UPDATE silver_bec_ods.ra_cust_trx_line_gl_dist_all SET IS_DELETED_FLG = 'Y'
WHERE
  (
    CUST_TRX_LINE_GL_DIST_ID
  ) IN (
    SELECT
      CUST_TRX_LINE_GL_DIST_ID
    FROM bec_raw_dl_ext.ra_cust_trx_line_gl_dist_all
    WHERE
      (CUST_TRX_LINE_GL_DIST_ID, KCA_SEQ_ID) IN (
        SELECT
          CUST_TRX_LINE_GL_DIST_ID,
          MAX(KCA_SEQ_ID) AS KCA_SEQ_ID
        FROM bec_raw_dl_ext.ra_cust_trx_line_gl_dist_all
        GROUP BY
          CUST_TRX_LINE_GL_DIST_ID
      )
      AND kca_operation = 'DELETE'
  );
UPDATE bec_etl_ctrl.batch_ods_info SET last_refresh_date = CURRENT_TIMESTAMP()
WHERE
  ods_table_name = 'ra_cust_trx_line_gl_dist_all';