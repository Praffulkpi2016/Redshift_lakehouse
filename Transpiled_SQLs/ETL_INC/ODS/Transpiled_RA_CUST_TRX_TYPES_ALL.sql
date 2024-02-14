/* Delete Records */
DELETE FROM silver_bec_ods.RA_CUST_TRX_TYPES_ALL
WHERE
  (CUST_TRX_TYPE_ID, COALESCE(org_id, 0)) IN (
    SELECT
      stg.CUST_TRX_TYPE_ID,
      COALESCE(stg.org_id, 0) AS org_id
    FROM silver_bec_ods.ra_cust_trx_types_all AS ods, bronze_bec_ods_stg.ra_cust_trx_types_all AS stg
    WHERE
      ods.CUST_TRX_TYPE_ID = stg.CUST_TRX_TYPE_ID
      AND COALESCE(ods.org_id, 0) = COALESCE(stg.org_id, 0)
      AND stg.kca_operation IN ('INSERT', 'UPDATE')
  );
/* Insert records */
INSERT INTO silver_bec_ods.ra_cust_trx_types_all (
  cust_trx_type_id,
  last_update_date,
  last_updated_by,
  creation_date,
  created_by,
  last_update_login,
  post_to_gl,
  accounting_affect_flag,
  credit_memo_type_id,
  status,
  `name`,
  description,
  `type`,
  default_term,
  default_printing_option,
  default_status,
  gl_id_rev,
  gl_id_freight,
  gl_id_rec,
  subsequent_trx_type_id,
  set_of_books_id,
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
  allow_freight_flag,
  allow_overapplication_flag,
  creation_sign,
  end_date,
  gl_id_clearing,
  gl_id_tax,
  gl_id_unbilled,
  gl_id_unearned,
  start_date,
  tax_calculation_flag,
  attribute11,
  attribute12,
  attribute13,
  attribute14,
  attribute15,
  natural_application_only_flag,
  org_id,
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
  global_attribute_category,
  rule_set_id,
  signed_flag,
  drawee_issued_flag,
  magnetic_format_code,
  format_program_id,
  gl_id_unpaid_rec,
  gl_id_remittance,
  gl_id_factor,
  allocate_tax_freight,
  legal_entity_id,
  exclude_from_late_charges,
  adj_post_to_gl,
  kca_operation,
  is_deleted_flg,
  kca_seq_id,
  kca_seq_date
)
SELECT
  cust_trx_type_id,
  last_update_date,
  last_updated_by,
  creation_date,
  created_by,
  last_update_login,
  post_to_gl,
  accounting_affect_flag,
  credit_memo_type_id,
  status,
  `name`,
  description,
  `type`,
  default_term,
  default_printing_option,
  default_status,
  gl_id_rev,
  gl_id_freight,
  gl_id_rec,
  subsequent_trx_type_id,
  set_of_books_id,
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
  allow_freight_flag,
  allow_overapplication_flag,
  creation_sign,
  end_date,
  gl_id_clearing,
  gl_id_tax,
  gl_id_unbilled,
  gl_id_unearned,
  start_date,
  tax_calculation_flag,
  attribute11,
  attribute12,
  attribute13,
  attribute14,
  attribute15,
  natural_application_only_flag,
  org_id,
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
  global_attribute_category,
  rule_set_id,
  signed_flag,
  drawee_issued_flag,
  magnetic_format_code,
  format_program_id,
  gl_id_unpaid_rec,
  gl_id_remittance,
  gl_id_factor,
  allocate_tax_freight,
  legal_entity_id,
  exclude_from_late_charges,
  adj_post_to_gl,
  KCA_OPERATION,
  'N' AS IS_DELETED_FLG,
  CAST(NULLIF(KCA_SEQ_ID, '') AS DECIMAL(36, 0)) AS KCA_SEQ_ID,
  KCA_SEQ_DATE
FROM bronze_bec_ods_stg.ra_cust_trx_types_all
WHERE
  kca_operation IN ('INSERT', 'UPDATE')
  AND (CUST_TRX_TYPE_ID, COALESCE(org_id, 0), kca_seq_id) IN (
    SELECT
      CUST_TRX_TYPE_ID,
      COALESCE(org_id, 0),
      MAX(kca_seq_id)
    FROM bronze_bec_ods_stg.ra_cust_trx_types_all
    WHERE
      kca_operation IN ('INSERT', 'UPDATE')
    GROUP BY
      CUST_TRX_TYPE_ID,
      org_id
  );
/* Soft delete */
UPDATE silver_bec_ods.ra_cust_trx_types_all SET IS_DELETED_FLG = 'N';
UPDATE silver_bec_ods.ra_cust_trx_types_all SET IS_DELETED_FLG = 'Y'
WHERE
  (CUST_TRX_TYPE_ID, COALESCE(org_id, 0)) IN (
    SELECT
      CUST_TRX_TYPE_ID,
      COALESCE(org_id, 0)
    FROM bec_raw_dl_ext.ra_cust_trx_types_all
    WHERE
      (CUST_TRX_TYPE_ID, COALESCE(org_id, 0), KCA_SEQ_ID) IN (
        SELECT
          CUST_TRX_TYPE_ID,
          COALESCE(org_id, 0),
          MAX(KCA_SEQ_ID) AS KCA_SEQ_ID
        FROM bec_raw_dl_ext.ra_cust_trx_types_all
        GROUP BY
          CUST_TRX_TYPE_ID,
          COALESCE(org_id, 0)
      )
      AND kca_operation = 'DELETE'
  );
UPDATE bec_etl_ctrl.batch_ods_info SET last_refresh_date = CURRENT_TIMESTAMP()
WHERE
  ods_table_name = 'ra_cust_trx_types_all';