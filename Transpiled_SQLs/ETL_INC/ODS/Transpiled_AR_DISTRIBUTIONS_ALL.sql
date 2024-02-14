/* Delete Records */
DELETE FROM silver_bec_ods.AR_DISTRIBUTIONS_ALL
WHERE
  (source_id, source_table, source_type, line_id) IN (
    SELECT
      stg.source_id,
      stg.source_table,
      stg.source_type,
      stg.line_id
    FROM silver_bec_ods.AR_DISTRIBUTIONS_ALL AS ods, bronze_bec_ods_stg.AR_DISTRIBUTIONS_ALL AS stg
    WHERE
      ods.source_id = stg.source_id
      AND ods.source_table = stg.source_table
      AND ods.source_type = stg.source_type
      AND ods.line_id = stg.line_id
      AND stg.kca_operation IN ('INSERT', 'UPDATE')
  );
/* Insert records */
INSERT INTO silver_bec_ods.AR_DISTRIBUTIONS_ALL (
  line_id,
  source_id,
  source_table,
  source_type,
  code_combination_id,
  amount_dr,
  amount_cr,
  acctd_amount_dr,
  acctd_amount_cr,
  creation_date,
  created_by,
  last_updated_by,
  last_update_date,
  last_update_login,
  org_id,
  source_table_secondary,
  source_id_secondary,
  currency_code,
  currency_conversion_rate,
  currency_conversion_type,
  currency_conversion_date,
  taxable_entered_dr,
  taxable_entered_cr,
  taxable_accounted_dr,
  taxable_accounted_cr,
  tax_link_id,
  third_party_id,
  third_party_sub_id,
  reversed_source_id,
  tax_code_id,
  location_segment_id,
  source_type_secondary,
  tax_group_code_id,
  ref_customer_trx_line_id,
  ref_cust_trx_line_gl_dist_id,
  ref_account_class,
  activity_bucket,
  ref_line_id,
  from_amount_dr,
  from_amount_cr,
  from_acctd_amount_dr,
  from_acctd_amount_cr,
  ref_mf_dist_flag,
  ref_dist_ccid,
  ref_prev_cust_trx_line_id,
  kca_operation,
  IS_DELETED_FLG,
  kca_seq_id,
  kca_seq_date
)
(
  SELECT
    line_id,
    source_id,
    source_table,
    source_type,
    code_combination_id,
    amount_dr,
    amount_cr,
    acctd_amount_dr,
    acctd_amount_cr,
    creation_date,
    created_by,
    last_updated_by,
    last_update_date,
    last_update_login,
    org_id,
    source_table_secondary,
    source_id_secondary,
    currency_code,
    currency_conversion_rate,
    currency_conversion_type,
    currency_conversion_date,
    taxable_entered_dr,
    taxable_entered_cr,
    taxable_accounted_dr,
    taxable_accounted_cr,
    tax_link_id,
    third_party_id,
    third_party_sub_id,
    reversed_source_id,
    tax_code_id,
    location_segment_id,
    source_type_secondary,
    tax_group_code_id,
    ref_customer_trx_line_id,
    ref_cust_trx_line_gl_dist_id,
    ref_account_class,
    activity_bucket,
    ref_line_id,
    from_amount_dr,
    from_amount_cr,
    from_acctd_amount_dr,
    from_acctd_amount_cr,
    ref_mf_dist_flag,
    ref_dist_ccid,
    ref_prev_cust_trx_line_id,
    kca_operation,
    'N' AS IS_DELETED_FLG,
    CAST(NULLIF(KCA_SEQ_ID, '') AS DECIMAL(36, 0)) AS KCA_SEQ_ID,
    kca_seq_date
  FROM bronze_bec_ods_stg.AR_DISTRIBUTIONS_ALL
  WHERE
    kca_operation IN ('INSERT', 'UPDATE')
    AND (source_id, source_table, source_type, line_id, kca_seq_id) IN (
      SELECT
        source_id,
        source_table,
        source_type,
        line_id,
        MAX(kca_seq_id)
      FROM bronze_bec_ods_stg.AR_DISTRIBUTIONS_ALL
      WHERE
        kca_operation IN ('INSERT', 'UPDATE')
      GROUP BY
        source_id,
        source_table,
        source_type,
        line_id
    )
);
/* Soft delete */
UPDATE silver_bec_ods.AR_DISTRIBUTIONS_ALL SET IS_DELETED_FLG = 'N';
UPDATE silver_bec_ods.AR_DISTRIBUTIONS_ALL SET IS_DELETED_FLG = 'Y'
WHERE
  (source_id, source_table, source_type, line_id) IN (
    SELECT
      source_id,
      source_table,
      source_type,
      line_id
    FROM bec_raw_dl_ext.AR_DISTRIBUTIONS_ALL
    WHERE
      (source_id, source_table, source_type, line_id, KCA_SEQ_ID) IN (
        SELECT
          source_id,
          source_table,
          source_type,
          line_id,
          MAX(KCA_SEQ_ID) AS KCA_SEQ_ID
        FROM bec_raw_dl_ext.AR_DISTRIBUTIONS_ALL
        GROUP BY
          source_id,
          source_table,
          source_type,
          line_id
      )
      AND kca_operation = 'DELETE'
  );
UPDATE bec_etl_ctrl.batch_ods_info SET last_refresh_date = CURRENT_TIMESTAMP()
WHERE
  ods_table_name = 'ar_distributions_all';