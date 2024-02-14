TRUNCATE table bronze_bec_ods_stg.AR_DISTRIBUTIONS_ALL;
INSERT INTO bronze_bec_ods_stg.AR_DISTRIBUTIONS_ALL (
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
  REF_PREV_CUST_TRX_LINE_ID,
  KCA_OPERATION,
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
    REF_PREV_CUST_TRX_LINE_ID,
    KCA_OPERATION,
    kca_seq_id,
    kca_seq_date
  FROM bec_raw_dl_ext.AR_DISTRIBUTIONS_ALL
  WHERE
    kca_operation <> 'DELETE'
    AND COALESCE(kca_seq_id, '') <> ''
    AND (SOURCE_ID, SOURCE_TABLE, SOURCE_TYPE, LINE_ID, kca_seq_id) IN (
      SELECT
        SOURCE_ID,
        SOURCE_TABLE,
        SOURCE_TYPE,
        LINE_ID,
        MAX(kca_seq_id)
      FROM bec_raw_dl_ext.AR_DISTRIBUTIONS_ALL
      WHERE
        kca_operation <> 'DELETE' AND COALESCE(kca_seq_id, '') <> ''
      GROUP BY
        SOURCE_ID,
        SOURCE_TABLE,
        SOURCE_TYPE,
        LINE_ID
    )
    AND kca_seq_date > (
      SELECT
        (
          executebegints - prune_days
        )
      FROM bec_etl_ctrl.batch_ods_info
      WHERE
        ods_table_name = 'ar_distributions_all'
    )
);