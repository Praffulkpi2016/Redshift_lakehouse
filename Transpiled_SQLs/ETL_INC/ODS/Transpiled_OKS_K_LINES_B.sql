/* Delete Records */
DELETE FROM silver_bec_ods.OKS_K_LINES_B
WHERE
  (
    COALESCE(ID, 'NA')
  ) IN (
    SELECT
      COALESCE(stg.ID, 'NA') AS ID
    FROM silver_bec_ods.OKS_K_LINES_B AS ods, bronze_bec_ods_stg.OKS_K_LINES_B AS stg
    WHERE
      COALESCE(ods.ID, 'NA') = COALESCE(stg.ID, 'NA')
      AND stg.kca_operation IN ('INSERT', 'UPDATE')
  );
/* Insert records */
INSERT INTO silver_bec_ods.OKS_K_LINES_B (
  id,
  cle_id,
  dnz_chr_id,
  discount_list,
  acct_rule_id,
  payment_type,
  cc_no,
  cc_expiry_date,
  cc_bank_acct_id,
  cc_auth_code,
  commitment_id,
  locked_price_list_id,
  usage_est_yn,
  usage_est_method,
  usage_est_start_date,
  termn_method,
  ubt_amount,
  credit_amount,
  suppressed_credit,
  override_amount,
  cust_po_number_req_yn,
  cust_po_number,
  grace_duration,
  grace_period,
  inv_print_flag,
  price_uom,
  tax_amount,
  tax_inclusive_yn,
  tax_status,
  tax_code,
  tax_exemption_id,
  ib_trans_type,
  ib_trans_date,
  prod_price,
  service_price,
  clvl_list_price,
  clvl_quantity,
  clvl_extended_amt,
  clvl_uom_code,
  toplvl_operand_code,
  toplvl_operand_val,
  toplvl_quantity,
  toplvl_uom_code,
  toplvl_adj_price,
  toplvl_price_qty,
  averaging_interval,
  settlement_interval,
  minimum_quantity,
  default_quantity,
  amcv_flag,
  fixed_quantity,
  usage_duration,
  usage_period,
  level_yn,
  usage_type,
  uom_quantified,
  base_reading,
  billing_schedule_type,
  full_credit,
  locked_price_list_line_id,
  break_uom,
  prorate,
  coverage_type,
  exception_cov_id,
  limit_uom_quantified,
  discount_amount,
  discount_percent,
  offset_duration,
  offset_period,
  incident_severity_id,
  pdf_id,
  work_thru_yn,
  react_active_yn,
  transfer_option,
  prod_upgrade_yn,
  inheritance_type,
  pm_program_id,
  pm_conf_req_yn,
  pm_sch_exists_yn,
  allow_bt_discount,
  apply_default_timezone,
  sync_date_install,
  object_version_number,
  security_group_id,
  request_id,
  created_by,
  creation_date,
  last_updated_by,
  last_update_date,
  last_update_login,
  trxn_extension_id,
  tax_classification_code,
  exempt_certificate_number,
  exempt_reason_code,
  coverage_id,
  standard_cov_yn,
  orig_system_id1,
  orig_system_reference1,
  orig_system_source_code,
  revenue_impact_date,
  counter_value_id,
  from_day,
  to_day,
  inv_organization_id,
  consolidation_yn,
  billing_option,
  usage_limit_definition,
  rollover_type,
  usage_limit_value,
  usage_grace_period,
  usage_grace_duration,
  KCA_OPERATION,
  IS_DELETED_FLG,
  kca_seq_id,
  kca_seq_date
)
(
  SELECT
    id,
    cle_id,
    dnz_chr_id,
    discount_list,
    acct_rule_id,
    payment_type,
    cc_no,
    cc_expiry_date,
    cc_bank_acct_id,
    cc_auth_code,
    commitment_id,
    locked_price_list_id,
    usage_est_yn,
    usage_est_method,
    usage_est_start_date,
    termn_method,
    ubt_amount,
    credit_amount,
    suppressed_credit,
    override_amount,
    cust_po_number_req_yn,
    cust_po_number,
    grace_duration,
    grace_period,
    inv_print_flag,
    price_uom,
    tax_amount,
    tax_inclusive_yn,
    tax_status,
    tax_code,
    tax_exemption_id,
    ib_trans_type,
    ib_trans_date,
    prod_price,
    service_price,
    clvl_list_price,
    clvl_quantity,
    clvl_extended_amt,
    clvl_uom_code,
    toplvl_operand_code,
    toplvl_operand_val,
    toplvl_quantity,
    toplvl_uom_code,
    toplvl_adj_price,
    toplvl_price_qty,
    averaging_interval,
    settlement_interval,
    minimum_quantity,
    default_quantity,
    amcv_flag,
    fixed_quantity,
    usage_duration,
    usage_period,
    level_yn,
    usage_type,
    uom_quantified,
    base_reading,
    billing_schedule_type,
    full_credit,
    locked_price_list_line_id,
    break_uom,
    prorate,
    coverage_type,
    exception_cov_id,
    limit_uom_quantified,
    discount_amount,
    discount_percent,
    offset_duration,
    offset_period,
    incident_severity_id,
    pdf_id,
    work_thru_yn,
    react_active_yn,
    transfer_option,
    prod_upgrade_yn,
    inheritance_type,
    pm_program_id,
    pm_conf_req_yn,
    pm_sch_exists_yn,
    allow_bt_discount,
    apply_default_timezone,
    sync_date_install,
    object_version_number,
    security_group_id,
    request_id,
    created_by,
    creation_date,
    last_updated_by,
    last_update_date,
    last_update_login,
    trxn_extension_id,
    tax_classification_code,
    exempt_certificate_number,
    exempt_reason_code,
    coverage_id,
    standard_cov_yn,
    orig_system_id1,
    orig_system_reference1,
    orig_system_source_code,
    revenue_impact_date,
    counter_value_id,
    from_day,
    to_day,
    inv_organization_id,
    consolidation_yn,
    billing_option,
    usage_limit_definition,
    rollover_type,
    usage_limit_value,
    usage_grace_period,
    usage_grace_duration,
    KCA_OPERATION,
    'N' AS IS_DELETED_FLG,
    CAST(NULLIF(KCA_SEQ_ID, '') AS DECIMAL(36, 0)) AS KCA_SEQ_ID,
    kca_seq_date
  FROM bronze_bec_ods_stg.OKS_K_LINES_B
  WHERE
    kca_operation IN ('INSERT', 'UPDATE')
    AND (COALESCE(ID, 'NA'), kca_seq_id) IN (
      SELECT
        COALESCE(ID, 'NA') AS ID,
        MAX(kca_seq_id)
      FROM bronze_bec_ods_stg.OKS_K_LINES_B
      WHERE
        kca_operation IN ('INSERT', 'UPDATE')
      GROUP BY
        COALESCE(ID, 'NA')
    )
);
/* Soft delete */
UPDATE silver_bec_ods.OKS_K_LINES_B SET IS_DELETED_FLG = 'N';
UPDATE silver_bec_ods.OKS_K_LINES_B SET IS_DELETED_FLG = 'Y'
WHERE
  (
    COALESCE(ID, 'NA')
  ) IN (
    SELECT
      COALESCE(ID, 'NA')
    FROM bec_raw_dl_ext.OKS_K_LINES_B
    WHERE
      (COALESCE(ID, 'NA'), KCA_SEQ_ID) IN (
        SELECT
          COALESCE(ID, 'NA'),
          MAX(KCA_SEQ_ID) AS KCA_SEQ_ID
        FROM bec_raw_dl_ext.OKS_K_LINES_B
        GROUP BY
          COALESCE(ID, 'NA')
      )
      AND kca_operation = 'DELETE'
  );
UPDATE bec_etl_ctrl.batch_ods_info SET last_refresh_date = CURRENT_TIMESTAMP()
WHERE
  ods_table_name = 'oks_k_lines_b';