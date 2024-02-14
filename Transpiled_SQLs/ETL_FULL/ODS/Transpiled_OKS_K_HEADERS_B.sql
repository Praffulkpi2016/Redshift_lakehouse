DROP TABLE IF EXISTS silver_bec_ods.OKS_K_HEADERS_B;
CREATE TABLE IF NOT EXISTS silver_bec_ods.OKS_K_HEADERS_B (
  id STRING,
  chr_id DECIMAL(15, 0),
  acct_rule_id DECIMAL(15, 0),
  payment_type STRING,
  cc_no STRING,
  cc_expiry_date TIMESTAMP,
  cc_bank_acct_id DECIMAL(15, 0),
  cc_auth_code STRING,
  commitment_id DECIMAL(15, 0),
  grace_duration DECIMAL(38, 10),
  grace_period STRING,
  est_rev_percent DECIMAL(28, 10),
  est_rev_date TIMESTAMP,
  tax_amount DECIMAL(28, 10),
  tax_status STRING,
  tax_code DECIMAL(28, 10),
  tax_exemption_id DECIMAL(15, 0),
  billing_profile_id DECIMAL(15, 0),
  renewal_status STRING,
  electronic_renewal_flag STRING,
  quote_to_contact_id DECIMAL(15, 0),
  quote_to_site_id DECIMAL(15, 0),
  quote_to_email_id DECIMAL(15, 0),
  quote_to_phone_id DECIMAL(15, 0),
  quote_to_fax_id DECIMAL(15, 0),
  renewal_po_required STRING,
  renewal_po_number STRING,
  renewal_price_list DECIMAL(28, 10),
  renewal_pricing_type STRING,
  renewal_markup_percent DECIMAL(28, 10),
  renewal_grace_duration DECIMAL(28, 10),
  renewal_grace_period STRING,
  renewal_est_rev_percent DECIMAL(28, 10),
  renewal_est_rev_duration DECIMAL(28, 10),
  renewal_est_rev_period STRING,
  renewal_price_list_used DECIMAL(28, 10),
  renewal_type_used STRING,
  renewal_notification_to DECIMAL(28, 10),
  renewal_po_used STRING,
  renewal_pricing_type_used STRING,
  renewal_markup_percent_used DECIMAL(28, 10),
  rev_est_percent_used DECIMAL(28, 10),
  rev_est_duration_used DECIMAL(28, 10),
  rev_est_period_used STRING,
  billing_profile_used DECIMAL(28, 10),
  ern_flag_used_yn STRING,
  evn_threshold_amt DECIMAL(28, 10),
  evn_threshold_cur STRING,
  ern_threshold_amt DECIMAL(28, 10),
  ern_threshold_cur STRING,
  renewal_grace_duration_used DECIMAL(28, 10),
  renewal_grace_period_used STRING,
  inv_trx_type STRING,
  inv_print_profile STRING,
  ar_interface_yn STRING,
  hold_billing STRING,
  summary_trx_yn STRING,
  service_po_number STRING,
  service_po_required STRING,
  billing_schedule_type STRING,
  object_version_number DECIMAL(28, 10),
  security_group_id DECIMAL(15, 0),
  request_id DECIMAL(15, 0),
  created_by DECIMAL(15, 0),
  creation_date TIMESTAMP,
  last_updated_by DECIMAL(15, 0),
  last_update_date TIMESTAMP,
  last_update_login DECIMAL(15, 0),
  follow_up_action STRING,
  follow_up_date TIMESTAMP,
  trxn_extension_id DECIMAL(15, 0),
  date_accepted TIMESTAMP,
  accepted_by DECIMAL(15, 0),
  rmndr_suppress_flag STRING,
  rmndr_sent_flag STRING,
  quote_sent_flag STRING,
  process_request_id DECIMAL(15, 0),
  wf_item_key STRING,
  period_start STRING,
  period_type STRING,
  price_uom STRING,
  person_party_id DECIMAL(15, 0),
  tax_classification_code STRING,
  exempt_certificate_number STRING,
  exempt_reason_code STRING,
  approval_type_used STRING,
  renewal_comment STRING,
  cc_email_address STRING,
  KCA_OPERATION STRING,
  IS_DELETED_FLG STRING,
  kca_seq_id DECIMAL(36, 0),
  kca_seq_date TIMESTAMP
);
INSERT INTO silver_bec_ods.OKS_K_HEADERS_B (
  ID,
  CHR_ID,
  ACCT_RULE_ID,
  PAYMENT_TYPE,
  CC_NO,
  CC_EXPIRY_DATE,
  CC_BANK_ACCT_ID,
  CC_AUTH_CODE,
  COMMITMENT_ID,
  GRACE_DURATION,
  GRACE_PERIOD,
  EST_REV_PERCENT,
  EST_REV_DATE,
  TAX_AMOUNT,
  TAX_STATUS,
  TAX_CODE,
  TAX_EXEMPTION_ID,
  BILLING_PROFILE_ID,
  RENEWAL_STATUS,
  ELECTRONIC_RENEWAL_FLAG,
  QUOTE_TO_CONTACT_ID,
  QUOTE_TO_SITE_ID,
  QUOTE_TO_EMAIL_ID,
  QUOTE_TO_PHONE_ID,
  QUOTE_TO_FAX_ID,
  RENEWAL_PO_REQUIRED,
  RENEWAL_PO_NUMBER,
  RENEWAL_PRICE_LIST,
  RENEWAL_PRICING_TYPE,
  RENEWAL_MARKUP_PERCENT,
  RENEWAL_GRACE_DURATION,
  RENEWAL_GRACE_PERIOD,
  RENEWAL_EST_REV_PERCENT,
  RENEWAL_EST_REV_DURATION,
  RENEWAL_EST_REV_PERIOD,
  RENEWAL_PRICE_LIST_USED,
  RENEWAL_TYPE_USED,
  RENEWAL_NOTIFICATION_TO,
  RENEWAL_PO_USED,
  RENEWAL_PRICING_TYPE_USED,
  RENEWAL_MARKUP_PERCENT_USED,
  REV_EST_PERCENT_USED,
  REV_EST_DURATION_USED,
  REV_EST_PERIOD_USED,
  BILLING_PROFILE_USED,
  ERN_FLAG_USED_YN,
  EVN_THRESHOLD_AMT,
  EVN_THRESHOLD_CUR,
  ERN_THRESHOLD_AMT,
  ERN_THRESHOLD_CUR,
  RENEWAL_GRACE_DURATION_USED,
  RENEWAL_GRACE_PERIOD_USED,
  INV_TRX_TYPE,
  INV_PRINT_PROFILE,
  AR_INTERFACE_YN,
  HOLD_BILLING,
  SUMMARY_TRX_YN,
  SERVICE_PO_NUMBER,
  SERVICE_PO_REQUIRED,
  BILLING_SCHEDULE_TYPE,
  OBJECT_VERSION_NUMBER,
  SECURITY_GROUP_ID,
  REQUEST_ID,
  CREATED_BY,
  CREATION_DATE,
  LAST_UPDATED_BY,
  LAST_UPDATE_DATE,
  LAST_UPDATE_LOGIN,
  FOLLOW_UP_ACTION,
  FOLLOW_UP_DATE,
  TRXN_EXTENSION_ID,
  DATE_ACCEPTED,
  ACCEPTED_BY,
  RMNDR_SUPPRESS_FLAG,
  RMNDR_SENT_FLAG,
  QUOTE_SENT_FLAG,
  PROCESS_REQUEST_ID,
  WF_ITEM_KEY,
  PERIOD_START,
  PERIOD_TYPE,
  PRICE_UOM,
  PERSON_PARTY_ID,
  TAX_CLASSIFICATION_CODE,
  EXEMPT_CERTIFICATE_NUMBER,
  EXEMPT_REASON_CODE,
  APPROVAL_TYPE_USED,
  RENEWAL_COMMENT,
  CC_EMAIL_ADDRESS,
  KCA_OPERATION,
  IS_DELETED_FLG,
  kca_seq_id,
  kca_seq_date
)
SELECT
  ID,
  CHR_ID,
  ACCT_RULE_ID,
  PAYMENT_TYPE,
  CC_NO,
  CC_EXPIRY_DATE,
  CC_BANK_ACCT_ID,
  CC_AUTH_CODE,
  COMMITMENT_ID,
  GRACE_DURATION,
  GRACE_PERIOD,
  EST_REV_PERCENT,
  EST_REV_DATE,
  TAX_AMOUNT,
  TAX_STATUS,
  TAX_CODE,
  TAX_EXEMPTION_ID,
  BILLING_PROFILE_ID,
  RENEWAL_STATUS,
  ELECTRONIC_RENEWAL_FLAG,
  QUOTE_TO_CONTACT_ID,
  QUOTE_TO_SITE_ID,
  QUOTE_TO_EMAIL_ID,
  QUOTE_TO_PHONE_ID,
  QUOTE_TO_FAX_ID,
  RENEWAL_PO_REQUIRED,
  RENEWAL_PO_NUMBER,
  RENEWAL_PRICE_LIST,
  RENEWAL_PRICING_TYPE,
  RENEWAL_MARKUP_PERCENT,
  RENEWAL_GRACE_DURATION,
  RENEWAL_GRACE_PERIOD,
  RENEWAL_EST_REV_PERCENT,
  RENEWAL_EST_REV_DURATION,
  RENEWAL_EST_REV_PERIOD,
  RENEWAL_PRICE_LIST_USED,
  RENEWAL_TYPE_USED,
  RENEWAL_NOTIFICATION_TO,
  RENEWAL_PO_USED,
  RENEWAL_PRICING_TYPE_USED,
  RENEWAL_MARKUP_PERCENT_USED,
  REV_EST_PERCENT_USED,
  REV_EST_DURATION_USED,
  REV_EST_PERIOD_USED,
  BILLING_PROFILE_USED,
  ERN_FLAG_USED_YN,
  EVN_THRESHOLD_AMT,
  EVN_THRESHOLD_CUR,
  ERN_THRESHOLD_AMT,
  ERN_THRESHOLD_CUR,
  RENEWAL_GRACE_DURATION_USED,
  RENEWAL_GRACE_PERIOD_USED,
  INV_TRX_TYPE,
  INV_PRINT_PROFILE,
  AR_INTERFACE_YN,
  HOLD_BILLING,
  SUMMARY_TRX_YN,
  SERVICE_PO_NUMBER,
  SERVICE_PO_REQUIRED,
  BILLING_SCHEDULE_TYPE,
  OBJECT_VERSION_NUMBER,
  SECURITY_GROUP_ID,
  REQUEST_ID,
  CREATED_BY,
  CREATION_DATE,
  LAST_UPDATED_BY,
  LAST_UPDATE_DATE,
  LAST_UPDATE_LOGIN,
  FOLLOW_UP_ACTION,
  FOLLOW_UP_DATE,
  TRXN_EXTENSION_ID,
  DATE_ACCEPTED,
  ACCEPTED_BY,
  RMNDR_SUPPRESS_FLAG,
  RMNDR_SENT_FLAG,
  QUOTE_SENT_FLAG,
  PROCESS_REQUEST_ID,
  WF_ITEM_KEY,
  PERIOD_START,
  PERIOD_TYPE,
  PRICE_UOM,
  PERSON_PARTY_ID,
  TAX_CLASSIFICATION_CODE,
  EXEMPT_CERTIFICATE_NUMBER,
  EXEMPT_REASON_CODE,
  APPROVAL_TYPE_USED,
  RENEWAL_COMMENT,
  CC_EMAIL_ADDRESS,
  KCA_OPERATION,
  'N' AS IS_DELETED_FLG,
  CAST(NULLIF(KCA_SEQ_ID, '') AS DECIMAL(36, 0)) AS KCA_SEQ_ID,
  kca_seq_date
FROM bronze_bec_ods_stg.OKS_K_HEADERS_B;
UPDATE bec_etl_ctrl.batch_ods_info SET load_type = 'I', last_refresh_date = CURRENT_TIMESTAMP()
WHERE
  ods_table_name = 'oks_k_headers_b';