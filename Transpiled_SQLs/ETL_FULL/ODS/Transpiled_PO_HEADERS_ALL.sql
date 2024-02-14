DROP table IF EXISTS silver_bec_ods.PO_HEADERS_ALL;
CREATE TABLE IF NOT EXISTS silver_bec_ods.PO_HEADERS_ALL (
  PO_HEADER_ID DECIMAL(15, 0),
  AGENT_ID DECIMAL(9, 0),
  TYPE_LOOKUP_CODE STRING,
  LAST_UPDATE_DATE TIMESTAMP,
  LAST_UPDATED_BY DECIMAL(15, 0),
  SEGMENT1 STRING,
  SUMMARY_FLAG STRING,
  ENABLED_FLAG STRING,
  SEGMENT2 STRING,
  SEGMENT3 STRING,
  SEGMENT4 STRING,
  SEGMENT5 STRING,
  START_DATE_ACTIVE TIMESTAMP,
  END_DATE_ACTIVE TIMESTAMP,
  LAST_UPDATE_LOGIN DECIMAL(15, 0),
  CREATION_DATE TIMESTAMP,
  CREATED_BY DECIMAL(15, 0),
  VENDOR_ID DECIMAL(15, 0),
  VENDOR_SITE_ID DECIMAL(15, 0),
  VENDOR_CONTACT_ID DECIMAL(15, 0),
  SHIP_TO_LOCATION_ID DECIMAL(15, 0),
  BILL_TO_LOCATION_ID DECIMAL(15, 0),
  TERMS_ID DECIMAL(15, 0),
  SHIP_VIA_LOOKUP_CODE STRING,
  FOB_LOOKUP_CODE STRING,
  FREIGHT_TERMS_LOOKUP_CODE STRING,
  STATUS_LOOKUP_CODE STRING,
  CURRENCY_CODE STRING,
  RATE_TYPE STRING,
  RATE_DATE TIMESTAMP,
  RATE DECIMAL(28, 10),
  FROM_HEADER_ID DECIMAL(15, 0),
  FROM_TYPE_LOOKUP_CODE STRING,
  START_DATE TIMESTAMP,
  END_DATE TIMESTAMP,
  BLANKET_TOTAL_AMOUNT DECIMAL(28, 10),
  AUTHORIZATION_STATUS STRING,
  REVISION_NUM DECIMAL(28, 10),
  REVISED_DATE TIMESTAMP,
  APPROVED_FLAG STRING,
  APPROVED_DATE TIMESTAMP,
  AMOUNT_LIMIT DECIMAL(28, 10),
  MIN_RELEASE_AMOUNT DECIMAL(28, 10),
  NOTE_TO_AUTHORIZER STRING,
  NOTE_TO_VENDOR STRING,
  NOTE_TO_RECEIVER STRING,
  PRINT_COUNT DECIMAL(28, 10),
  PRINTED_DATE TIMESTAMP,
  VENDOR_ORDER_NUM STRING,
  CONFIRMING_ORDER_FLAG STRING,
  COMMENTS STRING,
  REPLY_DATE TIMESTAMP,
  REPLY_METHOD_LOOKUP_CODE STRING,
  RFQ_CLOSE_DATE TIMESTAMP,
  QUOTE_TYPE_LOOKUP_CODE STRING,
  QUOTATION_CLASS_CODE STRING,
  QUOTE_WARNING_DELAY_UNIT STRING,
  QUOTE_WARNING_DELAY DECIMAL(28, 10),
  QUOTE_VENDOR_QUOTE_NUMBER STRING,
  ACCEPTANCE_REQUIRED_FLAG STRING,
  ACCEPTANCE_DUE_DATE TIMESTAMP,
  CLOSED_DATE TIMESTAMP,
  USER_HOLD_FLAG STRING,
  APPROVAL_REQUIRED_FLAG STRING,
  CANCEL_FLAG STRING,
  FIRM_STATUS_LOOKUP_CODE STRING,
  FIRM_DATE TIMESTAMP,
  FROZEN_FLAG STRING,
  ATTRIBUTE_CATEGORY STRING,
  ATTRIBUTE1 STRING,
  ATTRIBUTE2 STRING,
  ATTRIBUTE3 STRING,
  ATTRIBUTE4 STRING,
  ATTRIBUTE5 STRING,
  ATTRIBUTE6 STRING,
  ATTRIBUTE7 STRING,
  ATTRIBUTE8 STRING,
  ATTRIBUTE9 STRING,
  ATTRIBUTE10 STRING,
  ATTRIBUTE11 STRING,
  ATTRIBUTE12 STRING,
  ATTRIBUTE13 STRING,
  ATTRIBUTE14 STRING,
  ATTRIBUTE15 STRING,
  CLOSED_CODE STRING,
  USSGL_TRANSACTION_CODE STRING,
  GOVERNMENT_CONTEXT STRING,
  REQUEST_ID DECIMAL(15, 0),
  PROGRAM_APPLICATION_ID DECIMAL(15, 0),
  PROGRAM_ID DECIMAL(15, 0),
  PROGRAM_UPDATE_DATE TIMESTAMP,
  ORG_ID DECIMAL(15, 0),
  SUPPLY_AGREEMENT_FLAG STRING,
  EDI_PROCESSED_FLAG STRING,
  EDI_PROCESSED_STATUS STRING,
  GLOBAL_ATTRIBUTE_CATEGORY STRING,
  GLOBAL_ATTRIBUTE1 STRING,
  GLOBAL_ATTRIBUTE2 STRING,
  GLOBAL_ATTRIBUTE3 STRING,
  GLOBAL_ATTRIBUTE4 STRING,
  GLOBAL_ATTRIBUTE5 STRING,
  GLOBAL_ATTRIBUTE6 STRING,
  GLOBAL_ATTRIBUTE7 STRING,
  GLOBAL_ATTRIBUTE8 STRING,
  GLOBAL_ATTRIBUTE9 STRING,
  GLOBAL_ATTRIBUTE10 STRING,
  GLOBAL_ATTRIBUTE11 STRING,
  GLOBAL_ATTRIBUTE12 STRING,
  GLOBAL_ATTRIBUTE13 STRING,
  GLOBAL_ATTRIBUTE14 STRING,
  GLOBAL_ATTRIBUTE15 STRING,
  GLOBAL_ATTRIBUTE16 STRING,
  GLOBAL_ATTRIBUTE17 STRING,
  GLOBAL_ATTRIBUTE18 STRING,
  GLOBAL_ATTRIBUTE19 STRING,
  GLOBAL_ATTRIBUTE20 STRING,
  INTERFACE_SOURCE_CODE STRING,
  REFERENCE_NUM STRING,
  WF_ITEM_TYPE STRING,
  WF_ITEM_KEY STRING,
  MRC_RATE_TYPE STRING,
  MRC_RATE_DATE STRING,
  MRC_RATE STRING,
  PCARD_ID DECIMAL(15, 0),
  PRICE_UPDATE_TOLERANCE DECIMAL(28, 10),
  PAY_ON_CODE STRING,
  XML_FLAG STRING,
  XML_SEND_DATE TIMESTAMP,
  XML_CHANGE_SEND_DATE TIMESTAMP,
  GLOBAL_AGREEMENT_FLAG STRING,
  CONSIGNED_CONSUMPTION_FLAG STRING,
  CBC_ACCOUNTING_DATE TIMESTAMP,
  CONSUME_REQ_DEMAND_FLAG STRING,
  CHANGE_REQUESTED_BY STRING,
  SHIPPING_CONTROL STRING,
  CONTERMS_EXIST_FLAG STRING,
  CONTERMS_ARTICLES_UPD_DATE TIMESTAMP,
  CONTERMS_DELIV_UPD_DATE TIMESTAMP,
  ENCUMBRANCE_REQUIRED STRING,
  PENDING_SIGNATURE_FLAG STRING,
  CHANGE_SUMMARY STRING,
  ENCUMBRANCE_REQUIRED_FLAG STRING,
  DOCUMENT_CREATION_METHOD STRING,
  SUBMIT_DATE TIMESTAMP,
  SUPPLIER_NOTIF_METHOD STRING,
  FAX STRING,
  EMAIL_ADDRESS STRING,
  RETRO_PRICE_COMM_UPDATES_FLAG STRING,
  RETRO_PRICE_APPLY_UPDATES_FLAG STRING,
  UPDATE_SOURCING_RULES_FLAG STRING,
  AUTO_SOURCING_FLAG STRING,
  CREATED_LANGUAGE STRING,
  CPA_REFERENCE DECIMAL(28, 10),
  LOCK_OWNER_ROLE STRING,
  LOCK_OWNER_USER_ID DECIMAL(15, 0),
  SUPPLIER_AUTH_ENABLED_FLAG STRING,
  CAT_ADMIN_AUTH_ENABLED_FLAG STRING,
  STYLE_ID DECIMAL(15, 0),
  TAX_ATTRIBUTE_UPDATE_CODE STRING,
  LAST_UPDATED_PROGRAM STRING,
  ENABLE_ALL_SITES STRING,
  PAY_WHEN_PAID STRING,
  comm_rev_num DECIMAL(15, 0),
  clm_document_number STRING,
  otm_status_code STRING,
  otm_recovery_flag STRING,
  uda_template_id DECIMAL(15, 0),
  uda_template_date TIMESTAMP,
  user_document_status STRING,
  ame_approval_id DECIMAL(15, 0),
  draft_id DECIMAL(15, 0),
  clm_effective_date TIMESTAMP,
  clm_vendor_offer_number STRING,
  clm_award_administrator STRING,
  clm_no_signed_copies_to_return DECIMAL(15, 0),
  clm_min_guarantee_award_amt DECIMAL(28, 10),
  clm_min_guar_award_amt_percent DECIMAL(28, 10),
  clm_min_order_amount DECIMAL(28, 10),
  clm_max_order_amount DECIMAL(28, 10),
  clm_amt_synced_to_agreement STRING,
  clm_amount_released DECIMAL(38, 10),
  clm_external_idv STRING,
  clm_supplier_name STRING,
  clm_supplier_site_name STRING,
  clm_source_document_id DECIMAL(15, 0),
  clm_issuing_office STRING,
  clm_cotr_office STRING,
  clm_cotr_contact STRING,
  clm_priority_code STRING,
  clm_mod_issuing_office STRING,
  clm_standard_form STRING,
  clm_document_format STRING,
  ame_transaction_type STRING,
  clm_award_type STRING,
  clm_contract_officer DECIMAL(38, 10),
  clm_closeout_status STRING,
  umbrella_program_id DECIMAL(15, 0),
  fon_ref_id DECIMAL(15, 0),
  clm_default_dist_flag STRING,
  clm_edagen_date TIMESTAMP,
  clm_contract_finance_code STRING,
  clm_payment_instr_code STRING,
  clm_special_contract_type STRING, /* ,igt_document_number string   */ /* ,igt_gtnc_number string   */ /* ,igt_status string   */ /* ,agreement_type string   */ /* ,assisted_acquisition_ind string   */ /* ,advance_payment_ind string   */ /* ,enforce_total_amt_ind string   */ /* ,termination_days NUMERIC(28,10)   */ /* ,total_remaining_amt NUMERIC(28,10)   */ /* ,igt_business_txn_id string   */ /* ,order_originating_ind string   */
  KCA_OPERATION STRING,
  IS_DELETED_FLG STRING,
  kca_seq_id DECIMAL(36, 0),
  kca_seq_date TIMESTAMP
);
INSERT INTO silver_bec_ods.po_headers_all (
  PO_HEADER_ID,
  AGENT_ID,
  TYPE_LOOKUP_CODE,
  LAST_UPDATE_DATE,
  LAST_UPDATED_BY,
  SEGMENT1,
  SUMMARY_FLAG,
  ENABLED_FLAG,
  SEGMENT2,
  SEGMENT3,
  SEGMENT4,
  SEGMENT5,
  START_DATE_ACTIVE,
  END_DATE_ACTIVE,
  LAST_UPDATE_LOGIN,
  CREATION_DATE,
  CREATED_BY,
  VENDOR_ID,
  VENDOR_SITE_ID,
  VENDOR_CONTACT_ID,
  SHIP_TO_LOCATION_ID,
  BILL_TO_LOCATION_ID,
  TERMS_ID,
  SHIP_VIA_LOOKUP_CODE,
  FOB_LOOKUP_CODE,
  FREIGHT_TERMS_LOOKUP_CODE,
  STATUS_LOOKUP_CODE,
  CURRENCY_CODE,
  RATE_TYPE,
  RATE_DATE,
  RATE,
  FROM_HEADER_ID,
  FROM_TYPE_LOOKUP_CODE,
  START_DATE,
  END_DATE,
  BLANKET_TOTAL_AMOUNT,
  AUTHORIZATION_STATUS,
  REVISION_NUM,
  REVISED_DATE,
  APPROVED_FLAG,
  APPROVED_DATE,
  AMOUNT_LIMIT,
  MIN_RELEASE_AMOUNT,
  NOTE_TO_AUTHORIZER,
  NOTE_TO_VENDOR,
  NOTE_TO_RECEIVER,
  PRINT_COUNT,
  PRINTED_DATE,
  VENDOR_ORDER_NUM,
  CONFIRMING_ORDER_FLAG,
  COMMENTS,
  REPLY_DATE,
  REPLY_METHOD_LOOKUP_CODE,
  RFQ_CLOSE_DATE,
  QUOTE_TYPE_LOOKUP_CODE,
  QUOTATION_CLASS_CODE,
  QUOTE_WARNING_DELAY_UNIT,
  QUOTE_WARNING_DELAY,
  QUOTE_VENDOR_QUOTE_NUMBER,
  ACCEPTANCE_REQUIRED_FLAG,
  ACCEPTANCE_DUE_DATE,
  CLOSED_DATE,
  USER_HOLD_FLAG,
  APPROVAL_REQUIRED_FLAG,
  CANCEL_FLAG,
  FIRM_STATUS_LOOKUP_CODE,
  FIRM_DATE,
  FROZEN_FLAG,
  ATTRIBUTE_CATEGORY,
  ATTRIBUTE1,
  ATTRIBUTE2,
  ATTRIBUTE3,
  ATTRIBUTE4,
  ATTRIBUTE5,
  ATTRIBUTE6,
  ATTRIBUTE7,
  ATTRIBUTE8,
  ATTRIBUTE9,
  ATTRIBUTE10,
  ATTRIBUTE11,
  ATTRIBUTE12,
  ATTRIBUTE13,
  ATTRIBUTE14,
  ATTRIBUTE15,
  CLOSED_CODE,
  USSGL_TRANSACTION_CODE,
  GOVERNMENT_CONTEXT,
  REQUEST_ID,
  PROGRAM_APPLICATION_ID,
  PROGRAM_ID,
  PROGRAM_UPDATE_DATE,
  ORG_ID,
  SUPPLY_AGREEMENT_FLAG,
  EDI_PROCESSED_FLAG,
  EDI_PROCESSED_STATUS,
  GLOBAL_ATTRIBUTE_CATEGORY,
  GLOBAL_ATTRIBUTE1,
  GLOBAL_ATTRIBUTE2,
  GLOBAL_ATTRIBUTE3,
  GLOBAL_ATTRIBUTE4,
  GLOBAL_ATTRIBUTE5,
  GLOBAL_ATTRIBUTE6,
  GLOBAL_ATTRIBUTE7,
  GLOBAL_ATTRIBUTE8,
  GLOBAL_ATTRIBUTE9,
  GLOBAL_ATTRIBUTE10,
  GLOBAL_ATTRIBUTE11,
  GLOBAL_ATTRIBUTE12,
  GLOBAL_ATTRIBUTE13,
  GLOBAL_ATTRIBUTE14,
  GLOBAL_ATTRIBUTE15,
  GLOBAL_ATTRIBUTE16,
  GLOBAL_ATTRIBUTE17,
  GLOBAL_ATTRIBUTE18,
  GLOBAL_ATTRIBUTE19,
  GLOBAL_ATTRIBUTE20,
  INTERFACE_SOURCE_CODE,
  REFERENCE_NUM,
  WF_ITEM_TYPE,
  WF_ITEM_KEY,
  MRC_RATE_TYPE,
  MRC_RATE_DATE,
  MRC_RATE,
  PCARD_ID,
  PRICE_UPDATE_TOLERANCE,
  PAY_ON_CODE,
  XML_FLAG,
  XML_SEND_DATE,
  XML_CHANGE_SEND_DATE,
  GLOBAL_AGREEMENT_FLAG,
  CONSIGNED_CONSUMPTION_FLAG,
  CBC_ACCOUNTING_DATE,
  CONSUME_REQ_DEMAND_FLAG,
  CHANGE_REQUESTED_BY,
  SHIPPING_CONTROL,
  CONTERMS_EXIST_FLAG,
  CONTERMS_ARTICLES_UPD_DATE,
  CONTERMS_DELIV_UPD_DATE, /* ,ENCUMBRANCE_REQUIRED */
  PENDING_SIGNATURE_FLAG,
  CHANGE_SUMMARY,
  ENCUMBRANCE_REQUIRED_FLAG,
  DOCUMENT_CREATION_METHOD,
  SUBMIT_DATE,
  SUPPLIER_NOTIF_METHOD,
  FAX,
  EMAIL_ADDRESS,
  RETRO_PRICE_COMM_UPDATES_FLAG,
  RETRO_PRICE_APPLY_UPDATES_FLAG,
  UPDATE_SOURCING_RULES_FLAG,
  AUTO_SOURCING_FLAG,
  CREATED_LANGUAGE,
  CPA_REFERENCE,
  LOCK_OWNER_ROLE,
  LOCK_OWNER_USER_ID,
  SUPPLIER_AUTH_ENABLED_FLAG,
  CAT_ADMIN_AUTH_ENABLED_FLAG,
  STYLE_ID,
  TAX_ATTRIBUTE_UPDATE_CODE,
  LAST_UPDATED_PROGRAM,
  ENABLE_ALL_SITES,
  PAY_WHEN_PAID,
  comm_rev_num,
  clm_document_number,
  otm_status_code,
  otm_recovery_flag,
  uda_template_id,
  uda_template_date,
  user_document_status,
  ame_approval_id,
  draft_id,
  clm_effective_date,
  clm_vendor_offer_number,
  clm_award_administrator,
  clm_no_signed_copies_to_return,
  clm_min_guarantee_award_amt,
  clm_min_guar_award_amt_percent,
  clm_min_order_amount,
  clm_max_order_amount,
  clm_amt_synced_to_agreement,
  clm_amount_released,
  clm_external_idv,
  clm_supplier_name,
  clm_supplier_site_name,
  clm_source_document_id,
  clm_issuing_office,
  clm_cotr_office,
  clm_cotr_contact,
  clm_priority_code,
  clm_mod_issuing_office,
  clm_standard_form,
  clm_document_format,
  ame_transaction_type,
  clm_award_type,
  clm_contract_officer,
  clm_closeout_status,
  umbrella_program_id,
  fon_ref_id,
  clm_default_dist_flag,
  clm_edagen_date,
  clm_contract_finance_code,
  clm_payment_instr_code,
  clm_special_contract_type,
  KCA_OPERATION,
  IS_DELETED_FLG,
  kca_seq_id,
  kca_seq_date
)
(
  SELECT
    PO_HEADER_ID,
    AGENT_ID,
    TYPE_LOOKUP_CODE,
    LAST_UPDATE_DATE,
    LAST_UPDATED_BY,
    SEGMENT1,
    SUMMARY_FLAG,
    ENABLED_FLAG,
    SEGMENT2,
    SEGMENT3,
    SEGMENT4,
    SEGMENT5,
    START_DATE_ACTIVE,
    END_DATE_ACTIVE,
    LAST_UPDATE_LOGIN,
    CREATION_DATE,
    CREATED_BY,
    VENDOR_ID,
    VENDOR_SITE_ID,
    VENDOR_CONTACT_ID,
    SHIP_TO_LOCATION_ID,
    BILL_TO_LOCATION_ID,
    TERMS_ID,
    SHIP_VIA_LOOKUP_CODE,
    FOB_LOOKUP_CODE,
    FREIGHT_TERMS_LOOKUP_CODE,
    STATUS_LOOKUP_CODE,
    CURRENCY_CODE,
    RATE_TYPE,
    RATE_DATE,
    RATE,
    FROM_HEADER_ID,
    FROM_TYPE_LOOKUP_CODE,
    START_DATE,
    END_DATE,
    BLANKET_TOTAL_AMOUNT,
    AUTHORIZATION_STATUS,
    REVISION_NUM,
    REVISED_DATE,
    APPROVED_FLAG,
    APPROVED_DATE,
    AMOUNT_LIMIT,
    MIN_RELEASE_AMOUNT,
    NOTE_TO_AUTHORIZER,
    NOTE_TO_VENDOR,
    NOTE_TO_RECEIVER,
    PRINT_COUNT,
    PRINTED_DATE,
    VENDOR_ORDER_NUM,
    CONFIRMING_ORDER_FLAG,
    COMMENTS,
    REPLY_DATE,
    REPLY_METHOD_LOOKUP_CODE,
    RFQ_CLOSE_DATE,
    QUOTE_TYPE_LOOKUP_CODE,
    QUOTATION_CLASS_CODE,
    QUOTE_WARNING_DELAY_UNIT,
    QUOTE_WARNING_DELAY,
    QUOTE_VENDOR_QUOTE_NUMBER,
    ACCEPTANCE_REQUIRED_FLAG,
    ACCEPTANCE_DUE_DATE,
    CLOSED_DATE,
    USER_HOLD_FLAG,
    APPROVAL_REQUIRED_FLAG,
    CANCEL_FLAG,
    FIRM_STATUS_LOOKUP_CODE,
    FIRM_DATE,
    FROZEN_FLAG,
    ATTRIBUTE_CATEGORY,
    ATTRIBUTE1,
    ATTRIBUTE2,
    ATTRIBUTE3,
    ATTRIBUTE4,
    ATTRIBUTE5,
    ATTRIBUTE6,
    ATTRIBUTE7,
    ATTRIBUTE8,
    ATTRIBUTE9,
    ATTRIBUTE10,
    ATTRIBUTE11,
    ATTRIBUTE12,
    ATTRIBUTE13,
    ATTRIBUTE14,
    ATTRIBUTE15,
    CLOSED_CODE,
    USSGL_TRANSACTION_CODE,
    GOVERNMENT_CONTEXT,
    REQUEST_ID,
    PROGRAM_APPLICATION_ID,
    PROGRAM_ID,
    PROGRAM_UPDATE_DATE,
    ORG_ID,
    SUPPLY_AGREEMENT_FLAG,
    EDI_PROCESSED_FLAG,
    EDI_PROCESSED_STATUS,
    GLOBAL_ATTRIBUTE_CATEGORY,
    GLOBAL_ATTRIBUTE1,
    GLOBAL_ATTRIBUTE2,
    GLOBAL_ATTRIBUTE3,
    GLOBAL_ATTRIBUTE4,
    GLOBAL_ATTRIBUTE5,
    GLOBAL_ATTRIBUTE6,
    GLOBAL_ATTRIBUTE7,
    GLOBAL_ATTRIBUTE8,
    GLOBAL_ATTRIBUTE9,
    GLOBAL_ATTRIBUTE10,
    GLOBAL_ATTRIBUTE11,
    GLOBAL_ATTRIBUTE12,
    GLOBAL_ATTRIBUTE13,
    GLOBAL_ATTRIBUTE14,
    GLOBAL_ATTRIBUTE15,
    GLOBAL_ATTRIBUTE16,
    GLOBAL_ATTRIBUTE17,
    GLOBAL_ATTRIBUTE18,
    GLOBAL_ATTRIBUTE19,
    GLOBAL_ATTRIBUTE20,
    INTERFACE_SOURCE_CODE,
    REFERENCE_NUM,
    WF_ITEM_TYPE,
    WF_ITEM_KEY,
    MRC_RATE_TYPE,
    MRC_RATE_DATE,
    MRC_RATE,
    PCARD_ID,
    PRICE_UPDATE_TOLERANCE,
    PAY_ON_CODE,
    XML_FLAG,
    XML_SEND_DATE,
    XML_CHANGE_SEND_DATE,
    GLOBAL_AGREEMENT_FLAG,
    CONSIGNED_CONSUMPTION_FLAG,
    CBC_ACCOUNTING_DATE,
    CONSUME_REQ_DEMAND_FLAG,
    CHANGE_REQUESTED_BY,
    SHIPPING_CONTROL,
    CONTERMS_EXIST_FLAG,
    CONTERMS_ARTICLES_UPD_DATE,
    CONTERMS_DELIV_UPD_DATE, /* ,ENCUMBRANCE_REQUIRED */
    PENDING_SIGNATURE_FLAG,
    CHANGE_SUMMARY,
    ENCUMBRANCE_REQUIRED_FLAG,
    DOCUMENT_CREATION_METHOD,
    SUBMIT_DATE,
    SUPPLIER_NOTIF_METHOD,
    FAX,
    EMAIL_ADDRESS,
    RETRO_PRICE_COMM_UPDATES_FLAG,
    RETRO_PRICE_APPLY_UPDATES_FLAG,
    UPDATE_SOURCING_RULES_FLAG,
    AUTO_SOURCING_FLAG,
    CREATED_LANGUAGE,
    CPA_REFERENCE,
    LOCK_OWNER_ROLE,
    LOCK_OWNER_USER_ID,
    SUPPLIER_AUTH_ENABLED_FLAG,
    CAT_ADMIN_AUTH_ENABLED_FLAG,
    STYLE_ID,
    TAX_ATTRIBUTE_UPDATE_CODE,
    LAST_UPDATED_PROGRAM,
    ENABLE_ALL_SITES,
    PAY_WHEN_PAID,
    comm_rev_num,
    clm_document_number,
    otm_status_code,
    otm_recovery_flag,
    uda_template_id,
    uda_template_date,
    user_document_status,
    ame_approval_id,
    draft_id,
    clm_effective_date,
    clm_vendor_offer_number,
    clm_award_administrator,
    clm_no_signed_copies_to_return,
    clm_min_guarantee_award_amt,
    clm_min_guar_award_amt_percent,
    clm_min_order_amount,
    clm_max_order_amount,
    clm_amt_synced_to_agreement,
    clm_amount_released,
    clm_external_idv,
    clm_supplier_name,
    clm_supplier_site_name,
    clm_source_document_id,
    clm_issuing_office,
    clm_cotr_office,
    clm_cotr_contact,
    clm_priority_code,
    clm_mod_issuing_office,
    clm_standard_form,
    clm_document_format,
    ame_transaction_type,
    clm_award_type,
    clm_contract_officer,
    clm_closeout_status,
    umbrella_program_id,
    fon_ref_id,
    clm_default_dist_flag,
    clm_edagen_date,
    clm_contract_finance_code,
    clm_payment_instr_code,
    clm_special_contract_type,
    KCA_OPERATION, /* igt_document_number, */ /* igt_gtnc_number, */ /* igt_status, */ /* agreement_type, */ /* assisted_acquisition_ind, */ /* advance_payment_ind, */ /* enforce_total_amt_ind, */ /* termination_days, */ /* total_remaining_amt, */ /* igt_business_txn_id, */ /* order_originating_ind */
    'N' AS IS_DELETED_FLG,
    CAST(NULLIF(KCA_SEQ_ID, '') AS DECIMAL(36, 0)) AS KCA_SEQ_ID,
    kca_seq_date
  FROM bronze_bec_ods_stg.po_headers_all
);
UPDATE bec_etl_ctrl.batch_ods_info SET load_type = 'I', last_refresh_date = CURRENT_TIMESTAMP()
WHERE
  ods_table_name = 'po_headers_all';