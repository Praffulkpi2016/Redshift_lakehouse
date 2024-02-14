DROP table IF EXISTS silver_bec_ods.PO_DISTRIBUTIONS_ALL;
CREATE TABLE IF NOT EXISTS silver_bec_ods.PO_DISTRIBUTIONS_ALL (
  PO_DISTRIBUTION_ID DECIMAL(15, 0),
  LAST_UPDATE_DATE TIMESTAMP,
  LAST_UPDATED_BY DECIMAL(15, 0),
  PO_HEADER_ID DECIMAL(15, 0),
  PO_LINE_ID DECIMAL(15, 0),
  LINE_LOCATION_ID DECIMAL(15, 0),
  SET_OF_BOOKS_ID DECIMAL(15, 0),
  CODE_COMBINATION_ID DECIMAL(15, 0),
  QUANTITY_ORDERED DECIMAL(28, 10),
  LAST_UPDATE_LOGIN DECIMAL(15, 0),
  CREATION_DATE TIMESTAMP,
  CREATED_BY DECIMAL(15, 0),
  PO_RELEASE_ID DECIMAL(15, 0),
  QUANTITY_DELIVERED DECIMAL(28, 10),
  QUANTITY_BILLED DECIMAL(28, 10),
  QUANTITY_CANCELLED DECIMAL(28, 10),
  REQ_HEADER_REFERENCE_NUM STRING,
  REQ_LINE_REFERENCE_NUM STRING,
  REQ_DISTRIBUTION_ID DECIMAL(15, 0),
  DELIVER_TO_LOCATION_ID DECIMAL(15, 0),
  DELIVER_TO_PERSON_ID DECIMAL(9, 0),
  RATE_DATE TIMESTAMP,
  RATE DECIMAL(28, 10),
  AMOUNT_BILLED DECIMAL(28, 10),
  ACCRUED_FLAG STRING,
  ENCUMBERED_FLAG STRING,
  ENCUMBERED_AMOUNT DECIMAL(28, 10),
  UNENCUMBERED_QUANTITY DECIMAL(28, 10),
  UNENCUMBERED_AMOUNT DECIMAL(28, 10),
  FAILED_FUNDS_LOOKUP_CODE STRING,
  GL_ENCUMBERED_DATE TIMESTAMP,
  GL_ENCUMBERED_PERIOD_NAME STRING,
  GL_CANCELLED_DATE TIMESTAMP,
  DESTINATION_TYPE_CODE STRING,
  DESTINATION_ORGANIZATION_ID DECIMAL(15, 0),
  DESTINATION_SUBINVENTORY STRING,
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
  WIP_ENTITY_ID DECIMAL(15, 0),
  WIP_OPERATION_SEQ_NUM DECIMAL(15, 0),
  WIP_RESOURCE_SEQ_NUM DECIMAL(15, 0),
  WIP_REPETITIVE_SCHEDULE_ID DECIMAL(15, 0),
  WIP_LINE_ID DECIMAL(15, 0),
  BOM_RESOURCE_ID DECIMAL(15, 0),
  BUDGET_ACCOUNT_ID DECIMAL(15, 0),
  ACCRUAL_ACCOUNT_ID DECIMAL(15, 0),
  VARIANCE_ACCOUNT_ID DECIMAL(15, 0),
  PREVENT_ENCUMBRANCE_FLAG STRING,
  USSGL_TRANSACTION_CODE STRING,
  GOVERNMENT_CONTEXT STRING,
  DESTINATION_CONTEXT STRING,
  DISTRIBUTION_NUM DECIMAL(15, 0),
  SOURCE_DISTRIBUTION_ID DECIMAL(15, 0),
  REQUEST_ID DECIMAL(15, 0),
  PROGRAM_APPLICATION_ID DECIMAL(15, 0),
  PROGRAM_ID DECIMAL(15, 0),
  PROGRAM_UPDATE_DATE TIMESTAMP,
  PROJECT_ID DECIMAL(15, 0),
  TASK_ID DECIMAL(15, 0),
  EXPENDITURE_TYPE STRING,
  PROJECT_ACCOUNTING_CONTEXT STRING,
  EXPENDITURE_ORGANIZATION_ID DECIMAL(15, 0),
  GL_CLOSED_DATE TIMESTAMP,
  ACCRUE_ON_RECEIPT_FLAG STRING,
  EXPENDITURE_ITEM_DATE TIMESTAMP,
  ORG_ID DECIMAL(15, 0),
  KANBAN_CARD_ID DECIMAL(15, 0),
  AWARD_ID DECIMAL(15, 0),
  MRC_RATE_DATE STRING,
  MRC_RATE STRING,
  MRC_ENCUMBERED_AMOUNT STRING,
  MRC_UNENCUMBERED_AMOUNT STRING,
  END_ITEM_UNIT_NUMBER STRING,
  TAX_RECOVERY_OVERRIDE_FLAG STRING,
  RECOVERABLE_TAX DECIMAL(28, 10),
  NONRECOVERABLE_TAX DECIMAL(28, 10),
  RECOVERY_RATE DECIMAL(28, 10),
  OKE_CONTRACT_LINE_ID DECIMAL(15, 0),
  OKE_CONTRACT_DELIVERABLE_ID DECIMAL(15, 0),
  AMOUNT_ORDERED DECIMAL(28, 10),
  AMOUNT_DELIVERED DECIMAL(28, 10),
  AMOUNT_CANCELLED DECIMAL(28, 10),
  DISTRIBUTION_TYPE STRING,
  AMOUNT_TO_ENCUMBER DECIMAL(28, 10),
  INVOICE_ADJUSTMENT_FLAG STRING,
  DEST_CHARGE_ACCOUNT_ID DECIMAL(15, 0),
  DEST_VARIANCE_ACCOUNT_ID DECIMAL(15, 0),
  QUANTITY_FINANCED DECIMAL(28, 10),
  AMOUNT_FINANCED DECIMAL(28, 10),
  QUANTITY_RECOUPED DECIMAL(28, 10),
  AMOUNT_RECOUPED DECIMAL(28, 10),
  RETAINAGE_WITHHELD_AMOUNT DECIMAL(28, 10),
  RETAINAGE_RELEASED_AMOUNT DECIMAL(28, 10),
  WF_ITEM_KEY STRING,
  INVOICED_VAL_IN_NTFN DECIMAL(28, 10),
  TAX_ATTRIBUTE_UPDATE_CODE STRING,
  EVENT_ID DECIMAL(15, 0),
  INTERFACE_DISTRIBUTION_REF STRING,
  LCM_FLAG STRING,
  group_line_id DECIMAL(15, 0),
  uda_template_id DECIMAL(15, 0),
  draft_id DECIMAL(15, 0),
  amount_funded DECIMAL(28, 10),
  funded_value DECIMAL(28, 10),
  partial_funded_flag STRING,
  quantity_funded DECIMAL(28, 10),
  clm_misc_loa STRING,
  clm_defence_funding STRING,
  clm_fms_case_number STRING,
  clm_agency_acct_identifier STRING,
  change_in_funded_value DECIMAL(28, 10),
  acrn STRING,
  revision_num DECIMAL(28, 10),
  amount_reversed DECIMAL(28, 10),
  clm_payment_sequence_num DECIMAL(28, 10),
  amount_changed_flag STRING,
  global_attribute_category STRING,
  global_attribute1 STRING,
  global_attribute2 STRING,
  global_attribute3 STRING,
  global_attribute4 STRING,
  global_attribute5 STRING,
  global_attribute6 STRING,
  global_attribute7 STRING,
  global_attribute8 STRING,
  global_attribute9 STRING,
  global_attribute10 STRING,
  global_attribute11 STRING,
  global_attribute12 STRING,
  global_attribute13 STRING,
  global_attribute14 STRING,
  global_attribute15 STRING,
  global_attribute16 STRING,
  global_attribute17 STRING,
  global_attribute18 STRING,
  global_attribute19 STRING,
  global_attribute20 STRING,
  KCA_OPERATION STRING,
  IS_DELETED_FLG STRING,
  kca_seq_id DECIMAL(36, 0),
  kca_seq_date TIMESTAMP
);
INSERT INTO silver_bec_ods.PO_DISTRIBUTIONS_ALL (
  PO_DISTRIBUTION_ID,
  LAST_UPDATE_DATE,
  LAST_UPDATED_BY,
  PO_HEADER_ID,
  PO_LINE_ID,
  LINE_LOCATION_ID,
  SET_OF_BOOKS_ID,
  CODE_COMBINATION_ID,
  QUANTITY_ORDERED,
  LAST_UPDATE_LOGIN,
  CREATION_DATE,
  CREATED_BY,
  PO_RELEASE_ID,
  QUANTITY_DELIVERED,
  QUANTITY_BILLED,
  QUANTITY_CANCELLED,
  REQ_HEADER_REFERENCE_NUM,
  REQ_LINE_REFERENCE_NUM,
  REQ_DISTRIBUTION_ID,
  DELIVER_TO_LOCATION_ID,
  DELIVER_TO_PERSON_ID,
  RATE_DATE,
  RATE,
  AMOUNT_BILLED,
  ACCRUED_FLAG,
  ENCUMBERED_FLAG,
  ENCUMBERED_AMOUNT,
  UNENCUMBERED_QUANTITY,
  UNENCUMBERED_AMOUNT,
  FAILED_FUNDS_LOOKUP_CODE,
  GL_ENCUMBERED_DATE,
  GL_ENCUMBERED_PERIOD_NAME,
  GL_CANCELLED_DATE,
  DESTINATION_TYPE_CODE,
  DESTINATION_ORGANIZATION_ID,
  DESTINATION_SUBINVENTORY,
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
  WIP_ENTITY_ID,
  WIP_OPERATION_SEQ_NUM,
  WIP_RESOURCE_SEQ_NUM,
  WIP_REPETITIVE_SCHEDULE_ID,
  WIP_LINE_ID,
  BOM_RESOURCE_ID,
  BUDGET_ACCOUNT_ID,
  ACCRUAL_ACCOUNT_ID,
  VARIANCE_ACCOUNT_ID,
  PREVENT_ENCUMBRANCE_FLAG,
  USSGL_TRANSACTION_CODE,
  GOVERNMENT_CONTEXT,
  DESTINATION_CONTEXT,
  DISTRIBUTION_NUM,
  SOURCE_DISTRIBUTION_ID,
  REQUEST_ID,
  PROGRAM_APPLICATION_ID,
  PROGRAM_ID,
  PROGRAM_UPDATE_DATE,
  PROJECT_ID,
  TASK_ID,
  EXPENDITURE_TYPE,
  PROJECT_ACCOUNTING_CONTEXT,
  EXPENDITURE_ORGANIZATION_ID,
  GL_CLOSED_DATE,
  ACCRUE_ON_RECEIPT_FLAG,
  EXPENDITURE_ITEM_DATE,
  ORG_ID,
  KANBAN_CARD_ID,
  AWARD_ID,
  MRC_RATE_DATE,
  MRC_RATE,
  MRC_ENCUMBERED_AMOUNT,
  MRC_UNENCUMBERED_AMOUNT,
  END_ITEM_UNIT_NUMBER,
  TAX_RECOVERY_OVERRIDE_FLAG,
  RECOVERABLE_TAX,
  NONRECOVERABLE_TAX,
  RECOVERY_RATE,
  OKE_CONTRACT_LINE_ID,
  OKE_CONTRACT_DELIVERABLE_ID,
  AMOUNT_ORDERED,
  AMOUNT_DELIVERED,
  AMOUNT_CANCELLED,
  DISTRIBUTION_TYPE,
  AMOUNT_TO_ENCUMBER,
  INVOICE_ADJUSTMENT_FLAG,
  DEST_CHARGE_ACCOUNT_ID,
  DEST_VARIANCE_ACCOUNT_ID,
  QUANTITY_FINANCED,
  AMOUNT_FINANCED,
  QUANTITY_RECOUPED,
  AMOUNT_RECOUPED,
  RETAINAGE_WITHHELD_AMOUNT,
  RETAINAGE_RELEASED_AMOUNT,
  WF_ITEM_KEY,
  INVOICED_VAL_IN_NTFN,
  TAX_ATTRIBUTE_UPDATE_CODE,
  INTERFACE_DISTRIBUTION_REF,
  LCM_FLAG,
  group_line_id,
  uda_template_id,
  draft_id,
  amount_funded,
  funded_value,
  partial_funded_flag,
  quantity_funded,
  clm_misc_loa,
  clm_defence_funding,
  clm_fms_case_number,
  clm_agency_acct_identifier,
  change_in_funded_value,
  acrn,
  revision_num,
  amount_reversed,
  clm_payment_sequence_num,
  amount_changed_flag,
  global_attribute_category,
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
  KCA_OPERATION,
  IS_DELETED_FLG,
  kca_seq_id,
  kca_seq_date
)
(
  SELECT
    PO_DISTRIBUTION_ID,
    LAST_UPDATE_DATE,
    LAST_UPDATED_BY,
    PO_HEADER_ID,
    PO_LINE_ID,
    LINE_LOCATION_ID,
    SET_OF_BOOKS_ID,
    CODE_COMBINATION_ID,
    QUANTITY_ORDERED,
    LAST_UPDATE_LOGIN,
    CREATION_DATE,
    CREATED_BY,
    PO_RELEASE_ID,
    QUANTITY_DELIVERED,
    QUANTITY_BILLED,
    QUANTITY_CANCELLED,
    REQ_HEADER_REFERENCE_NUM,
    REQ_LINE_REFERENCE_NUM,
    REQ_DISTRIBUTION_ID,
    DELIVER_TO_LOCATION_ID,
    DELIVER_TO_PERSON_ID,
    RATE_DATE,
    RATE,
    AMOUNT_BILLED,
    ACCRUED_FLAG,
    ENCUMBERED_FLAG,
    ENCUMBERED_AMOUNT,
    UNENCUMBERED_QUANTITY,
    UNENCUMBERED_AMOUNT,
    FAILED_FUNDS_LOOKUP_CODE,
    GL_ENCUMBERED_DATE,
    GL_ENCUMBERED_PERIOD_NAME,
    GL_CANCELLED_DATE,
    DESTINATION_TYPE_CODE,
    DESTINATION_ORGANIZATION_ID,
    DESTINATION_SUBINVENTORY,
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
    WIP_ENTITY_ID,
    WIP_OPERATION_SEQ_NUM,
    WIP_RESOURCE_SEQ_NUM,
    WIP_REPETITIVE_SCHEDULE_ID,
    WIP_LINE_ID,
    BOM_RESOURCE_ID,
    BUDGET_ACCOUNT_ID,
    ACCRUAL_ACCOUNT_ID,
    VARIANCE_ACCOUNT_ID,
    PREVENT_ENCUMBRANCE_FLAG,
    USSGL_TRANSACTION_CODE,
    GOVERNMENT_CONTEXT,
    DESTINATION_CONTEXT,
    DISTRIBUTION_NUM,
    SOURCE_DISTRIBUTION_ID,
    REQUEST_ID,
    PROGRAM_APPLICATION_ID,
    PROGRAM_ID,
    PROGRAM_UPDATE_DATE,
    PROJECT_ID,
    TASK_ID,
    EXPENDITURE_TYPE,
    PROJECT_ACCOUNTING_CONTEXT,
    EXPENDITURE_ORGANIZATION_ID,
    GL_CLOSED_DATE,
    ACCRUE_ON_RECEIPT_FLAG,
    EXPENDITURE_ITEM_DATE,
    ORG_ID,
    KANBAN_CARD_ID,
    AWARD_ID,
    MRC_RATE_DATE,
    MRC_RATE,
    MRC_ENCUMBERED_AMOUNT,
    MRC_UNENCUMBERED_AMOUNT,
    END_ITEM_UNIT_NUMBER,
    TAX_RECOVERY_OVERRIDE_FLAG,
    RECOVERABLE_TAX,
    NONRECOVERABLE_TAX,
    RECOVERY_RATE,
    OKE_CONTRACT_LINE_ID,
    OKE_CONTRACT_DELIVERABLE_ID,
    AMOUNT_ORDERED,
    AMOUNT_DELIVERED,
    AMOUNT_CANCELLED,
    DISTRIBUTION_TYPE,
    AMOUNT_TO_ENCUMBER,
    INVOICE_ADJUSTMENT_FLAG,
    DEST_CHARGE_ACCOUNT_ID,
    DEST_VARIANCE_ACCOUNT_ID,
    QUANTITY_FINANCED,
    AMOUNT_FINANCED,
    QUANTITY_RECOUPED,
    AMOUNT_RECOUPED,
    RETAINAGE_WITHHELD_AMOUNT,
    RETAINAGE_RELEASED_AMOUNT,
    WF_ITEM_KEY,
    INVOICED_VAL_IN_NTFN,
    TAX_ATTRIBUTE_UPDATE_CODE,
    INTERFACE_DISTRIBUTION_REF,
    LCM_FLAG,
    group_line_id,
    uda_template_id,
    draft_id,
    amount_funded,
    funded_value,
    partial_funded_flag,
    quantity_funded,
    clm_misc_loa,
    clm_defence_funding,
    clm_fms_case_number,
    clm_agency_acct_identifier,
    change_in_funded_value,
    acrn,
    revision_num,
    amount_reversed,
    clm_payment_sequence_num,
    amount_changed_flag,
    global_attribute_category,
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
    KCA_OPERATION,
    'N' AS IS_DELETED_FLG,
    CAST(NULLIF(KCA_SEQ_ID, '') AS DECIMAL(36, 0)) AS KCA_SEQ_ID,
    kca_seq_date
  FROM bronze_bec_ods_stg.PO_DISTRIBUTIONS_ALL
);
UPDATE bec_etl_ctrl.batch_ods_info SET load_type = 'I', last_refresh_date = CURRENT_TIMESTAMP()
WHERE
  ods_table_name = 'po_distributions_all';