/* Delete Records */
DELETE FROM silver_bec_ods.oe_transaction_types_all
WHERE
  (
    COALESCE(TRANSACTION_TYPE_ID, 0)
  ) IN (
    SELECT
      COALESCE(stg.TRANSACTION_TYPE_ID, 0) AS TRANSACTION_TYPE_ID
    FROM silver_bec_ods.oe_transaction_types_all AS ods, bronze_bec_ods_stg.oe_transaction_types_all AS stg
    WHERE
      COALESCE(ods.TRANSACTION_TYPE_ID, 0) = COALESCE(stg.TRANSACTION_TYPE_ID, 0)
      AND stg.kca_operation IN ('INSERT', 'UPDATE')
  );
/* Insert records */
INSERT INTO silver_bec_ods.oe_transaction_types_all (
  TRANSACTION_TYPE_ID,
  TRANSACTION_TYPE_CODE,
  ORDER_CATEGORY_CODE,
  START_DATE_ACTIVE,
  END_DATE_ACTIVE,
  CREATION_DATE,
  CREATED_BY,
  LAST_UPDATE_DATE,
  LAST_UPDATED_BY,
  LAST_UPDATE_LOGIN,
  PROGRAM_APPLICATION_ID,
  PROGRAM_ID,
  REQUEST_ID,
  CURRENCY_CODE,
  CONVERSION_TYPE_CODE,
  CUST_TRX_TYPE_ID,
  COST_OF_GOODS_SOLD_ACCOUNT,
  ENTRY_CREDIT_CHECK_RULE_ID,
  SHIPPING_CREDIT_CHECK_RULE_ID,
  PRICE_LIST_ID,
  ENFORCE_LINE_PRICES_FLAG,
  WAREHOUSE_ID,
  DEMAND_CLASS_CODE,
  SHIPMENT_PRIORITY_CODE,
  SHIPPING_METHOD_CODE,
  FREIGHT_TERMS_CODE,
  FOB_POINT_CODE,
  SHIP_SOURCE_TYPE_CODE,
  AGREEMENT_TYPE_CODE,
  AGREEMENT_REQUIRED_FLAG,
  PO_REQUIRED_FLAG,
  INVOICING_RULE_ID,
  INVOICING_CREDIT_METHOD_CODE,
  ACCOUNTING_RULE_ID,
  ACCOUNTING_CREDIT_METHOD_CODE,
  INVOICE_SOURCE_ID,
  NON_DELIVERY_INVOICE_SOURCE_ID,
  INSPECTION_REQUIRED_FLAG,
  DEPOT_REPAIR_CODE,
  ORG_ID,
  AUTO_SCHEDULING_FLAG,
  SCHEDULING_LEVEL_CODE,
  CONTEXT,
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
  DEFAULT_INBOUND_LINE_TYPE_ID,
  DEFAULT_OUTBOUND_LINE_TYPE_ID,
  TAX_CALCULATION_EVENT_CODE,
  PICKING_CREDIT_CHECK_RULE_ID,
  PACKING_CREDIT_CHECK_RULE_ID,
  MIN_MARGIN_PERCENT,
  SALES_DOCUMENT_TYPE_CODE,
  DEFAULT_LINE_SET_CODE,
  DEFAULT_FULFILLMENT_SET,
  DEF_TRANSACTION_PHASE_CODE,
  QUOTE_NUM_AS_ORD_NUM_FLAG,
  LAYOUT_TEMPLATE_ID,
  CONTRACT_TEMPLATE_ID,
  CREDIT_CARD_REV_REAUTH_CODE,
  USE_AME_APPROVAL,
  BILL_ONLY,
  KCA_OPERATION,
  IS_DELETED_FLG,
  kca_seq_id,
  kca_seq_date
)
(
  SELECT
    TRANSACTION_TYPE_ID,
    TRANSACTION_TYPE_CODE,
    ORDER_CATEGORY_CODE,
    START_DATE_ACTIVE,
    END_DATE_ACTIVE,
    CREATION_DATE,
    CREATED_BY,
    LAST_UPDATE_DATE,
    LAST_UPDATED_BY,
    LAST_UPDATE_LOGIN,
    PROGRAM_APPLICATION_ID,
    PROGRAM_ID,
    REQUEST_ID,
    CURRENCY_CODE,
    CONVERSION_TYPE_CODE,
    CUST_TRX_TYPE_ID,
    COST_OF_GOODS_SOLD_ACCOUNT,
    ENTRY_CREDIT_CHECK_RULE_ID,
    SHIPPING_CREDIT_CHECK_RULE_ID,
    PRICE_LIST_ID,
    ENFORCE_LINE_PRICES_FLAG,
    WAREHOUSE_ID,
    DEMAND_CLASS_CODE,
    SHIPMENT_PRIORITY_CODE,
    SHIPPING_METHOD_CODE,
    FREIGHT_TERMS_CODE,
    FOB_POINT_CODE,
    SHIP_SOURCE_TYPE_CODE,
    AGREEMENT_TYPE_CODE,
    AGREEMENT_REQUIRED_FLAG,
    PO_REQUIRED_FLAG,
    INVOICING_RULE_ID,
    INVOICING_CREDIT_METHOD_CODE,
    ACCOUNTING_RULE_ID,
    ACCOUNTING_CREDIT_METHOD_CODE,
    INVOICE_SOURCE_ID,
    NON_DELIVERY_INVOICE_SOURCE_ID,
    INSPECTION_REQUIRED_FLAG,
    DEPOT_REPAIR_CODE,
    ORG_ID,
    AUTO_SCHEDULING_FLAG,
    SCHEDULING_LEVEL_CODE,
    CONTEXT,
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
    DEFAULT_INBOUND_LINE_TYPE_ID,
    DEFAULT_OUTBOUND_LINE_TYPE_ID,
    TAX_CALCULATION_EVENT_CODE,
    PICKING_CREDIT_CHECK_RULE_ID,
    PACKING_CREDIT_CHECK_RULE_ID,
    MIN_MARGIN_PERCENT,
    SALES_DOCUMENT_TYPE_CODE,
    DEFAULT_LINE_SET_CODE,
    DEFAULT_FULFILLMENT_SET,
    DEF_TRANSACTION_PHASE_CODE,
    QUOTE_NUM_AS_ORD_NUM_FLAG,
    LAYOUT_TEMPLATE_ID,
    CONTRACT_TEMPLATE_ID,
    CREDIT_CARD_REV_REAUTH_CODE,
    USE_AME_APPROVAL,
    BILL_ONLY,
    KCA_OPERATION,
    'N' AS IS_DELETED_FLG,
    CAST(NULLIF(KCA_SEQ_ID, '') AS DECIMAL(36, 0)) AS KCA_SEQ_ID,
    KCA_SEQ_DATE
  FROM bronze_bec_ods_stg.oe_transaction_types_all
  WHERE
    kca_operation IN ('INSERT', 'UPDATE')
    AND (COALESCE(TRANSACTION_TYPE_ID, 0), kca_seq_id) IN (
      SELECT
        COALESCE(TRANSACTION_TYPE_ID, 0) AS TRANSACTION_TYPE_ID,
        MAX(kca_seq_id)
      FROM bronze_bec_ods_stg.oe_transaction_types_all
      WHERE
        kca_operation IN ('INSERT', 'UPDATE')
      GROUP BY
        COALESCE(TRANSACTION_TYPE_ID, 0)
    )
);
/* Soft delete */
UPDATE silver_bec_ods.oe_transaction_types_all SET IS_DELETED_FLG = 'N';
UPDATE silver_bec_ods.oe_transaction_types_all SET IS_DELETED_FLG = 'Y'
WHERE
  (
    TRANSACTION_TYPE_ID
  ) IN (
    SELECT
      TRANSACTION_TYPE_ID
    FROM bec_raw_dl_ext.oe_transaction_types_all
    WHERE
      (TRANSACTION_TYPE_ID, KCA_SEQ_ID) IN (
        SELECT
          TRANSACTION_TYPE_ID,
          MAX(KCA_SEQ_ID) AS KCA_SEQ_ID
        FROM bec_raw_dl_ext.oe_transaction_types_all
        GROUP BY
          TRANSACTION_TYPE_ID
      )
      AND kca_operation = 'DELETE'
  );
UPDATE bec_etl_ctrl.batch_ods_info SET last_refresh_date = CURRENT_TIMESTAMP()
WHERE
  ods_table_name = 'oe_transaction_types_all';