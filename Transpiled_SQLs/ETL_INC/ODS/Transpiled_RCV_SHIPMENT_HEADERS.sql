/* Delete Records */
DELETE FROM silver_bec_ods.rcv_shipment_headers
WHERE
  SHIPMENT_HEADER_ID IN (
    SELECT
      stg.SHIPMENT_HEADER_ID
    FROM silver_bec_ods.rcv_shipment_headers AS ods, bronze_bec_ods_stg.rcv_shipment_headers AS stg
    WHERE
      ods.SHIPMENT_HEADER_ID = stg.SHIPMENT_HEADER_ID
      AND stg.kca_operation IN ('INSERT', 'UPDATE')
  );
/* Insert records */
INSERT INTO silver_bec_ods.RCV_SHIPMENT_HEADERS (
  SHIPMENT_HEADER_ID,
  LAST_UPDATE_DATE,
  LAST_UPDATED_BY,
  CREATION_DATE,
  CREATED_BY,
  LAST_UPDATE_LOGIN,
  RECEIPT_SOURCE_CODE,
  VENDOR_ID,
  VENDOR_SITE_ID,
  ORGANIZATION_ID,
  SHIPMENT_NUM,
  RECEIPT_NUM,
  SHIP_TO_LOCATION_ID,
  BILL_OF_LADING,
  PACKING_SLIP,
  SHIPPED_DATE,
  FREIGHT_CARRIER_CODE,
  EXPECTED_RECEIPT_DATE,
  EMPLOYEE_ID,
  NUM_OF_CONTAINERS,
  WAYBILL_AIRBILL_NUM,
  COMMENTS,
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
  USSGL_TRANSACTION_CODE,
  GOVERNMENT_CONTEXT,
  REQUEST_ID,
  PROGRAM_APPLICATION_ID,
  PROGRAM_ID,
  PROGRAM_UPDATE_DATE,
  ASN_TYPE,
  EDI_CONTROL_NUM,
  NOTICE_CREATION_DATE,
  GROSS_WEIGHT,
  GROSS_WEIGHT_UOM_CODE,
  NET_WEIGHT,
  NET_WEIGHT_UOM_CODE,
  TAR_WEIGHT,
  TAR_WEIGHT_UOM_CODE,
  PACKAGING_CODE,
  CARRIER_METHOD,
  CARRIER_EQUIPMENT,
  CARRIER_EQUIPMENT_NUM,
  CARRIER_EQUIPMENT_ALPHA,
  SPECIAL_HANDLING_CODE,
  HAZARD_CODE,
  HAZARD_CLASS,
  HAZARD_DESCRIPTION,
  FREIGHT_TERMS,
  FREIGHT_BILL_NUMBER,
  INVOICE_NUM,
  INVOICE_DATE,
  INVOICE_AMOUNT,
  TAX_NAME,
  TAX_AMOUNT,
  FREIGHT_AMOUNT,
  INVOICE_STATUS_CODE,
  ASN_STATUS,
  CURRENCY_CODE,
  CONVERSION_RATE_TYPE,
  CONVERSION_RATE,
  CONVERSION_DATE,
  PAYMENT_TERMS_ID,
  MRC_CONVERSION_RATE_TYPE,
  MRC_CONVERSION_DATE,
  MRC_CONVERSION_RATE,
  SHIP_TO_ORG_ID,
  CUSTOMER_ID,
  CUSTOMER_SITE_ID,
  REMIT_TO_SITE_ID,
  SHIP_FROM_LOCATION_ID,
  WF_ITEM_TYPE,
  WF_ITEM_KEY,
  APPROVAL_STATUS,
  PERFORMANCE_PERIOD_FROM,
  PERFORMANCE_PERIOD_TO,
  REQUEST_DATE,
  KCA_OPERATION,
  IS_DELETED_FLG,
  kca_seq_id,
  kca_seq_date
)
(
  SELECT
    shipment_header_id,
    last_update_date,
    last_updated_by,
    creation_date,
    created_by,
    last_update_login,
    receipt_source_code,
    vendor_id,
    vendor_site_id,
    organization_id,
    shipment_num,
    receipt_num,
    ship_to_location_id,
    bill_of_lading,
    packing_slip,
    shipped_date,
    freight_carrier_code,
    expected_receipt_date,
    employee_id,
    num_of_containers,
    waybill_airbill_num,
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
    attribute11,
    attribute12,
    attribute13,
    attribute14,
    attribute15,
    ussgl_transaction_code,
    government_context,
    request_id,
    program_application_id,
    program_id,
    program_update_date,
    asn_type,
    edi_control_num,
    notice_creation_date,
    gross_weight,
    gross_weight_uom_code,
    net_weight,
    net_weight_uom_code,
    tar_weight,
    tar_weight_uom_code,
    packaging_code,
    carrier_method,
    carrier_equipment,
    carrier_equipment_num,
    carrier_equipment_alpha,
    special_handling_code,
    hazard_code,
    hazard_class,
    hazard_description,
    freight_terms,
    freight_bill_number,
    invoice_num,
    invoice_date,
    invoice_amount,
    tax_name,
    tax_amount,
    freight_amount,
    invoice_status_code,
    asn_status,
    currency_code,
    conversion_rate_type,
    conversion_rate,
    conversion_date,
    payment_terms_id,
    mrc_conversion_rate_type,
    mrc_conversion_date,
    mrc_conversion_rate,
    ship_to_org_id,
    customer_id,
    customer_site_id,
    remit_to_site_id,
    ship_from_location_id,
    wf_item_type,
    wf_item_key,
    approval_status,
    performance_period_from,
    performance_period_to,
    request_date,
    KCA_OPERATION,
    'N' AS IS_DELETED_FLG,
    CAST(NULLIF(KCA_SEQ_ID, '') AS DECIMAL(36, 0)) AS KCA_SEQ_ID,
    KCA_SEQ_DATE
  FROM bronze_bec_ods_stg.rcv_shipment_headers
  WHERE
    kca_operation IN ('INSERT', 'UPDATE')
    AND (SHIPMENT_HEADER_ID, kca_seq_id) IN (
      SELECT
        SHIPMENT_HEADER_ID,
        MAX(kca_seq_id)
      FROM bronze_bec_ods_stg.rcv_shipment_headers
      WHERE
        kca_operation IN ('INSERT', 'UPDATE')
      GROUP BY
        SHIPMENT_HEADER_ID
    )
);
/* Soft delete */
UPDATE silver_bec_ods.rcv_shipment_headers SET IS_DELETED_FLG = 'N';
UPDATE silver_bec_ods.rcv_shipment_headers SET IS_DELETED_FLG = 'Y'
WHERE
  (
    SHIPMENT_HEADER_ID
  ) IN (
    SELECT
      SHIPMENT_HEADER_ID
    FROM bec_raw_dl_ext.rcv_shipment_headers
    WHERE
      (SHIPMENT_HEADER_ID, KCA_SEQ_ID) IN (
        SELECT
          SHIPMENT_HEADER_ID,
          MAX(KCA_SEQ_ID) AS KCA_SEQ_ID
        FROM bec_raw_dl_ext.rcv_shipment_headers
        GROUP BY
          SHIPMENT_HEADER_ID
      )
      AND kca_operation = 'DELETE'
  );
UPDATE bec_etl_ctrl.batch_ods_info SET load_type = 'I', last_refresh_date = CURRENT_TIMESTAMP()
WHERE
  ods_table_name = 'rcv_shipment_headers';