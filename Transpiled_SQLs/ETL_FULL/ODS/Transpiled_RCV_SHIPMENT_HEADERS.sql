DROP TABLE IF EXISTS silver_bec_ods.RCV_SHIPMENT_HEADERS;
CREATE TABLE IF NOT EXISTS silver_bec_ods.RCV_SHIPMENT_HEADERS (
  shipment_header_id DECIMAL(15, 0),
  last_update_date TIMESTAMP,
  last_updated_by DECIMAL(15, 0),
  creation_date TIMESTAMP,
  created_by DECIMAL(15, 0),
  last_update_login DECIMAL(15, 0),
  receipt_source_code STRING,
  vendor_id DECIMAL(15, 0),
  vendor_site_id DECIMAL(15, 0),
  organization_id DECIMAL(15, 0),
  shipment_num STRING,
  receipt_num STRING,
  ship_to_location_id DECIMAL(15, 0),
  bill_of_lading STRING,
  packing_slip STRING,
  shipped_date TIMESTAMP,
  freight_carrier_code STRING,
  expected_receipt_date TIMESTAMP,
  employee_id DECIMAL(9, 0),
  num_of_containers DECIMAL(28, 10),
  waybill_airbill_num STRING,
  comments STRING,
  attribute_category STRING,
  attribute1 STRING,
  attribute2 STRING,
  attribute3 STRING,
  attribute4 STRING,
  attribute5 STRING,
  attribute6 STRING,
  attribute7 STRING,
  attribute8 STRING,
  attribute9 STRING,
  attribute10 STRING,
  attribute11 STRING,
  attribute12 STRING,
  attribute13 STRING,
  attribute14 STRING,
  attribute15 STRING,
  ussgl_transaction_code STRING,
  government_context STRING,
  request_id DECIMAL(15, 0),
  program_application_id DECIMAL(15, 0),
  program_id DECIMAL(15, 0),
  program_update_date TIMESTAMP,
  asn_type STRING,
  edi_control_num STRING,
  notice_creation_date TIMESTAMP,
  gross_weight DECIMAL(28, 10),
  gross_weight_uom_code STRING,
  net_weight DECIMAL(28, 10),
  net_weight_uom_code STRING,
  tar_weight DECIMAL(28, 10),
  tar_weight_uom_code STRING,
  packaging_code STRING,
  carrier_method STRING,
  carrier_equipment STRING,
  carrier_equipment_num STRING,
  carrier_equipment_alpha STRING,
  special_handling_code STRING,
  hazard_code STRING,
  hazard_class STRING,
  hazard_description STRING,
  freight_terms STRING,
  freight_bill_number STRING,
  invoice_num STRING,
  invoice_date TIMESTAMP,
  invoice_amount DECIMAL(28, 10),
  tax_name STRING,
  tax_amount DECIMAL(28, 10),
  freight_amount DECIMAL(28, 10),
  invoice_status_code STRING,
  asn_status STRING,
  currency_code STRING,
  conversion_rate_type STRING,
  conversion_rate DECIMAL(28, 10),
  conversion_date TIMESTAMP,
  payment_terms_id DECIMAL(15, 0),
  mrc_conversion_rate_type STRING,
  mrc_conversion_date STRING,
  mrc_conversion_rate STRING,
  ship_to_org_id DECIMAL(15, 0),
  customer_id DECIMAL(15, 0),
  customer_site_id DECIMAL(15, 0),
  remit_to_site_id DECIMAL(15, 0),
  ship_from_location_id DECIMAL(15, 0),
  wf_item_type STRING,
  wf_item_key STRING,
  approval_status STRING,
  performance_period_from TIMESTAMP,
  performance_period_to TIMESTAMP,
  request_date TIMESTAMP,
  KCA_OPERATION STRING,
  IS_DELETED_FLG STRING,
  kca_seq_id DECIMAL(36, 0),
  kca_seq_date TIMESTAMP
);
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
SELECT
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
  'N' AS IS_DELETED_FLG,
  CAST(NULLIF(KCA_SEQ_ID, '') AS DECIMAL(36, 0)) AS KCA_SEQ_ID,
  kca_seq_date
FROM bronze_bec_ods_stg.RCV_SHIPMENT_HEADERS;
UPDATE bec_etl_ctrl.batch_ods_info SET load_type = 'I', last_refresh_date = CURRENT_TIMESTAMP()
WHERE
  ods_table_name = 'rcv_shipment_headers';