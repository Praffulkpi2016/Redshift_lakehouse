TRUNCATE table bronze_bec_ods_stg.WSH_DOCUMENT_INSTANCES;
INSERT INTO bronze_bec_ods_stg.WSH_DOCUMENT_INSTANCES (
  DOCUMENT_INSTANCE_ID,
  ENTITY_NAME,
  ENTITY_ID,
  DOCUMENT_TYPE,
  SEQUENCE_NUMBER,
  STATUS,
  FINAL_PRINT_DATE,
  DOC_SEQUENCE_CATEGORY_ID,
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
  CREATION_DATE,
  CREATED_BY,
  LAST_UPDATE_DATE,
  LAST_UPDATED_BY,
  LAST_UPDATE_LOGIN,
  PROGRAM_APPLICATION_ID,
  PROGRAM_ID,
  PROGRAM_UPDATE_DATE,
  REQUEST_ID,
  POD_FLAG,
  POD_BY,
  POD_DATE,
  REASON_OF_TRANSPORT,
  DESCRIPTION,
  COD_AMOUNT,
  COD_CURRENCY_CODE,
  COD_REMIT_TO,
  COD_CHARGE_PAID_BY,
  PROBLEM_CONTACT_REFERENCE,
  BILL_FREIGHT_TO,
  CARRIED_BY,
  PORT_OF_LOADING,
  PORT_OF_DISCHARGE,
  BOOKING_OFFICE,
  BOOKING_NUMBER,
  SERVICE_CONTRACT,
  SHIPPER_EXPORT_REF,
  CARRIER_EXPORT_REF,
  BOL_NOTIFY_PARTY,
  SUPPLIER_CODE,
  AETC_NUMBER,
  SHIPPER_SIGNED_BY,
  SHIPPER_DATE,
  CARRIER_SIGNED_BY,
  CARRIER_DATE,
  BOL_ISSUE_OFFICE,
  BOL_ISSUED_BY,
  BOL_DATE_ISSUED,
  SHIPPER_HM_BY,
  SHIPPER_HM_DATE,
  CARRIER_HM_BY,
  CARRIER_HM_DATE,
  EXPECTED_POD_DATE,
  FUNC_AMOUNT,
  GROSS_AMOUNT,
  CURRENCY_CODE,
  CERTIFICATE_NUMBER,
  KEY_VERSION,
  SYSTEM_ENTRY_DATE,
  DOCUMENT_SEQ_ID,
  SIGNATURE,
  KCA_OPERATION,
  kca_seq_id,
  KCA_SEQ_DATE
)
(
  SELECT
    DOCUMENT_INSTANCE_ID,
    ENTITY_NAME,
    ENTITY_ID,
    DOCUMENT_TYPE,
    SEQUENCE_NUMBER,
    STATUS,
    FINAL_PRINT_DATE,
    DOC_SEQUENCE_CATEGORY_ID,
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
    CREATION_DATE,
    CREATED_BY,
    LAST_UPDATE_DATE,
    LAST_UPDATED_BY,
    LAST_UPDATE_LOGIN,
    PROGRAM_APPLICATION_ID,
    PROGRAM_ID,
    PROGRAM_UPDATE_DATE,
    REQUEST_ID,
    POD_FLAG,
    POD_BY,
    POD_DATE,
    REASON_OF_TRANSPORT,
    DESCRIPTION,
    COD_AMOUNT,
    COD_CURRENCY_CODE,
    COD_REMIT_TO,
    COD_CHARGE_PAID_BY,
    PROBLEM_CONTACT_REFERENCE,
    BILL_FREIGHT_TO,
    CARRIED_BY,
    PORT_OF_LOADING,
    PORT_OF_DISCHARGE,
    BOOKING_OFFICE,
    BOOKING_NUMBER,
    SERVICE_CONTRACT,
    SHIPPER_EXPORT_REF,
    CARRIER_EXPORT_REF,
    BOL_NOTIFY_PARTY,
    SUPPLIER_CODE,
    AETC_NUMBER,
    SHIPPER_SIGNED_BY,
    SHIPPER_DATE,
    CARRIER_SIGNED_BY,
    CARRIER_DATE,
    BOL_ISSUE_OFFICE,
    BOL_ISSUED_BY,
    BOL_DATE_ISSUED,
    SHIPPER_HM_BY,
    SHIPPER_HM_DATE,
    CARRIER_HM_BY,
    CARRIER_HM_DATE,
    EXPECTED_POD_DATE,
    FUNC_AMOUNT,
    GROSS_AMOUNT,
    CURRENCY_CODE,
    CERTIFICATE_NUMBER,
    KEY_VERSION,
    SYSTEM_ENTRY_DATE,
    DOCUMENT_SEQ_ID,
    SIGNATURE,
    KCA_OPERATION,
    kca_seq_id,
    KCA_SEQ_DATE
  FROM bec_raw_dl_ext.WSH_DOCUMENT_INSTANCES
  WHERE
    kca_operation <> 'DELETE'
    AND COALESCE(kca_seq_id, '') <> ''
    AND (COALESCE(DOCUMENT_INSTANCE_ID, 0), kca_seq_id) IN (
      SELECT
        COALESCE(DOCUMENT_INSTANCE_ID, 0) AS DOCUMENT_INSTANCE_ID,
        MAX(kca_seq_id)
      FROM bec_raw_dl_ext.WSH_DOCUMENT_INSTANCES
      WHERE
        kca_operation <> 'DELETE' AND COALESCE(kca_seq_id, '') <> ''
      GROUP BY
        COALESCE(DOCUMENT_INSTANCE_ID, 0)
    )
    AND (
      KCA_SEQ_DATE > (
        SELECT
          (
            executebegints - prune_days
          )
        FROM bec_etl_ctrl.batch_ods_info
        WHERE
          ods_table_name = 'wsh_document_instances'
      )
    )
);