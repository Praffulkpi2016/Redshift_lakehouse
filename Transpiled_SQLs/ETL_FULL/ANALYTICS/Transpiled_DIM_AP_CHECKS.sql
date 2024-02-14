DROP table IF EXISTS gold_bec_dwh.dim_ap_checks;
CREATE TABLE gold_bec_dwh.dim_ap_checks AS
(
  SELECT
    AMOUNT,
    BANK_ACCOUNT_ID,
    BANK_ACCOUNT_NAME,
    CHECK_DATE,
    CHECK_ID,
    CHECK_NUMBER,
    CURRENCY_CODE,
    PAYMENT_METHOD_CODE AS PAYMENT_METHOD_LOOKUP_CODE,
    ADDRESS_LINE1,
    ADDRESS_LINE2,
    ADDRESS_LINE3,
    CHECKRUN_NAME,
    CITY,
    COUNTRY,
    STATUS_LOOKUP_CODE,
    VENDOR_NAME,
    VENDOR_SITE_CODE,
    ZIP,
    BANK_ACCOUNT_NUM AS SUPPLIER_BANK_ACCOUNT,
    BANK_ACCOUNT_TYPE AS SUPPLIER_BANK_TYPE,
    BANK_NUM AS SUPPLIER_BANK_NUM,
    CLEARED_AMOUNT,
    CLEARED_DATE,
    CLEARED_BASE_AMOUNT,
    CLEARED_EXCHANGE_RATE,
    CLEARED_EXCHANGE_DATE,
    ORG_ID,
    VENDOR_ID,
    VENDOR_SITE_ID,
    EXCHANGE_RATE,
    EXCHANGE_DATE,
    EXCHANGE_RATE_TYPE,
    COALESCE(BASE_AMOUNT, AMOUNT) AS PAYMENT_BASE_AMOUNT,
    CHECKRUN_ID,
    EXTERNAL_BANK_ACCOUNT_ID,
    VOID_DATE,
    LEGAL_ENTITY_ID,
    REMIT_TO_SUPPLIER_NAME,
    REMIT_TO_SUPPLIER_SITE,
    DOC_SEQUENCE_VALUE,
    'N' AS is_deleted_flg,
    (
      SELECT
        system_id
      FROM bec_etl_ctrl.etlsourceappid
      WHERE
        source_system = 'EBS'
    ) AS source_app_id,
    (
      SELECT
        system_id
      FROM bec_etl_ctrl.etlsourceappid
      WHERE
        source_system = 'EBS'
    ) || '-' || COALESCE(CHECK_ID, 0) AS dw_load_id,
    CURRENT_TIMESTAMP() AS dw_insert_date,
    CURRENT_TIMESTAMP() AS dw_update_date
  FROM silver_bec_ods.AP_CHECKS_ALL
);
UPDATE bec_etl_ctrl.batch_dw_info SET load_type = 'I', last_refresh_date = CURRENT_TIMESTAMP()
WHERE
  dw_table_name = 'dim_ap_checks' AND batch_name = 'ap';