DROP table IF EXISTS gold_bec_dwh.FACT_CHECK_DETAILS;
CREATE TABLE gold_bec_dwh.FACT_CHECK_DETAILS AS
(
  SELECT
    CH.CHECK_ID,
    CH.VENDOR_ID,
    CH.VENDOR_SITE_ID,
    (
      SELECT
        system_id
      FROM bec_etl_ctrl.etlsourceappid
      WHERE
        source_system = 'EBS'
    ) || '-' || CH.CHECK_ID AS CHECK_ID_KEY,
    (
      SELECT
        system_id
      FROM bec_etl_ctrl.etlsourceappid
      WHERE
        source_system = 'EBS'
    ) || '-' || CH.VENDOR_ID AS VENDOR_ID_KEY,
    (
      SELECT
        system_id
      FROM bec_etl_ctrl.etlsourceappid
      WHERE
        source_system = 'EBS'
    ) || '-' || CH.VENDOR_SITE_ID AS VENDOR_SITE_ID_KEY,
    BA.BANK_ACCOUNT_ID,
    BA.BANK_BRANCH_ID,
    CH.CHECK_NUMBER AS CHECK_NUMBER,
    CH.CHECK_DATE AS CHECK_DATE,
    CH.AMOUNT AS AMOUNT,
    COALESCE(SUBSTRING(CH.VENDOR_NAME, 1, 240), SUBSTRING(CH.REMIT_TO_SUPPLIER_NAME, 1, 240)) AS VENDOR_NAME,
    COALESCE(CH.REMIT_TO_SUPPLIER_SITE, APSA.VENDOR_SITE_CODE) AS VENDOR_SITE_CODE,
    SUBSTRING(CH.ADDRESS_LINE1, 1, 23) AS ADDRESS_LINE1,
    SUBSTRING(CH.ADDRESS_LINE2, 1, 23) AS ADDRESS_LINE2,
    SUBSTRING(CH.ADDRESS_LINE3, 1, 23) AS ADDRESS_LINE3,
    SUBSTRING(CH.CITY, 1, 13) AS CITY,
    SUBSTRING(CH.STATE, 1, 4) AS STATE,
    SUBSTRING(CH.ZIP, 1, 6) AS ZIP,
    CH.CLEARED_DATE AS CLEARED_DATE,
    CH.CLEARED_AMOUNT AS CLEARED_AMOUNT,
    CPD.PAYMENT_DOCUMENT_NAME AS CHECK_STOCK_NAME,
    LK2.MEANING AS NLS_STATUS,
    CH.BANK_ACCOUNT_NAME AS ACCOUNT,
    CH.BANK_ACCOUNT_ID AS ACCOUNTID,
    BA.CURRENCY_CODE AS CURRENCY_CODE,
    CH.CURRENCY_CODE AS PAY_CURRENCY_CODE,
    CH.DOC_SEQUENCE_VALUE AS DOC_SEQUENCE_VALUE,
    CH.PAYMENT_METHOD_CODE AS PAYMENT_METHOD_CODE,
    (
      SELECT
        system_id
      FROM bec_etl_ctrl.etlsourceappid
      WHERE
        source_system = 'EBS'
    ) || '-' || CH.ORG_ID AS ORG_ID_KEY,
    CH.ORG_ID AS ORG_ID,
    'N' AS IS_DELETED_FLG,
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
    ) || '-' || COALESCE(CH.CHECK_ID, 0) AS dw_load_id,
    CURRENT_TIMESTAMP() AS dw_insert_date,
    CURRENT_TIMESTAMP() AS dw_update_date
  FROM (
    SELECT
      *
    FROM silver_bec_ods.AP_CHECKS_ALL
    WHERE
      is_deleted_flg <> 'Y'
  ) AS CH, (
    SELECT
      *
    FROM silver_bec_ods.CE_PAYMENT_DOCUMENTS
    WHERE
      is_deleted_flg <> 'Y'
  ) AS CPD, (
    SELECT
      *
    FROM silver_bec_ods.CE_BANK_ACCT_USES_ALL
    WHERE
      is_deleted_flg <> 'Y'
  ) AS CBAU, (
    SELECT
      *
    FROM silver_bec_ods.CE_BANK_ACCOUNTS
    WHERE
      is_deleted_flg <> 'Y'
  ) AS BA, (
    SELECT
      *
    FROM silver_bec_ods.FND_LOOKUP_VALUES
    WHERE
      is_deleted_flg <> 'Y'
  ) AS LK2, (
    SELECT
      *
    FROM silver_bec_ods.AP_SUPPLIER_SITES_ALL
    WHERE
      is_deleted_flg <> 'Y'
  ) AS APSA
  WHERE
    1 = 1
    AND CPD.PAYMENT_DOCUMENT_ID() = CH.PAYMENT_DOCUMENT_ID
    AND CH.CE_BANK_ACCT_USE_ID = CBAU.BANK_ACCT_USE_ID
    AND CBAU.BANK_ACCOUNT_ID = BA.BANK_ACCOUNT_ID
    AND LK2.LOOKUP_TYPE = 'CHECK STATE'
    AND LK2.LANGUAGE = 'US'
    AND LK2.LOOKUP_CODE = CH.STATUS_LOOKUP_CODE
    AND CH.VENDOR_SITE_ID = APSA.VENDOR_SITE_ID()
  ORDER BY
    UPPER(CH.BANK_ACCOUNT_NAME) /*  UPPER(BR.BANK_NAME), */ /*  UPPER(BR.BANK_BRANCH_NAME), */ NULLS LAST,
    CH.CURRENCY_CODE NULLS LAST,
    CPD.PAYMENT_DOCUMENT_NAME NULLS LAST,
    CH.CHECK_NUMBER NULLS LAST
);
UPDATE bec_etl_ctrl.batch_dw_info SET load_type = 'I', last_refresh_date = CURRENT_TIMESTAMP()
WHERE
  dw_table_name = 'fact_check_details' AND batch_name = 'ap';