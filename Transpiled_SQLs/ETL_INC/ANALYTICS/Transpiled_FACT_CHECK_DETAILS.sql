/* Delete Records */
DELETE FROM gold_bec_dwh.fact_check_details
WHERE
  CHECK_ID IN (
    SELECT
      ods.CHECK_ID
    FROM gold_bec_dwh.fact_check_details AS dw, (
      SELECT
        CH.CHECK_ID
      FROM silver_bec_ods.AP_CHECKS_ALL AS CH, silver_bec_ods.CE_PAYMENT_DOCUMENTS AS CPD, silver_bec_ods.CE_BANK_ACCT_USES_ALL AS CBAU, silver_bec_ods.CE_BANK_ACCOUNTS AS BA, silver_bec_ods.FND_LOOKUP_VALUES AS LK2, silver_bec_ods.AP_SUPPLIER_SITES_ALL AS APSA
      WHERE
        1 = 1
        AND CPD.PAYMENT_DOCUMENT_ID() = CH.PAYMENT_DOCUMENT_ID
        AND CH.CE_BANK_ACCT_USE_ID = CBAU.BANK_ACCT_USE_ID
        AND CBAU.BANK_ACCOUNT_ID = BA.BANK_ACCOUNT_ID
        AND LK2.LOOKUP_TYPE = 'CHECK STATE'
        AND LK2.LANGUAGE = 'US'
        AND LK2.LOOKUP_CODE = CH.STATUS_LOOKUP_CODE
        AND CH.VENDOR_SITE_ID = APSA.VENDOR_SITE_ID()
        AND (
          CH.kca_seq_date > (
            SELECT
              (
                executebegints - prune_days
              )
            FROM bec_etl_ctrl.batch_dw_info
            WHERE
              dw_table_name = 'fact_check_details' AND batch_name = 'ap'
          )
          OR BA.kca_seq_date > (
            SELECT
              (
                executebegints - prune_days
              )
            FROM bec_etl_ctrl.batch_dw_info
            WHERE
              dw_table_name = 'fact_check_details' AND batch_name = 'ap'
          )
          OR CH.is_deleted_flg = 'Y'
          OR CPD.is_deleted_flg = 'Y'
          OR CBAU.is_deleted_flg = 'Y'
          OR BA.is_deleted_flg = 'Y'
          OR LK2.is_deleted_flg = 'Y'
          OR APSA.is_deleted_flg = 'Y'
        )
      ORDER BY
        UPPER(CH.BANK_ACCOUNT_NAME) NULLS LAST,
        CH.CURRENCY_CODE NULLS LAST,
        CPD.PAYMENT_DOCUMENT_NAME NULLS LAST,
        CH.CHECK_NUMBER NULLS LAST
    ) AS ods
    WHERE
      dw.dw_load_id = (
        SELECT
          system_id
        FROM bec_etl_ctrl.etlsourceappid
        WHERE
          source_system = 'EBS'
      ) || '-' || COALESCE(ods.CHECK_ID, 0)
  );
/* Insert records */
INSERT INTO gold_bec_dwh.fact_check_details (
  CHECK_ID,
  VENDOR_ID,
  VENDOR_SITE_ID,
  CHECK_ID_KEY,
  VENDOR_ID_KEY,
  VENDOR_SITE_ID_KEY,
  BANK_ACCOUNT_ID,
  BANK_BRANCH_ID,
  CHECK_NUMBER,
  CHECK_DATE,
  AMOUNT,
  VENDOR_NAME,
  VENDOR_SITE_CODE,
  ADDRESS_LINE1,
  ADDRESS_LINE2,
  ADDRESS_LINE3,
  CITY,
  STATE,
  ZIP,
  CLEARED_DATE,
  CLEARED_AMOUNT,
  CHECK_STOCK_NAME,
  NLS_STATUS,
  ACCOUNT,
  ACCOUNTID,
  CURRENCY_CODE,
  PAY_CURRENCY_CODE,
  DOC_SEQUENCE_VALUE,
  PAYMENT_METHOD_CODE,
  ORG_ID_KEY,
  ORG_ID,
  IS_DELETED_FLG,
  source_app_id,
  dw_load_id,
  dw_insert_date,
  dw_update_date
)
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
    AND (
      CH.kca_seq_date > (
        SELECT
          (
            executebegints - prune_days
          )
        FROM bec_etl_ctrl.batch_dw_info
        WHERE
          dw_table_name = 'fact_check_details' AND batch_name = 'ap'
      )
      OR BA.kca_seq_date > (
        SELECT
          (
            executebegints - prune_days
          )
        FROM bec_etl_ctrl.batch_dw_info
        WHERE
          dw_table_name = 'fact_check_details' AND batch_name = 'ap'
      )
    )
  ORDER BY
    UPPER(CH.BANK_ACCOUNT_NAME) /*  UPPER(BR.BANK_NAME), */ /*  UPPER(BR.BANK_BRANCH_NAME), */ NULLS LAST,
    CH.CURRENCY_CODE NULLS LAST,
    CPD.PAYMENT_DOCUMENT_NAME NULLS LAST,
    CH.CHECK_NUMBER NULLS LAST
);
UPDATE bec_etl_ctrl.batch_dw_info SET last_refresh_date = CURRENT_TIMESTAMP()
WHERE
  dw_table_name = 'fact_check_details' AND batch_name = 'ap';