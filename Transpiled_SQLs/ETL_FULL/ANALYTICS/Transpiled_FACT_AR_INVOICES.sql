DROP table IF EXISTS gold_bec_dwh.FACT_AR_INVOICES;
CREATE TABLE gold_bec_dwh.FACT_AR_INVOICES AS
(
  SELECT
    T.CUSTOMER_TRX_ID,
    T.TRX_NUMBER,
    (
      SELECT
        system_id
      FROM bec_etl_ctrl.etlsourceappid
      WHERE
        source_system = 'EBS'
    ) || '-' || T.SET_OF_BOOKS_ID AS LEDGER_ID_KEY,
    (
      SELECT
        system_id
      FROM bec_etl_ctrl.etlsourceappid
      WHERE
        source_system = 'EBS'
    ) || '-' || T.ORG_ID AS ORG_ID_KEY,
    T.ORG_ID AS ORG_ID,
    (
      SELECT
        system_id
      FROM bec_etl_ctrl.etlsourceappid
      WHERE
        source_system = 'EBS'
    ) || '-' || T.BILL_TO_CUSTOMER_ID AS Bill_to_customer_id_key,
    (
      SELECT
        system_id
      FROM bec_etl_ctrl.etlsourceappid
      WHERE
        source_system = 'EBS'
    ) || '-' || T.BILL_TO_SITE_USE_ID AS CUST_BILL_TO_SITE_USE_ID_KEY,
    (
      SELECT
        system_id
      FROM bec_etl_ctrl.etlsourceappid
      WHERE
        source_system = 'EBS'
    ) || '-' || T.SHIP_TO_CUSTOMER_ID AS CUST_SHIP_TO_CUSTOMER_ID_KEY,
    T.BILL_TO_CUSTOMER_ID,
    T.BILL_TO_SITE_USE_ID,
    T.SHIP_TO_CUSTOMER_ID,
    T.SHIP_TO_SITE_USE_ID,
    (
      SELECT
        system_id
      FROM bec_etl_ctrl.etlsourceappid
      WHERE
        source_system = 'EBS'
    ) || '-' || T.SHIP_TO_SITE_USE_ID AS CUST_SHIP_TO_SITE_USE_ID_KEY,
    T.TRX_DATE,
    CASE WHEN PS.CLASS = 'INV' THEN PS.DUE_DATE END AS INVOICE_DUE_DATE,
    CASE WHEN PS.CLASS = 'INV' THEN PS.GL_DATE END AS INVOICE_GL_DATE,
    T.COMMENTS AS INVOICE_COMMENTS,
    TM.NAME AS PAYMENT_TERM,
    TM.DESCRIPTION AS PAYMENT_TERM_DESCRIPTION,
    T.PURCHASE_ORDER,
    T.INVOICE_CURRENCY_CODE,
    T.LAST_UPDATE_DATE,
    RT.NAME AS TRX_TYPE_NAME,
    RT.DESCRIPTION AS TRX_TYPE_DESCRIPTION,
    RT.TYPE AS TRX_TYPE,
    T.OLD_TRX_NUMBER,
    T.CT_REFERENCE,
    PT.TRX_NUMBER AS ORIGINAL_INVOICE_NUMBER,
    PT.TRX_DATE AS ORIGINAL_INVOICE_DATE,
    PS.FULLY_PAID_FLAG,
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
    ) || '-' || COALESCE(T.CUSTOMER_TRX_ID, 0) AS dw_load_id,
    CURRENT_TIMESTAMP() AS dw_insert_date,
    CURRENT_TIMESTAMP() AS dw_update_date
  FROM (
    SELECT
      *
    FROM silver_bec_ods.RA_CUSTOMER_TRX_ALL
    WHERE
      is_deleted_flg <> 'Y'
  ) AS T, (
    SELECT
      *
    FROM silver_bec_ods.RA_CUSTOMER_TRX_ALL
    WHERE
      is_deleted_flg <> 'Y'
  ) AS PT, (
    SELECT
      CUSTOMER_TRX_ID,
      CLASS,
      MIN(DUE_DATE) AS DUE_DATE,
      MIN(GL_DATE) AS GL_DATE,
      CASE WHEN SUM(AMOUNT_DUE_REMAINING) = 0 THEN 'Y' ELSE 'N' END AS FULLY_PAID_FLAG
    FROM (
      SELECT
        *
      FROM silver_bec_ods.AR_PAYMENT_SCHEDULES_ALL
      WHERE
        is_deleted_flg <> 'Y'
    ) AS AR_PAYMENT_SCHEDULES_ALL
    GROUP BY
      CUSTOMER_TRX_ID,
      CLASS
  ) AS PS, (
    SELECT
      *
    FROM silver_bec_ods.RA_TERMS_TL
    WHERE
      is_deleted_flg <> 'Y'
  ) AS TM, (
    SELECT
      *
    FROM silver_bec_ods.RA_CUST_TRX_TYPES_ALL
    WHERE
      is_deleted_flg <> 'Y'
  ) AS RT
  WHERE
    1 = 1
    AND T.PREVIOUS_CUSTOMER_TRX_ID = PT.CUSTOMER_TRX_ID()
    AND T.CUST_TRX_TYPE_ID = RT.CUST_TRX_TYPE_ID
    AND T.ORG_ID = RT.ORG_ID
    AND T.TERM_ID = TM.TERM_ID()
    AND T.CUSTOMER_TRX_ID = PS.CUSTOMER_TRX_ID()
    AND TM.LANGUAGE = 'US'
);
UPDATE bec_etl_ctrl.batch_dw_info SET load_type = 'I', last_refresh_date = CURRENT_TIMESTAMP()
WHERE
  dw_table_name = 'fact_ar_invoices' AND batch_name = 'ar';