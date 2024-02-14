DROP table IF EXISTS gold_bec_dwh.FACT_AR_INVOICE_BILLING;
CREATE TABLE gold_bec_dwh.FACT_AR_INVOICE_BILLING AS
(
  SELECT
    RAG.CUST_TRX_LINE_GL_DIST_ID,
    RAG.ORG_ID,
    (
      SELECT
        system_id
      FROM bec_etl_ctrl.etlsourceappid
      WHERE
        source_system = 'EBS'
    ) || '-' || RAG.ORG_ID AS ORG_ID_KEY,
    RAG.CODE_COMBINATION_ID,
    (
      SELECT
        system_id
      FROM bec_etl_ctrl.etlsourceappid
      WHERE
        source_system = 'EBS'
    ) || '-' || RAG.CODE_COMBINATION_ID AS ACCOUNT_ID_KEY,
    RAG.CUSTOMER_TRX_ID,
    RAG.CUSTOMER_TRX_LINE_ID,
    APS.PAYMENT_SCHEDULE_ID,
    ARA.ADJUSTMENT_ID,
    (
      SELECT
        system_id
      FROM bec_etl_ctrl.etlsourceappid
      WHERE
        source_system = 'EBS'
    ) || '-' || ARA.ADJUSTMENT_ID AS ADJUSTMENT_ID_KEY,
    RAT.BATCH_SOURCE_ID,
    RAT.INVOICE_CURRENCY_CODE,
    RAT.CUST_TRX_TYPE_ID,
    RAT.TRX_DATE,
    RAS.NAME AS SOURCE_NAME,
    RAG.GL_DATE,
    APS.DUE_DATE,
    APS.AMOUNT_DUE_REMAINING,
    RAG.AMOUNT,
    COALESCE(ARA.AMOUNT, 0) AS ADJ_AMOUNT,
    CAST(COALESCE(APS.AMOUNT_DUE_REMAINING, 0) * COALESCE(DCR.conversion_rate, 1) AS DECIMAL(18, 2)) AS GBL_INVOICE_AMOUNT,
    CAST(COALESCE(RAG.AMOUNT, 0) * COALESCE(DCR.conversion_rate, 1) AS DECIMAL(18, 2)) AS GBL_AMOUNT_PAID,
    CAST(COALESCE(ARA.AMOUNT, 0) * COALESCE(DCR.conversion_rate, 1) AS DECIMAL(18, 2)) AS GBL_ADJ_AMOUNT,
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
    ) || '-' || COALESCE(RAG.CUST_TRX_LINE_GL_DIST_ID, 0) || '-' || COALESCE(ARA.ADJUSTMENT_ID, 0) || '-' || COALESCE(APS.PAYMENT_SCHEDULE_ID, 0) AS dw_load_id,
    CURRENT_TIMESTAMP() AS dw_insert_date,
    CURRENT_TIMESTAMP() AS dw_update_date
  FROM (
    SELECT
      *
    FROM silver_bec_ods.RA_CUST_TRX_LINE_GL_DIST_ALL
    WHERE
      is_deleted_flg <> 'Y'
  ) AS RAG
  INNER JOIN (
    SELECT
      *
    FROM silver_bec_ods.RA_CUSTOMER_TRX_LINES_ALL
    WHERE
      is_deleted_flg <> 'Y'
  ) AS RAL
    ON RAG.CUSTOMER_TRX_LINE_ID = RAL.CUSTOMER_TRX_LINE_ID
  INNER JOIN (
    SELECT
      *
    FROM silver_bec_ods.RA_CUSTOMER_TRX_ALL
    WHERE
      is_deleted_flg <> 'Y'
  ) AS RAT
    ON RAG.CUSTOMER_TRX_ID = RAT.CUSTOMER_TRX_ID
  LEFT OUTER JOIN (
    SELECT
      *
    FROM silver_bec_ods.AR_PAYMENT_SCHEDULES_ALL
    WHERE
      is_deleted_flg <> 'Y'
  ) AS APS
    ON RAT.CUSTOMER_TRX_ID = APS.CUSTOMER_TRX_ID
  LEFT OUTER JOIN (
    SELECT
      *
    FROM silver_bec_ods.AR_ADJUSTMENTS_ALL
    WHERE
      is_deleted_flg <> 'Y'
  ) AS ARA
    ON RAT.CUSTOMER_TRX_ID = ARA.CUSTOMER_TRX_ID
  LEFT OUTER JOIN (
    SELECT
      *
    FROM silver_bec_ods.RA_BATCH_SOURCES_ALL
    WHERE
      is_deleted_flg <> 'Y'
  ) AS RAS
    ON RAT.BATCH_SOURCE_ID = RAS.BATCH_SOURCE_ID AND RAG.ORG_ID = RAS.ORG_ID
  LEFT OUTER JOIN (
    SELECT
      *
    FROM silver_bec_ods.GL_DAILY_RATES
    WHERE
      conversion_type = 'Corporate' AND to_currency = 'USD' AND is_deleted_flg <> 'Y'
  ) AS DCR
    ON DCR.from_currency = RAT.invoice_currency_code AND DCR.conversion_date = RAG.GL_DATE
);
UPDATE bec_etl_ctrl.batch_dw_info SET load_type = 'I', last_refresh_date = CURRENT_TIMESTAMP()
WHERE
  dw_table_name = 'fact_ar_invoice_billing' AND batch_name = 'ar';