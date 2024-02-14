DROP table IF EXISTS gold_bec_dwh.FACT_AP_AGING;
CREATE TABLE gold_bec_dwh.fact_ap_aging AS
(
  SELECT
    (
      SELECT
        system_id
      FROM bec_etl_ctrl.etlsourceappid
      WHERE
        source_system = 'EBS'
    ) || '-' || APSC.INVOICE_ID AS INVOICE_ID_KEY,
    API.INVOICE_NUM,
    (
      SELECT
        system_id
      FROM bec_etl_ctrl.etlsourceappid
      WHERE
        source_system = 'EBS'
    ) || '-' || API.VENDOR_ID AS VENDOR_ID_KEY,
    (
      SELECT
        system_id
      FROM bec_etl_ctrl.etlsourceappid
      WHERE
        source_system = 'EBS'
    ) || '-' || APSC.ORG_ID AS ORG_ID_KEY,
    APSC.ORG_ID AS ORG_ID,
    APSC.payment_num,
    API.INVOICE_CURRENCY_CODE,
    API.PAYMENT_CURRENCY_CODE,
    (
      SELECT
        system_id
      FROM bec_etl_ctrl.etlsourceappid
      WHERE
        source_system = 'EBS'
    ) || '-' || API.VENDOR_SITE_ID AS VENDOR_SITE_ID_KEY,
    (
      SELECT
        system_id
      FROM bec_etl_ctrl.etlsourceappid
      WHERE
        source_system = 'EBS'
    ) || '-' || API.SET_OF_BOOKS_ID AS LEDGER_ID_KEY,
    (
      SELECT
        system_id
      FROM bec_etl_ctrl.etlsourceappid
      WHERE
        source_system = 'EBS'
    ) || '-' || API.TERMS_ID AS TERMS_ID_KEY,
    APSC.INVOICE_ID,
    API.INVOICE_DATE,
    APSC.DUE_DATE,
    API.INVOICE_AMOUNT,
    API.AMOUNT_PAID,
    APSC.AMOUNT_REMAINING,
    CASE
      WHEN api.payment_status_flag = 'P'
      AND APSC.payment_status_flag = 'P'
      AND API.INVOICE_AMOUNT = API.AMOUNT_PAID
      THEN APSC.AMOUNT_REMAINING
      ELSE 0
    END AS prepay_amount,
    api.payment_status_flag AS inv_payment_status_flag,
    APSC.payment_status_flag AS payment_status_flag,
    COALESCE(API.BASE_AMOUNT, API.INVOICE_AMOUNT) AS `INVOICE_BASE_AMOUNT`,
    'ABC' AS `HOLD_FLAG`,
    (
      FLOOR(CURRENT_TIMESTAMP()) - FLOOR(APSC.DUE_DATE)
    ) AS `DAYS_OVERDUE`,
    (
      CASE
        WHEN FLOOR(CURRENT_TIMESTAMP()) - FLOOR(APSC.DUE_DATE) <= 0
        THEN SUM(APSC.AMOUNT_REMAINING)
        ELSE 0
      END
    ) AS `CURRENT_AP`,
    (
      CASE
        WHEN FLOOR(CURRENT_TIMESTAMP()) - FLOOR(APSC.DUE_DATE) BETWEEN 1 AND 15
        THEN SUM(APSC.AMOUNT_REMAINING)
        ELSE 0
      END
    ) AS `AP01_15`,
    (
      CASE
        WHEN FLOOR(CURRENT_TIMESTAMP()) - FLOOR(APSC.DUE_DATE) BETWEEN 16 AND 30
        THEN SUM(APSC.AMOUNT_REMAINING)
        ELSE 0
      END
    ) AS `AP16_30`,
    (
      CASE
        WHEN FLOOR(CURRENT_TIMESTAMP()) - FLOOR(APSC.DUE_DATE) BETWEEN 31 AND 60
        THEN SUM(APSC.AMOUNT_REMAINING)
        ELSE 0
      END
    ) AS `AP31_60`,
    (
      CASE
        WHEN FLOOR(CURRENT_TIMESTAMP()) - FLOOR(APSC.DUE_DATE) BETWEEN 61 AND 90
        THEN SUM(APSC.AMOUNT_REMAINING)
        ELSE 0
      END
    ) AS `AP61_90`,
    (
      CASE
        WHEN FLOOR(CURRENT_TIMESTAMP()) - FLOOR(APSC.DUE_DATE) > 90
        THEN SUM(APSC.AMOUNT_REMAINING)
        ELSE 0
      END
    ) AS `AP90_PLUS`,
    CAST(COALESCE(API.invoice_amount, 0) * COALESCE(DCR.conversion_rate, 1) AS DECIMAL(18, 2)) AS GBL_INVOICE_AMOUNT,
    CAST(COALESCE(API.amount_paid, 0) * COALESCE(DCR.conversion_rate, 1) AS DECIMAL(18, 2)) AS GBL_AMOUNT_PAID,
    CAST(COALESCE(APSC.AMOUNT_REMAINING, 0) * COALESCE(DCR.conversion_rate, 1) AS DECIMAL(18, 2)) AS GBL_AMOUNT_REMAINING,
    CAST(COALESCE(
      CASE
        WHEN api.payment_status_flag = 'P'
        AND APSC.payment_status_flag = 'P'
        AND API.INVOICE_AMOUNT = API.AMOUNT_PAID
        THEN APSC.AMOUNT_REMAINING
        ELSE 0
      END,
      0
    ) * COALESCE(DCR.conversion_rate, 1) AS DECIMAL(18, 2)) AS GBL_PREPAY_AMOUNT,
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
    ) || '-' || COALESCE(APSC.INVOICE_ID, 0) || '-' || COALESCE(APSC.PAYMENT_NUM, 0) AS dw_load_id,
    CURRENT_TIMESTAMP() AS dw_insert_date,
    CURRENT_TIMESTAMP() AS dw_update_date
  FROM (
    SELECT
      *
    FROM silver_bec_ods.AP_PAYMENT_SCHEDULES_ALL
    WHERE
      is_deleted_flg <> 'Y'
  ) AS APSC, (
    SELECT
      *
    FROM silver_bec_ods.AP_INVOICES_ALL
    WHERE
      is_deleted_flg <> 'Y'
  ) AS API, (
    SELECT
      *
    FROM silver_bec_ods.GL_DAILY_RATES
    WHERE
      is_deleted_flg <> 'Y'
  ) AS DCR
  WHERE
    1 = 1
    AND APSC.INVOICE_ID = API.INVOICE_ID
    AND APSC.AMOUNT_REMAINING <> 0
    AND DCR.TO_CURRENCY() = 'USD'
    AND DCR.CONVERSION_TYPE() = 'Corporate'
    AND API.invoice_currency_code = DCR.FROM_CURRENCY()
    AND DCR.CONVERSION_DATE() = API.invoice_date
    AND CASE
      WHEN api.payment_status_flag = 'Y'
      AND APSC.payment_status_flag = 'N'
      AND invoice_amount = amount_paid
      THEN 'N'
      ELSE 'Y'
    END = 'Y'
  GROUP BY
    INVOICE_ID_KEY,
    API.INVOICE_NUM,
    VENDOR_ID_KEY,
    ORG_ID_KEY,
    APSC.ORG_ID,
    apsc.payment_num,
    API.INVOICE_CURRENCY_CODE,
    API.PAYMENT_CURRENCY_CODE,
    VENDOR_SITE_ID_KEY,
    LEDGER_ID_KEY,
    TERMS_ID_KEY,
    APSC.INVOICE_ID,
    API.INVOICE_DATE,
    APSC.DUE_DATE,
    API.INVOICE_AMOUNT,
    API.AMOUNT_PAID,
    COALESCE(API.BASE_AMOUNT, API.INVOICE_AMOUNT),
    FLOOR(CURRENT_TIMESTAMP()) - FLOOR(APSC.DUE_DATE),
    GBL_INVOICE_AMOUNT,
    GBL_AMOUNT_PAID,
    APSC.AMOUNT_REMAINING,
    api.payment_status_flag,
    apsc.payment_status_flag,
    COALESCE(DCR.conversion_rate, 1)
);
UPDATE bec_etl_ctrl.batch_dw_info SET load_type = 'I', last_refresh_date = CURRENT_TIMESTAMP()
WHERE
  dw_table_name = 'fact_ap_aging' AND batch_name = 'ap';