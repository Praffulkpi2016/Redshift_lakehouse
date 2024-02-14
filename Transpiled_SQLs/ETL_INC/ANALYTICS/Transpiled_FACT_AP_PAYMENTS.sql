/* Delete Records */
DELETE FROM gold_bec_dwh.fact_ap_payments
WHERE
  (COALESCE(INVOICE_ID, 0), COALESCE(INVOICE_PAYMENT_ID, 0), COALESCE(PAYMENT_NUM, 0)) IN (
    SELECT
      COALESCE(ods.INVOICE_ID, 0) AS INVOICE_ID,
      COALESCE(ods.INVOICE_PAYMENT_ID, 0) AS INVOICE_PAYMENT_ID,
      COALESCE(ods.PAYMENT_NUM, 0) AS PAYMENT_NUM
    FROM gold_bec_dwh.fact_ap_payments AS dw, (
      SELECT
        (
          SELECT
            system_id
          FROM bec_etl_ctrl.etlsourceappid
          WHERE
            source_system = 'EBS'
        ) || '-' || APSCA.INVOICE_ID AS INVOICE_ID_KEY,
        APSCA.PAYMENT_NUM,
        APSCA.AMOUNT_REMAINING,
        APSCA.DISCOUNT_DATE,
        APSCA.DUE_DATE,
        APSCA.GROSS_AMOUNT,
        APSCA.PAYMENT_METHOD_CODE AS PAYMENT_METHOD_LOOKUP_CODE,
        APSCA.HOLD_FLAG,
        APSCA.PAYMENT_STATUS_FLAG,
        APSCA.SECOND_DISCOUNT_DATE,
        APSCA.THIRD_DISCOUNT_DATE,
        APSCA.DISCOUNT_AMOUNT_REMAINING,
        APSCA.SECOND_DISC_AMT_AVAILABLE,
        APSCA.THIRD_DISC_AMT_AVAILABLE,
        (
          SELECT
            system_id
          FROM bec_etl_ctrl.etlsourceappid
          WHERE
            source_system = 'EBS'
        ) || '-' || APSCA.ORG_ID AS ORG_ID_KEY,
        APSCA.ORG_ID AS ORG_ID,
        APSCA.IBY_HOLD_REASON,
        APSCA.REMIT_TO_SUPPLIER_NAME,
        APSCA.REMIT_TO_SUPPLIER_ID,
        APSCA.REMIT_TO_SUPPLIER_SITE,
        APSCA.REMIT_TO_SUPPLIER_SITE_ID,
        APSCA.INVOICE_ID,
        AIPA.ACCOUNTING_DATE,
        AIPA.AMOUNT,
        (
          SELECT
            system_id
          FROM bec_etl_ctrl.etlsourceappid
          WHERE
            source_system = 'EBS'
        ) || '-' || AIPA.CHECK_ID AS CHECK_ID_KEY,
        AIPA.INVOICE_PAYMENT_ID,
        AIPA.PERIOD_NAME,
        AIPA.POSTED_FLAG,
        (
          SELECT
            system_id
          FROM bec_etl_ctrl.etlsourceappid
          WHERE
            source_system = 'EBS'
        ) || '-' || AIPA.SET_OF_BOOKS_ID AS LEDGER_ID_KEY,
        (
          SELECT
            system_id
          FROM bec_etl_ctrl.etlsourceappid
          WHERE
            source_system = 'EBS'
        ) || '-' || AIPA.ACCTS_PAY_CODE_COMBINATION_ID AS GL_ACCOUNT_ID_KEY,
        (
          SELECT
            system_id
          FROM bec_etl_ctrl.etlsourceappid
          WHERE
            source_system = 'EBS'
        ) || '-' || AIA.TERMS_ID AS TERMS_ID_KEY,
        (
          SELECT
            system_id
          FROM bec_etl_ctrl.etlsourceappid
          WHERE
            source_system = 'EBS'
        ) || '-' || VENDOR_ID AS VENDOR_ID_KEY,
        (
          SELECT
            system_id
          FROM bec_etl_ctrl.etlsourceappid
          WHERE
            source_system = 'EBS'
        ) || '-' || VENDOR_SITE_ID AS VENDOR_SITE_ID_KEY,
        AIPA.ASSET_CODE_COMBINATION_ID,
        AIPA.BANK_ACCOUNT_NUM,
        AIPA.BANK_ACCOUNT_TYPE,
        AIPA.BANK_NUM,
        AIPA.DISCOUNT_LOST,
        AIPA.DISCOUNT_TAKEN,
        AIPA.EXCHANGE_DATE,
        AIPA.EXCHANGE_RATE,
        AIPA.EXCHANGE_RATE_TYPE,
        AIPA.INVOICE_BASE_AMOUNT AS `INV_AMT_FUNC_CURR`,
        AIPA.PAYMENT_BASE_AMOUNT AS `PAY_AMT_FUNC_CURR`,
        AIPA.ORG_ID AS `Payment Org`,
        AIPA.ACCOUNTING_EVENT_ID,
        AIPA.REVERSAL_FLAG,
        AIPA.REVERSAL_INV_PMT_ID,
        AIA.INVOICE_AMOUNT,
        AIA.AMOUNT_PAID,
        CAST(COALESCE(AIA.invoice_amount, 0) * COALESCE(DCR.conversion_rate, 1) AS DECIMAL(18, 2)) AS GBL_INVOICE_AMOUNT,
        CAST(COALESCE(AIA.amount_paid, 0) * COALESCE(DCR.conversion_rate, 1) AS DECIMAL(18, 2)) AS GBL_AMOUNT_PAID,
        CAST(COALESCE(AIPA.AMOUNT, 0) * COALESCE(DCR.conversion_rate, 1) AS DECIMAL(18, 2)) AS GBL_PAY_AMOUNT,
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
        ) || '-' || COALESCE(APSCA.INVOICE_ID, 0) || '-' || COALESCE(AIPA.INVOICE_PAYMENT_ID, 0) || '-' || COALESCE(APSCA.PAYMENT_NUM, 0) AS dw_load_id,
        CURRENT_TIMESTAMP() AS dw_insert_date,
        CURRENT_TIMESTAMP() AS dw_update_date
      FROM silver_bec_ods.AP_INVOICE_PAYMENTS_ALL AS AIPA, silver_bec_ods.AP_PAYMENT_SCHEDULES_ALL AS APSCA, silver_bec_ods.AP_INVOICES_ALL AS AIA, silver_bec_ods.GL_DAILY_RATES AS DCR
      WHERE
        1 = 1
        AND AIA.INVOICE_ID = APSCA.INVOICE_ID
        AND AIPA.INVOICE_ID() = APSCA.INVOICE_ID
        AND DCR.TO_CURRENCY() = 'USD'
        AND DCR.CONVERSION_TYPE() = 'Corporate'
        AND AIA.invoice_currency_code = DCR.FROM_CURRENCY()
        AND DCR.CONVERSION_DATE() = AIA.invoice_date
        AND (
          AIPA.kca_seq_date > (
            SELECT
              (
                executebegints - prune_days
              )
            FROM bec_etl_ctrl.batch_dw_info
            WHERE
              dw_table_name = 'fact_ap_payments' AND batch_name = 'ap'
          )
          OR APSCA.kca_seq_date > (
            SELECT
              (
                executebegints - prune_days
              )
            FROM bec_etl_ctrl.batch_dw_info
            WHERE
              dw_table_name = 'fact_ap_payments' AND batch_name = 'ap'
          )
          OR AIPA.is_deleted_flg = 'Y'
          OR APSCA.is_deleted_flg = 'Y'
          OR AIA.is_deleted_flg = 'Y'
          OR DCR.is_deleted_flg = 'Y'
        )
    ) AS ods
    WHERE
      dw.dw_load_id = (
        SELECT
          system_id
        FROM bec_etl_ctrl.etlsourceappid
        WHERE
          source_system = 'EBS'
      ) || '-' || COALESCE(ods.INVOICE_ID, 0) || '-' || COALESCE(ods.INVOICE_PAYMENT_ID, 0) || '-' || COALESCE(ods.PAYMENT_NUM, 0)
  );
/* Insert records */
INSERT INTO gold_bec_dwh.fact_ap_payments (
  INVOICE_ID_KEY,
  PAYMENT_NUM,
  AMOUNT_REMAINING,
  DISCOUNT_DATE,
  DUE_DATE,
  GROSS_AMOUNT,
  PAYMENT_METHOD_LOOKUP_CODE,
  HOLD_FLAG,
  PAYMENT_STATUS_FLAG,
  SECOND_DISCOUNT_DATE,
  THIRD_DISCOUNT_DATE,
  DISCOUNT_AMOUNT_REMAINING,
  SECOND_DISC_AMT_AVAILABLE,
  THIRD_DISC_AMT_AVAILABLE,
  ORG_ID_KEY,
  ORG_ID,
  IBY_HOLD_REASON,
  REMIT_TO_SUPPLIER_NAME,
  REMIT_TO_SUPPLIER_ID,
  REMIT_TO_SUPPLIER_SITE,
  REMIT_TO_SUPPLIER_SITE_ID,
  INVOICE_ID,
  ACCOUNTING_DATE,
  AMOUNT,
  CHECK_ID_KEY,
  INVOICE_PAYMENT_ID,
  PERIOD_NAME,
  POSTED_FLAG,
  LEDGER_ID_KEY,
  GL_ACCOUNT_ID_KEY,
  TERMS_ID_KEY,
  VENDOR_ID_KEY,
  VENDOR_SITE_ID_KEY,
  ASSET_CODE_COMBINATION_ID,
  BANK_ACCOUNT_NUM,
  BANK_ACCOUNT_TYPE,
  BANK_NUM,
  DISCOUNT_LOST,
  DISCOUNT_TAKEN,
  EXCHANGE_DATE,
  EXCHANGE_RATE,
  EXCHANGE_RATE_TYPE,
  `INV_AMT_FUNC_CURR`,
  `PAY_AMT_FUNC_CURR`,
  `Payment Org`,
  ACCOUNTING_EVENT_ID,
  REVERSAL_FLAG,
  REVERSAL_INV_PMT_ID,
  INVOICE_AMOUNT,
  AMOUNT_PAID,
  GBL_INVOICE_AMOUNT,
  GBL_AMOUNT_PAID,
  GBL_PAY_AMOUNT,
  IS_DELETED_FLG,
  source_app_id,
  dw_load_id,
  dw_insert_date,
  dw_update_date
)
(
  SELECT
    (
      SELECT
        system_id
      FROM bec_etl_ctrl.etlsourceappid
      WHERE
        source_system = 'EBS'
    ) || '-' || APSCA.INVOICE_ID AS INVOICE_ID_KEY,
    APSCA.PAYMENT_NUM,
    APSCA.AMOUNT_REMAINING,
    APSCA.DISCOUNT_DATE,
    APSCA.DUE_DATE,
    APSCA.GROSS_AMOUNT,
    APSCA.PAYMENT_METHOD_CODE AS PAYMENT_METHOD_LOOKUP_CODE,
    APSCA.HOLD_FLAG,
    APSCA.PAYMENT_STATUS_FLAG,
    APSCA.SECOND_DISCOUNT_DATE,
    APSCA.THIRD_DISCOUNT_DATE,
    APSCA.DISCOUNT_AMOUNT_REMAINING,
    APSCA.SECOND_DISC_AMT_AVAILABLE,
    APSCA.THIRD_DISC_AMT_AVAILABLE,
    (
      SELECT
        system_id
      FROM bec_etl_ctrl.etlsourceappid
      WHERE
        source_system = 'EBS'
    ) || '-' || APSCA.ORG_ID AS ORG_ID_KEY,
    APSCA.ORG_ID AS ORG_ID,
    APSCA.IBY_HOLD_REASON,
    APSCA.REMIT_TO_SUPPLIER_NAME,
    APSCA.REMIT_TO_SUPPLIER_ID,
    APSCA.REMIT_TO_SUPPLIER_SITE,
    APSCA.REMIT_TO_SUPPLIER_SITE_ID,
    APSCA.INVOICE_ID,
    AIPA.ACCOUNTING_DATE,
    AIPA.AMOUNT,
    (
      SELECT
        system_id
      FROM bec_etl_ctrl.etlsourceappid
      WHERE
        source_system = 'EBS'
    ) || '-' || AIPA.CHECK_ID AS CHECK_ID_KEY,
    AIPA.INVOICE_PAYMENT_ID,
    AIPA.PERIOD_NAME,
    AIPA.POSTED_FLAG,
    (
      SELECT
        system_id
      FROM bec_etl_ctrl.etlsourceappid
      WHERE
        source_system = 'EBS'
    ) || '-' || AIPA.SET_OF_BOOKS_ID AS LEDGER_ID_KEY,
    (
      SELECT
        system_id
      FROM bec_etl_ctrl.etlsourceappid
      WHERE
        source_system = 'EBS'
    ) || '-' || AIPA.ACCTS_PAY_CODE_COMBINATION_ID AS GL_ACCOUNT_ID_KEY,
    (
      SELECT
        system_id
      FROM bec_etl_ctrl.etlsourceappid
      WHERE
        source_system = 'EBS'
    ) || '-' || AIA.TERMS_ID AS TERMS_ID_KEY,
    (
      SELECT
        system_id
      FROM bec_etl_ctrl.etlsourceappid
      WHERE
        source_system = 'EBS'
    ) || '-' || VENDOR_ID AS VENDOR_ID_KEY,
    (
      SELECT
        system_id
      FROM bec_etl_ctrl.etlsourceappid
      WHERE
        source_system = 'EBS'
    ) || '-' || VENDOR_SITE_ID AS VENDOR_SITE_ID_KEY,
    AIPA.ASSET_CODE_COMBINATION_ID,
    AIPA.BANK_ACCOUNT_NUM,
    AIPA.BANK_ACCOUNT_TYPE,
    AIPA.BANK_NUM,
    AIPA.DISCOUNT_LOST,
    AIPA.DISCOUNT_TAKEN,
    AIPA.EXCHANGE_DATE,
    AIPA.EXCHANGE_RATE,
    AIPA.EXCHANGE_RATE_TYPE,
    AIPA.INVOICE_BASE_AMOUNT AS `INV_AMT_FUNC_CURR`,
    AIPA.PAYMENT_BASE_AMOUNT AS `PAY_AMT_FUNC_CURR`,
    AIPA.ORG_ID AS `Payment Org`,
    AIPA.ACCOUNTING_EVENT_ID,
    AIPA.REVERSAL_FLAG,
    AIPA.REVERSAL_INV_PMT_ID,
    AIA.INVOICE_AMOUNT,
    AIA.AMOUNT_PAID,
    CAST(COALESCE(AIA.invoice_amount, 0) * COALESCE(DCR.conversion_rate, 1) AS DECIMAL(18, 2)) AS GBL_INVOICE_AMOUNT,
    CAST(COALESCE(AIA.amount_paid, 0) * COALESCE(DCR.conversion_rate, 1) AS DECIMAL(18, 2)) AS GBL_AMOUNT_PAID,
    CAST(COALESCE(AIPA.AMOUNT, 0) * COALESCE(DCR.conversion_rate, 1) AS DECIMAL(18, 2)) AS GBL_PAY_AMOUNT,
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
    ) || '-' || COALESCE(APSCA.INVOICE_ID, 0) || '-' || COALESCE(AIPA.INVOICE_PAYMENT_ID, 0) || '-' || COALESCE(APSCA.PAYMENT_NUM, 0) AS dw_load_id,
    CURRENT_TIMESTAMP() AS dw_insert_date,
    CURRENT_TIMESTAMP() AS dw_update_date
  FROM (
    SELECT
      *
    FROM silver_bec_ods.AP_INVOICE_PAYMENTS_ALL
    WHERE
      is_deleted_flg <> 'Y'
  ) AS AIPA, (
    SELECT
      *
    FROM silver_bec_ods.AP_PAYMENT_SCHEDULES_ALL
    WHERE
      is_deleted_flg <> 'Y'
  ) AS APSCA, (
    SELECT
      *
    FROM silver_bec_ods.AP_INVOICES_ALL
    WHERE
      is_deleted_flg <> 'Y'
  ) AS AIA, (
    SELECT
      *
    FROM silver_bec_ods.GL_DAILY_RATES
    WHERE
      is_deleted_flg <> 'Y'
  ) AS DCR
  WHERE
    1 = 1
    AND AIA.INVOICE_ID = APSCA.INVOICE_ID
    AND AIPA.INVOICE_ID() = APSCA.INVOICE_ID
    AND DCR.TO_CURRENCY() = 'USD'
    AND DCR.CONVERSION_TYPE() = 'Corporate'
    AND AIA.invoice_currency_code = DCR.FROM_CURRENCY()
    AND DCR.CONVERSION_DATE() = AIA.invoice_date
    AND (
      AIPA.kca_seq_date > (
        SELECT
          (
            executebegints - prune_days
          )
        FROM bec_etl_ctrl.batch_dw_info
        WHERE
          dw_table_name = 'fact_ap_payments' AND batch_name = 'ap'
      )
      OR APSCA.kca_seq_date > (
        SELECT
          (
            executebegints - prune_days
          )
        FROM bec_etl_ctrl.batch_dw_info
        WHERE
          dw_table_name = 'fact_ap_payments' AND batch_name = 'ap'
      )
    )
);
UPDATE bec_etl_ctrl.batch_dw_info SET last_refresh_date = CURRENT_TIMESTAMP()
WHERE
  dw_table_name = 'fact_ap_payments' AND batch_name = 'ap';