DROP table IF EXISTS gold_bec_dwh.FACT_AP_HOLDS;
CREATE TABLE gold_bec_dwh.FACT_AP_HOLDS AS
(
  SELECT
    (
      SELECT
        system_id
      FROM bec_etl_ctrl.etlsourceappid
      WHERE
        source_system = 'EBS'
    ) || '-' || ah.invoice_id AS INVOICE_ID_KEY,
    ah.hold_id,
    (
      SELECT
        system_id
      FROM bec_etl_ctrl.etlsourceappid
      WHERE
        source_system = 'EBS'
    ) || '-' || ai.vendor_id AS VENDOR_ID_KEY,
    (
      SELECT
        system_id
      FROM bec_etl_ctrl.etlsourceappid
      WHERE
        source_system = 'EBS'
    ) || '-' || ai.VENDOR_SITE_ID AS VENDOR_SITE_ID_KEY,
    (
      SELECT
        system_id
      FROM bec_etl_ctrl.etlsourceappid
      WHERE
        source_system = 'EBS'
    ) || '-' || SET_OF_BOOKS_ID AS LEDGER_ID_KEY,
    ah.line_location_id,
    ah.hold_lookup_code,
    ah.release_lookup_code,
    ah.held_by,
    (
      SELECT
        system_id
      FROM bec_etl_ctrl.etlsourceappid
      WHERE
        source_system = 'EBS'
    ) || '-' || ah.ORG_ID AS ORG_ID_KEY,
    ah.ORG_ID AS ORG_ID,
    CASE WHEN ah.RELEASE_LOOKUP_CODE IS NULL THEN NULL ELSE ah.LAST_UPDATE_DATE END AS RELEASE_DATE,
    RES.RELEASE_BY_USER_NAME,
    ah.hold_date,
    ah.hold_reason,
    ah.release_reason,
    ah.status_flag,
    ah.rcv_transaction_id,
    ah.creation_date,
    ah.last_update_date,
    ah.invoice_id,
    ai.invoice_amount,
    COALESCE(ai.BASE_AMOUNT, ai.INVOICE_AMOUNT) AS `INVOICE_BASE_AMOUNT`,
    CAST(COALESCE(ai.invoice_amount, 0) * COALESCE(DCR.conversion_rate, 1) AS DECIMAL(18, 2)) AS GBL_INVOICE_AMOUNT,
    CAST(COALESCE(ai.amount_paid, 0) * COALESCE(DCR.conversion_rate, 1) AS DECIMAL(18, 2)) AS GBL_AMOUNT_PAID,
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
    ) || '-' || COALESCE(ah.invoice_id, 0) || '-' || COALESCE(ah.hold_id, 0) AS dw_load_id,
    CURRENT_TIMESTAMP() AS dw_insert_date,
    CURRENT_TIMESTAMP() AS dw_update_date
  FROM (
    SELECT
      *
    FROM silver_bec_ods.ap_holds_all
    WHERE
      is_deleted_flg <> 'Y'
  ) AS ah, (
    SELECT
      *
    FROM silver_bec_ods.ap_invoices_all
    WHERE
      is_deleted_flg <> 'Y'
  ) AS ai, (
    SELECT
      *
    FROM silver_bec_ods.GL_DAILY_RATES
    WHERE
      is_deleted_flg <> 'Y'
  ) AS DCR, (
    SELECT DISTINCT
      invoice_id,
      hold_id,
      CASE
        WHEN AH.RELEASE_LOOKUP_CODE IS NULL
        THEN NULL
        ELSE CASE
          WHEN AH.LAST_UPDATED_BY = 5
          THEN (
            SELECT
              ALC6.meaning
            FROM (
              SELECT
                *
              FROM silver_bec_ods.fnd_lookup_values
              WHERE
                is_deleted_flg <> 'Y'
            ) AS ALC6
            WHERE
              ALC6.LOOKUP_TYPE() = 'NLS TRANSLATION'
              AND ALC6.LOOKUP_CODE() = 'SYSTEM'
              AND ALC6.LANGUAGE = 'US'
          )
          ELSE FU2.USER_NAME
        END
      END AS RELEASE_BY_USER_NAME
    FROM (
      SELECT
        *
      FROM silver_bec_ods.ap_holds_all
      WHERE
        is_deleted_flg <> 'Y'
    ) AS ah, (
      SELECT
        *
      FROM silver_bec_ods.FND_USER
      WHERE
        is_deleted_flg <> 'Y'
    ) AS FU2
    WHERE
      AH.LAST_UPDATED_BY = FU2.USER_ID()
  ) AS res
  WHERE
    1 = 1
    AND ah.invoice_id = ai.invoice_id
    AND DCR.TO_CURRENCY() = 'USD'
    AND DCR.CONVERSION_TYPE() = 'Corporate'
    AND ai.invoice_currency_code = DCR.FROM_CURRENCY()
    AND DCR.CONVERSION_DATE() = ai.invoice_date
    AND ah.invoice_id = res.INVOICE_ID()
    AND ah.hold_id = res.HOLD_ID()
);
UPDATE bec_etl_ctrl.batch_dw_info SET load_type = 'I', last_refresh_date = CURRENT_TIMESTAMP()
WHERE
  dw_table_name = 'fact_ap_holds' AND batch_name = 'ap';