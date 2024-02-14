/* Delete Records */
DELETE FROM gold_bec_dwh.fact_ap_holds
WHERE
  (COALESCE(invoice_id, 0), COALESCE(hold_id, 0)) IN (
    SELECT
      COALESCE(ods.invoice_id, 0) AS invoice_id,
      COALESCE(ods.hold_id, 0) AS hold_id
    FROM gold_bec_dwh.fact_ap_holds AS dw, (
      SELECT
        ah.invoice_id,
        ah.hold_id,
        ai.last_update_date,
        ah.kca_operation,
        ai.kca_seq_date,
        ah.is_deleted_flg,
        ai.is_deleted_flg AS is_deleted_flg1,
        dcr.is_deleted_flg AS is_deleted_flg2,
        res.is_deleted_flg3,
        res.is_deleted_flg4
      FROM silver_bec_ods.ap_holds_all AS ah, silver_bec_ods.ap_invoices_all AS ai, silver_bec_ods.GL_DAILY_RATES AS DCR, (
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
                FROM silver_bec_ods.fnd_lookup_values AS ALC6
                WHERE
                  ALC6.LOOKUP_TYPE() = 'NLS TRANSLATION'
                  AND ALC6.LOOKUP_CODE() = 'SYSTEM'
                  AND ALC6.LANGUAGE = 'US'
              )
              ELSE FU2.USER_NAME
            END
          END AS RELEASE_BY_USER_NAME,
          ah.is_deleted_flg AS is_deleted_flg3,
          FU2.is_deleted_flg AS is_deleted_flg4
        FROM silver_bec_ods.ap_holds_all AS ah, silver_bec_ods.FND_USER AS FU2
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
    ) AS ods
    WHERE
      dw.dw_load_id = (
        SELECT
          system_id
        FROM bec_etl_ctrl.etlsourceappid
        WHERE
          source_system = 'EBS'
      ) || '-' || COALESCE(ods.invoice_id, 0) || '-' || COALESCE(ods.hold_id, 0)
      AND (
        ods.kca_seq_date > (
          SELECT
            (
              executebegints - prune_days
            )
          FROM bec_etl_ctrl.batch_dw_info
          WHERE
            dw_table_name = 'fact_ap_holds' AND batch_name = 'ap'
        )
        OR ods.is_deleted_flg = 'Y'
        OR ods.is_deleted_flg1 = 'Y'
        OR ods.is_deleted_flg2 = 'Y'
        OR ods.is_deleted_flg3 = 'Y'
        OR ods.is_deleted_flg4 = 'Y'
      )
  );
/* Insert records */
INSERT INTO gold_bec_dwh.FACT_AP_HOLDS (
  INVOICE_ID_KEY,
  hold_id,
  VENDOR_ID_KEY,
  VENDOR_SITE_ID_KEY,
  LEDGER_ID_KEY,
  line_location_id,
  hold_lookup_code,
  release_lookup_code,
  held_by,
  ORG_ID_KEY,
  ORG_ID,
  RELEASE_DATE,
  RELEASE_BY_USER_NAME,
  hold_date,
  hold_reason,
  release_reason,
  status_flag,
  rcv_transaction_id,
  creation_date,
  last_update_date,
  invoice_id,
  invoice_amount,
  invoice_base_amount,
  GBL_INVOICE_AMOUNT,
  GBL_AMOUNT_PAID,
  is_deleted_flg,
  source_app_id,
  dw_load_id,
  dw_insert_date,
  dw_update_date
)
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
  AND (
    ai.kca_seq_date > (
      SELECT
        (
          executebegints - prune_days
        )
      FROM bec_etl_ctrl.batch_dw_info
      WHERE
        dw_table_name = 'fact_ap_holds' AND batch_name = 'ap'
    )
  );
UPDATE bec_etl_ctrl.batch_dw_info SET load_type = 'I', last_refresh_date = CURRENT_TIMESTAMP()
WHERE
  dw_table_name = 'fact_ap_holds' AND batch_name = 'ap';