/* Delete Records */
DELETE FROM gold_bec_dwh.fact_ap_invoice
WHERE
  INVOICE_ID IN (
    SELECT
      ods.INVOICE_ID
    FROM gold_bec_dwh.fact_ap_invoice AS dw, silver_bec_ods.AP_INVOICES_ALL AS ODS
    WHERE
      dw.dw_load_id = (
        SELECT
          system_id
        FROM bec_etl_ctrl.etlsourceappid
        WHERE
          source_system = 'EBS'
      ) || '-' || COALESCE(ods.INVOICE_ID, 0)
      AND (
        ods.kca_seq_date > (
          SELECT
            (
              executebegints - prune_days
            )
          FROM bec_etl_ctrl.batch_dw_info
          WHERE
            dw_table_name = 'fact_ap_invoice' AND batch_name = 'ap'
        )
        OR ods.is_deleted_flg = 'Y'
      )
  );
/* Insert records */
INSERT INTO gold_bec_dwh.FACT_AP_INVOICE (
  INVOICE_ID_KEY,
  VENDOR_ID_KEY,
  VENDOR_SITE_ID_KEY,
  LEDGER_ID_KEY,
  BATCH_ID_KEY,
  INVOICE_NUM,
  INVOICE_AMOUNT,
  AMOUNT_PAID,
  DISCOUNT_AMOUNT_TAKEN,
  INVOICE_DATE,
  INVOICE_ID,
  TERMS_ID_KEY,
  TERMS_DATE,
  PO_HEADER_ID,
  FREIGHT_AMOUNT,
  INVOICE_RECEIVED_DATE,
  EXCHANGE_RATE,
  EXCHANGE_DATE,
  CANCELLED_DATE,
  CREATION_DATE,
  LAST_UPDATE_DATE,
  CANCELLED_AMOUNT,
  PROJECT_ID_KEY,
  PROJECT_TASK_ID_KEY,
  ORG_ID_KEY,
  ORG_ID,
  PAY_CURR_INVOICE_AMOUNT,
  GL_DATE,
  TOTAL_TAX_AMOUNT,
  LEGAL_ENTITY_ID,
  PARTY_ID,
  PARTY_SITE_ID,
  AMOUNT_APPLICABLE_TO_DISCOUNT,
  GL_ACCOUNT_ID_KEY,
  INVOICE_BASE_AMOUNT,
  ORIGINAL_PREPAYMENT_AMOUNT,
  GBL_INVOICE_AMOUNT,
  GBL_AMOUNT_PAID,
  terms_id,
  approval_status_code,
  VALIDATE_CNT,
  VALIDATE_CNT1,
  NVALIDATE_CNT,
  REVAL_CNT,
  STOPPED_CNT,
  hold_cnt,
  approver_name,
  aproval_action_date,
  IS_DELETED_FLG,
  SOURCE_APP_ID,
  DW_LOAD_ID,
  DW_INSERT_DATE,
  DW_UPDATE_DATE,
  invoice_status
)
(
  SELECT
    inv.*,
    (
      CASE
        WHEN NOT cancelled_Date IS NULL
        THEN 'Cancelled'
        WHEN (
          VALIDATE_CNT + VALIDATE_CNT1 > 0
          AND NVALIDATE_CNT = 0
          AND REVAL_CNT = 0
          AND STOPPED_CNT = 0
          AND hold_cnt = 0
        )
        THEN 'Validated'
        WHEN (
          VALIDATE_CNT + VALIDATE_CNT1 = 0 AND NVALIDATE_CNT > 0 AND REVAL_CNT = 0 AND STOPPED_CNT = 0
        )
        THEN 'Never Validated'
        WHEN (
          VALIDATE_CNT + VALIDATE_CNT1 = 0 AND NVALIDATE_CNT = 0 AND REVAL_CNT = 0 AND STOPPED_CNT = 0
        )
        THEN 'Never Validated'
        WHEN (
          VALIDATE_CNT + VALIDATE_CNT1 = 0
          OR (
            NVALIDATE_CNT > 0 OR REVAL_CNT > 0 OR hold_cnt > 0
          )
          OR STOPPED_CNT = 0
        )
        THEN 'Needs Revalidation'
        ELSE 'Never Validated'
      END
    ) AS invoice_status
  FROM (
    SELECT
      (
        SELECT
          system_id
        FROM bec_etl_ctrl.etlsourceappid
        WHERE
          source_system = 'EBS'
      ) || '-' || FAP.INVOICE_ID AS INVOICE_ID_KEY,
      (
        SELECT
          system_id
        FROM bec_etl_ctrl.etlsourceappid
        WHERE
          source_system = 'EBS'
      ) || '-' || FAP.VENDOR_ID AS VENDOR_ID_KEY,
      (
        SELECT
          system_id
        FROM bec_etl_ctrl.etlsourceappid
        WHERE
          source_system = 'EBS'
      ) || '-' || FAP.VENDOR_SITE_ID AS VENDOR_SITE_ID_KEY,
      (
        SELECT
          system_id
        FROM bec_etl_ctrl.etlsourceappid
        WHERE
          source_system = 'EBS'
      ) || '-' || FAP.SET_OF_BOOKS_ID AS LEDGER_ID_KEY,
      (
        SELECT
          system_id
        FROM bec_etl_ctrl.etlsourceappid
        WHERE
          source_system = 'EBS'
      ) || '-' || COALESCE(FAP.BATCH_ID, 0) AS BATCH_ID_KEY,
      FAP.INVOICE_NUM,
      FAP.INVOICE_AMOUNT,
      FAP.AMOUNT_PAID,
      FAP.DISCOUNT_AMOUNT_TAKEN,
      FAP.INVOICE_DATE,
      FAP.INVOICE_ID,
      (
        SELECT
          system_id
        FROM bec_etl_ctrl.etlsourceappid
        WHERE
          source_system = 'EBS'
      ) || '-' || FAP.TERMS_ID AS TERMS_ID_KEY,
      FAP.TERMS_DATE,
      FAP.PO_HEADER_ID,
      FAP.FREIGHT_AMOUNT,
      FAP.INVOICE_RECEIVED_DATE,
      FAP.EXCHANGE_RATE,
      FAP.EXCHANGE_DATE,
      FAP.CANCELLED_DATE,
      FAP.CREATION_DATE,
      FAP.LAST_UPDATE_DATE,
      FAP.CANCELLED_AMOUNT,
      (
        SELECT
          system_id
        FROM bec_etl_ctrl.etlsourceappid
        WHERE
          source_system = 'EBS'
      ) || '-' || FAP.PROJECT_ID AS PROJECT_ID_KEY,
      (
        SELECT
          system_id
        FROM bec_etl_ctrl.etlsourceappid
        WHERE
          source_system = 'EBS'
      ) || '-' || FAP.PROJECT_ID || '-' || TASK_ID AS PROJECT_TASK_ID_KEY,
      (
        SELECT
          system_id
        FROM bec_etl_ctrl.etlsourceappid
        WHERE
          source_system = 'EBS'
      ) || '-' || FAP.ORG_ID AS ORG_ID_KEY,
      FAP.ORG_ID AS ORG_ID,
      FAP.PAY_CURR_INVOICE_AMOUNT,
      FAP.GL_DATE,
      FAP.TOTAL_TAX_AMOUNT,
      FAP.LEGAL_ENTITY_ID,
      FAP.PARTY_ID,
      FAP.PARTY_SITE_ID,
      FAP.AMOUNT_APPLICABLE_TO_DISCOUNT,
      (
        SELECT
          system_id
        FROM bec_etl_ctrl.etlsourceappid
        WHERE
          source_system = 'EBS'
      ) || '-' || FAP.ACCTS_PAY_CODE_COMBINATION_ID AS GL_ACCOUNT_ID_KEY,
      COALESCE(FAP.BASE_AMOUNT, FAP.INVOICE_AMOUNT) AS `INVOICE_BASE_AMOUNT`,
      FAP.ORIGINAL_PREPAYMENT_AMOUNT,
      CAST(COALESCE(FAP.invoice_amount, 0) * COALESCE(DCR.conversion_rate, 1) AS DECIMAL(18, 2)) AS GBL_INVOICE_AMOUNT,
      CAST(COALESCE(FAP.amount_paid, 0) * COALESCE(DCR.conversion_rate, 1) AS DECIMAL(18, 2)) AS GBL_AMOUNT_PAID,
      fap.terms_id,
      fap.WFAPPROVAL_STATUS AS approval_status_code, /* AP_WFAPPROVAL_STATUS */
      (
        SELECT
          COUNT(1)
        FROM silver_bec_ods.AP_INVOICE_DISTRIBUTIONS_ALL AS AID
        WHERE
          AID.INVOICE_ID = FAP.INVOICE_ID AND MATCH_STATUS_FLAG = 'A'
      ) AS VALIDATE_CNT,
      (
        SELECT
          COUNT(1)
        FROM silver_bec_ods.AP_INVOICE_DISTRIBUTIONS_ALL AS AID
        WHERE
          AID.INVOICE_ID = FAP.INVOICE_ID
          AND MATCH_STATUS_FLAG = 'T'
          AND COALESCE(encumbered_flag, 'N') = 'N'
      ) AS VALIDATE_CNT1,
      (
        SELECT
          COUNT(1)
        FROM silver_bec_ods.AP_INVOICE_DISTRIBUTIONS_ALL AS AID
        WHERE
          AID.INVOICE_ID = FAP.INVOICE_ID
          AND (
            MATCH_STATUS_FLAG = 'N' OR MATCH_STATUS_FLAG IS NULL
          )
      ) AS NVALIDATE_CNT,
      (
        SELECT
          COUNT(1)
        FROM silver_bec_ods.AP_INVOICE_DISTRIBUTIONS_ALL AS AID
        WHERE
          AID.INVOICE_ID = FAP.INVOICE_ID
          AND MATCH_STATUS_FLAG = 'T'
          AND COALESCE(encumbered_flag, 'N') = 'Y'
      ) AS REVAL_CNT,
      (
        SELECT
          COUNT(1)
        FROM silver_bec_ods.AP_INVOICE_DISTRIBUTIONS_ALL AS AID
        WHERE
          AID.INVOICE_ID = FAP.INVOICE_ID AND MATCH_STATUS_FLAG = 'S'
      ) AS STOPPED_CNT,
      (
        SELECT
          COUNT(1)
        /* INTO   invoice_holds */
        FROM silver_bec_ods.ap_holds_all
        WHERE
          invoice_id = FAP.INVOICE_ID AND release_lookup_code IS NULL
      ) AS hold_cnt,
      (
        SELECT
          approver_name
        FROM silver_bec_ods.ap_inv_aprvl_hist_all
        WHERE
          invoice_id = fap.invoice_id AND approval_history_id = apv.approval_history_id
      ) AS approver_name,
      (
        SELECT
          creation_date
        FROM silver_bec_ods.ap_inv_aprvl_hist_all
        WHERE
          invoice_id = fap.invoice_id AND approval_history_id = apv.approval_history_id
      ) AS aproval_action_date,
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
      ) || '-' || COALESCE(FAP.INVOICE_ID, 0) AS dw_load_id,
      CURRENT_TIMESTAMP() AS dw_insert_date,
      CURRENT_TIMESTAMP() AS dw_update_date
    FROM (
      SELECT
        *
      FROM silver_bec_ods.AP_INVOICES_ALL
      WHERE
        is_deleted_flg <> 'Y'
    ) AS FAP, (
      SELECT
        *
      FROM silver_bec_ods.GL_DAILY_RATES
      WHERE
        is_deleted_flg <> 'Y'
    ) AS DCR, (
      SELECT
        invoice_id,
        MAX(approval_history_id) AS approval_history_id
      FROM silver_bec_ods.ap_inv_aprvl_hist_all
      WHERE
        is_deleted_flg <> 'Y'
      GROUP BY
        invoice_id
    ) AS apv
    WHERE
      1 = 1
      AND DCR.TO_CURRENCY() = 'USD'
      AND DCR.CONVERSION_TYPE() = 'Corporate'
      AND FAP.invoice_currency_code = DCR.FROM_CURRENCY()
      AND DCR.CONVERSION_DATE() = FAP.invoice_date
      AND fap.invoice_id = apv.INVOICE_ID()
      AND (
        FAP.kca_seq_date > (
          SELECT
            (
              executebegints - prune_days
            )
          FROM bec_etl_ctrl.batch_dw_info
          WHERE
            dw_table_name = 'fact_ap_invoice' AND batch_name = 'ap'
        )
      )
  ) AS inv
);
UPDATE bec_etl_ctrl.batch_dw_info SET last_refresh_date = CURRENT_TIMESTAMP()
WHERE
  dw_table_name = 'fact_ap_invoice' AND batch_name = 'ap';