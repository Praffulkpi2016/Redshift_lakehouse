/* Delete Records */
DELETE FROM gold_bec_dwh.FACT_AR_ADJUSTMENTS
WHERE
  ADJUSTMENT_ID IN (
    SELECT
      ods.ADJUSTMENT_ID
    FROM gold_bec_dwh.FACT_AR_ADJUSTMENTS AS dw, silver_bec_ods.ar_adjustments_all AS ods
    WHERE
      dw.dw_load_id = (
        SELECT
          system_id
        FROM bec_etl_ctrl.etlsourceappid
        WHERE
          source_system = 'EBS'
      ) || '-' || COALESCE(ods.ADJUSTMENT_ID, 0)
      AND (
        ods.kca_seq_date > (
          SELECT
            (
              executebegints - prune_days
            )
          FROM bec_etl_ctrl.batch_dw_info
          WHERE
            dw_table_name = 'fact_ar_adjustments' AND batch_name = 'ar'
        )
        OR ods.is_deleted_flg = 'Y'
      )
  );
/* Insert records */
INSERT INTO gold_bec_dwh.FACT_AR_ADJUSTMENTS (
  adjusment_info,
  adjustment_id,
  adjustment_number,
  dist_source_id,
  dist_source_table,
  dist_source_type,
  adj_status,
  adj_org_id,
  org_id_key,
  ORG_ID,
  adj_apply_date,
  adj_gl_date,
  adj_set_of_books_id,
  ledger_id_key,
  adj_type,
  adj_adjust_type,
  adj_gl_post_date,
  adj_amount,
  adj_line_adjusted,
  adj_freight_adjusted,
  adj_tax_adjusted,
  adj_rec_charge_adjusted,
  invoice_info,
  inv_trx_id,
  inv_trx_type_id_key,
  inv_trx_number,
  inv_trx_date,
  inv_sold_to_site_use_id,
  inv_bill_to_cust_id_key,
  inv_bill_site_use_id_key,
  inv_bill_to_contact_id,
  inv_ship_site_use_id_key,
  inv_term_id_key,
  inv_sales_rep_id_key,
  primary_salesrep_id,
  term_id,
  ship_to_site_use_id,
  bill_to_site_use_id,
  bill_to_customer_id,
  cust_trx_type_id,
  inv_po,
  inv_territory,
  inv_currency_code,
  receipt_method_id_key,
  receipt_method_id,
  interface_header_attribute1,
  interface_header_context,
  inv_pay_site_use_id_key,
  paying_site_use_id,
  inv_legal_entity_id,
  proj_num,
  inv_payment_schdule_id,
  inv_due_date,
  inv_cust_site_id_key,
  inv_cust_site_use_id_key,
  customer_site_use_id,
  terms_sequence_number,
  inv_status,
  adjustment_dist_info,
  line_id,
  amount_dr,
  amount_cr,
  acctd_amount_dr,
  acctd_amount_cr,
  last_update_date,
  code_combination_id_key,
  code_combination_id,
  GBL_AMOUNT,
  GBL_ADJ_LINE_ADJUSTED,
  GBL_ADJ_FREIGHT_ADJUSTED,
  GBL_ADJ_REC_CHARGE_ADJUSTED,
  GBL_ADJ_TAX_ADJUSTED,
  is_deleted_flg,
  source_app_id,
  dw_load_id,
  dw_insert_date,
  dw_update_date
)
(
  SELECT
    'ADJUSTMENT_INFO' AS Adjusment_Info,
    AAA.ADJUSTMENT_ID AS ADJUSTMENT_ID,
    AAA.ADJUSTMENT_NUMBER AS ADJUSTMENT_NUMBER,
    ADAL.SOURCE_ID AS DIST_SOURCE_ID,
    ADAL.SOURCE_TABLE AS DIST_SOURCE_TABLE,
    ADAL.SOURCE_TYPE AS DIST_SOURCE_TYPE,
    AAA.STATUS AS ADJ_STATUS,
    AAA.ORG_ID AS ADJ_ORG_ID,
    (
      SELECT
        system_id
      FROM bec_etl_ctrl.etlsourceappid
      WHERE
        source_system = 'EBS'
    ) || '-' || AAA.ORG_ID AS ORG_ID_KEY,
    AAA.ORG_ID AS ORG_ID,
    AAA.APPLY_DATE AS ADJ_APPLY_DATE,
    AAA.GL_DATE AS ADJ_GL_DATE,
    AAA.SET_OF_BOOKS_ID AS ADJ_SET_OF_BOOKS_ID,
    (
      SELECT
        system_id
      FROM bec_etl_ctrl.etlsourceappid
      WHERE
        source_system = 'EBS'
    ) || '-' || AAA.SET_OF_BOOKS_ID AS LEDGER_ID_KEY,
    AAA.TYPE AS ADJ_TYPE,
    AAA.ADJUSTMENT_TYPE AS ADJ_ADJUST_TYPE,
    AAA.GL_POSTED_DATE AS ADJ_GL_POST_DATE,
    AAA.AMOUNT AS ADJ_AMOUNT,
    AAA.LINE_ADJUSTED AS ADJ_LINE_ADJUSTED,
    AAA.FREIGHT_ADJUSTED AS ADJ_FREIGHT_ADJUSTED,
    AAA.TAX_ADJUSTED AS ADJ_TAX_ADJUSTED,
    AAA.RECEIVABLES_CHARGES_ADJUSTED AS ADJ_REC_CHARGE_ADJUSTED,
    'INVOICE_INFO' AS Invoice_Info,
    RCTA.CUSTOMER_TRX_ID AS INV_TRX_ID,
    (
      SELECT
        system_id
      FROM bec_etl_ctrl.etlsourceappid
      WHERE
        source_system = 'EBS'
    ) || '-' || RCTA.CUST_TRX_TYPE_ID AS INV_TRX_TYPE_ID_KEY,
    RCTA.TRX_NUMBER AS INV_TRX_NUMBER,
    RCTA.TRX_DATE AS INV_TRX_DATE,
    RCTA.SOLD_TO_SITE_USE_ID AS Inv_Sold_to_Site_Use_Id,
    (
      SELECT
        system_id
      FROM bec_etl_ctrl.etlsourceappid
      WHERE
        source_system = 'EBS'
    ) || RCTA.BILL_TO_CUSTOMER_ID AS INV_BILL_TO_CUST_ID_KEY,
    (
      SELECT
        system_id
      FROM bec_etl_ctrl.etlsourceappid
      WHERE
        source_system = 'EBS'
    ) || '-' || RCTA.BILL_TO_SITE_USE_ID AS Inv_Bill_Site_Use_Id_KEY,
    RCTA.BILL_TO_CONTACT_ID AS Inv_Bill_To_Contact_Id,
    (
      SELECT
        system_id
      FROM bec_etl_ctrl.etlsourceappid
      WHERE
        source_system = 'EBS'
    ) || '-' || RCTA.SHIP_TO_SITE_USE_ID AS Inv_Ship_Site_Use_Id_KEY,
    (
      SELECT
        system_id
      FROM bec_etl_ctrl.etlsourceappid
      WHERE
        source_system = 'EBS'
    ) || '-' || RCTA.TERM_ID AS INV_TERM_ID_KEY,
    (
      SELECT
        system_id
      FROM bec_etl_ctrl.etlsourceappid
      WHERE
        source_system = 'EBS'
    ) || '-' || RCTA.PRIMARY_SALESREP_ID AS INV_SALES_REP_ID_KEY,
    RCTA.PRIMARY_SALESREP_ID,
    RCTA.TERM_ID,
    RCTA.SHIP_TO_SITE_USE_ID,
    RCTA.BILL_TO_SITE_USE_ID,
    RCTA.BILL_TO_CUSTOMER_ID,
    RCTA.CUST_TRX_TYPE_ID,
    RCTA.PURCHASE_ORDER AS Inv_PO,
    RCTA.TERRITORY_ID AS Inv_Territory,
    RCTA.INVOICE_CURRENCY_CODE AS Inv_Currency_Code,
    (
      SELECT
        system_id
      FROM bec_etl_ctrl.etlsourceappid
      WHERE
        source_system = 'EBS'
    ) || '-' || RCTA.RECEIPT_METHOD_ID AS RECEIPT_METHOD_ID_KEY,
    RCTA.RECEIPT_METHOD_ID,
    RCTA.INTERFACE_HEADER_ATTRIBUTE1,
    RCTA.INTERFACE_HEADER_CONTEXT,
    (
      SELECT
        system_id
      FROM bec_etl_ctrl.etlsourceappid
      WHERE
        source_system = 'EBS'
    ) || '-' || RCTA.PAYING_SITE_USE_ID AS INV_PAY_SITE_USE_ID_KEY,
    RCTA.PAYING_SITE_USE_ID,
    RCTA.LEGAL_ENTITY_ID AS Inv_Legal_Entity_Id,
    RCTA.INTERFACE_HEADER_ATTRIBUTE1 AS PROJ_NUM,
    APSA.PAYMENT_SCHEDULE_ID AS Inv_Payment_Schdule_Id,
    APSA.DUE_DATE AS Inv_Due_Date,
    (
      SELECT
        system_id
      FROM bec_etl_ctrl.etlsourceappid
      WHERE
        source_system = 'EBS'
    ) || '-' || (
      SELECT
        CUST_ACCT_SITE_ID
      FROM silver_bec_ods.HZ_CUST_SITE_USES_ALL
      WHERE
        is_deleted_flg <> 'Y' AND site_use_id = APSA.CUSTOMER_SITE_USE_ID
    ) AS INV_CUST_SITE_ID_KEY,
    (
      SELECT
        system_id
      FROM bec_etl_ctrl.etlsourceappid
      WHERE
        source_system = 'EBS'
    ) || '-' || APSA.CUSTOMER_SITE_USE_ID AS INV_CUST_SITE_USE_ID_KEY,
    APSA.CUSTOMER_SITE_USE_ID,
    APSA.TERMS_SEQUENCE_NUMBER,
    APSA.STATUS AS Inv_Status,
    'ADJUSTMENT_DIST_INFO' AS Adjustment_Dist_Info,
    ADAL.LINE_ID,
    ADAL.AMOUNT_DR,
    ADAL.AMOUNT_CR,
    ADAL.ACCTD_AMOUNT_DR,
    ADAL.ACCTD_AMOUNT_CR,
    AAA.LAST_UPDATE_DATE,
    (
      SELECT
        system_id
      FROM bec_etl_ctrl.etlsourceappid
      WHERE
        source_system = 'EBS'
    ) || '-' || ADAL.CODE_COMBINATION_ID AS CODE_COMBINATION_ID_KEY,
    ADAL.CODE_COMBINATION_ID,
    CAST(COALESCE(AAA.AMOUNT, 0) * COALESCE(DCR.conversion_rate, 1) AS DECIMAL(18, 2)) AS GBL_AMOUNT,
    CAST(COALESCE(AAA.LINE_ADJUSTED, 0) * COALESCE(DCR.conversion_rate, 1) AS DECIMAL(18, 2)) AS GBL_ADJ_LINE_ADJUSTED,
    CAST(COALESCE(AAA.FREIGHT_ADJUSTED, 0) * COALESCE(DCR.conversion_rate, 1) AS DECIMAL(18, 2)) AS GBL_ADJ_FREIGHT_ADJUSTED,
    CAST(COALESCE(AAA.RECEIVABLES_CHARGES_ADJUSTED, 0) * COALESCE(DCR.conversion_rate, 1) AS DECIMAL(18, 2)) AS GBL_ADJ_REC_CHARGE_ADJUSTED,
    CAST(COALESCE(AAA.TAX_ADJUSTED, 0) * COALESCE(DCR.conversion_rate, 1) AS DECIMAL(18, 2)) AS GBL_ADJ_TAX_ADJUSTED,
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
    ) || '-' || COALESCE(AAA.ADJUSTMENT_ID, 0) AS dw_load_id,
    CURRENT_TIMESTAMP() AS dw_insert_date,
    CURRENT_TIMESTAMP() AS dw_update_date
  FROM (
    SELECT
      *
    FROM silver_bec_ods.ar_adjustments_all
    WHERE
      is_deleted_flg <> 'Y'
  ) AS AAA
  INNER JOIN (
    SELECT
      *
    FROM silver_bec_ods.AR_DISTRIBUTIONS_ALL
    WHERE
      is_deleted_flg <> 'Y'
  ) AS ADAL
    ON ADAL.SOURCE_ID = AAA.ADJUSTMENT_ID
  INNER JOIN (
    SELECT
      *
    FROM silver_bec_ods.RA_CUSTOMER_TRX_ALL
    WHERE
      is_deleted_flg <> 'Y'
  ) AS RCTA
    ON RCTA.CUSTOMER_TRX_ID = AAA.CUSTOMER_TRX_ID
  INNER JOIN (
    SELECT
      *
    FROM silver_bec_ods.AR_PAYMENT_SCHEDULES_ALL
    WHERE
      is_deleted_flg <> 'Y'
  ) AS APSA
    ON APSA.PAYMENT_SCHEDULE_ID = AAA.PAYMENT_SCHEDULE_ID
  LEFT OUTER JOIN (
    SELECT
      *
    FROM silver_bec_ods.GL_DAILY_RATES
    WHERE
      conversion_type = 'Corporate' AND to_currency = 'USD' AND is_deleted_flg <> 'Y'
  ) AS DCR
    ON DCR.from_currency = RCTA.invoice_currency_code AND DCR.conversion_date = AAA.GL_DATE
  WHERE
    ADAL.SOURCE_TABLE = 'ADJ'
    AND ADAL.SOURCE_TYPE = 'REC'
    AND AAA.STATUS = 'A'
    AND (
      AAA.kca_seq_date > (
        SELECT
          (
            executebegints - prune_days
          )
        FROM bec_etl_ctrl.batch_dw_info
        WHERE
          dw_table_name = 'fact_ar_adjustments' AND batch_name = 'ar'
      )
    )
);
UPDATE bec_etl_ctrl.batch_dw_info SET last_refresh_date = CURRENT_TIMESTAMP()
WHERE
  dw_table_name = 'fact_ar_adjustments' AND batch_name = 'ar';