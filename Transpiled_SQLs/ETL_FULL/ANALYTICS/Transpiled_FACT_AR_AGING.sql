DROP table IF EXISTS gold_bec_dwh.FACT_AR_AGING;
CREATE TABLE gold_bec_dwh.FACT_AR_AGING AS
(
  SELECT
    customer_id,
    customer_site_use_id,
    (
      SELECT
        SYSTEM_ID
      FROM BEC_ETL_CTRL.ETLSOURCEAPPID
      WHERE
        SOURCE_SYSTEM = 'EBS'
    ) || '-' || CUSTOMER_SITE_USE_ID AS CUSTOMER_SITE_USE_ID_KEY,
    customer_trx_id,
    payment_schedule_id,
    class,
    primary_salesrep_id,
    batch_source_id,
    due_date,
    acct_amount_due_remaining,
    trns_amount_due_remaining,
    trx_number,
    amount_adjusted,
    amount_applied,
    AMOUNT_DUE_ORIGINAL,
    amount_credited,
    amount_adjusted_pending,
    gl_date,
    gl_date_closed,
    cust_trx_type_id,
    (
      SELECT
        SYSTEM_ID
      FROM BEC_ETL_CTRL.ETLSOURCEAPPID
      WHERE
        SOURCE_SYSTEM = 'EBS'
    ) || '-' || CUST_TRX_TYPE_ID AS CUST_TRX_TYPE_ID_KEY,
    org_id,
    (
      SELECT
        SYSTEM_ID
      FROM BEC_ETL_CTRL.ETLSOURCEAPPID
      WHERE
        SOURCE_SYSTEM = 'EBS'
    ) || '-' || ORG_ID AS ORG_ID_KEY,
    invoice_currency_code,
    source_name,
    exchange_rate,
    GBL_TOTAL_AMOUNT,
    GBL_AMOUNT_DUE_REMAINING,
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
    ) || '-' || COALESCE(customer_trx_id, 0) || '-' || COALESCE(payment_schedule_id, 0) || '-' || COALESCE(customer_site_use_id, 0) || '-' || COALESCE(source_name, 'NA') AS dw_load_id,
    CURRENT_TIMESTAMP() AS dw_insert_date,
    CURRENT_TIMESTAMP() AS dw_update_date
  FROM (
    SELECT
      a.customer_id,
      a.customer_site_use_id,
      a.customer_trx_id,
      a.payment_schedule_id,
      a.class,
      SUM(a.primary_salesrep_id) AS primary_salesrep_id,
      a.batch_source_id,
      a.due_date,
      SUM(a.acct_amount_due_remaining) AS acct_amount_due_remaining,
      SUM(a.trns_amount_due_remaining) AS trns_amount_due_remaining,
      a.trx_number,
      a.amount_adjusted,
      a.amount_applied,
      a.AMOUNT_DUE_ORIGINAL,
      a.amount_credited,
      a.amount_adjusted_pending,
      a.gl_date,
      a.gl_date_closed,
      a.cust_trx_type_id,
      a.org_id,
      a.invoice_currency_code,
      a.source_name,
      a.exchange_rate,
      (
        a.GBL_TOTAL_AMOUNT
      ) AS GBL_TOTAL_AMOUNT,
      SUM(a.GBL_AMOUNT_DUE_REMAINING) AS GBL_AMOUNT_DUE_REMAINING
    FROM (
      SELECT
        ps.customer_id,
        ps.customer_site_use_id,
        ps.customer_trx_id,
        ps.payment_schedule_id,
        ps.class,
        0 AS primary_salesrep_id,
        ct.batch_source_id,
        ps.due_date,
        SUM(COALESCE(adj.acctd_amount, 0)) AS acct_amount_due_remaining,
        SUM(COALESCE(adj.amount, 0)) AS trns_amount_due_remaining,
        ps.trx_number,
        ps.amount_adjusted,
        ps.amount_applied,
        ps.AMOUNT_DUE_ORIGINAL,
        ps.amount_credited,
        ps.amount_adjusted_pending,
        ps.gl_date,
        ps.gl_date_closed,
        ps.cust_trx_type_id,
        ps.org_id,
        ps.invoice_currency_code,
        ra_batch_sources_all.name AS source_name,
        COALESCE(ps.exchange_rate, 1) AS exchange_rate,
        CAST(COALESCE(ps.AMOUNT_DUE_ORIGINAL, 0) * COALESCE(DCR.conversion_rate, 1) AS DECIMAL(18, 2)) AS GBL_TOTAL_AMOUNT,
        CAST(COALESCE(adj.amount, 0) * COALESCE(DCR.conversion_rate, 1) AS DECIMAL(18, 2)) AS GBL_AMOUNT_DUE_REMAINING
      FROM (
        SELECT
          *
        FROM silver_bec_ods.ar_payment_schedules_all
        WHERE
          is_deleted_flg <> 'Y'
      ) AS ps, (
        SELECT
          *
        FROM silver_bec_ods.ar_adjustments_all
        WHERE
          is_deleted_flg <> 'Y'
      ) AS adj, (
        SELECT
          *
        FROM silver_bec_ods.ra_customer_trx_all
        WHERE
          is_deleted_flg <> 'Y'
      ) AS ct, (
        SELECT
          *
        FROM silver_bec_ods.ra_batch_sources_all
        WHERE
          is_deleted_flg <> 'Y'
      ) AS ra_batch_sources_all, (
        SELECT
          *
        FROM (
          SELECT
            *
          FROM silver_bec_ods.GL_DAILY_RATES
          WHERE
            is_deleted_flg <> 'Y'
        )
        WHERE
          to_currency = 'USD' AND conversion_type = 'Corporate'
      ) AS DCR
      WHERE
        1 = 1
        AND adj.payment_schedule_id = ps.payment_schedule_id
        AND adj.status = 'A'
        AND ps.customer_id > 0
        AND ps.customer_trx_id = ct.CUSTOMER_TRX_ID()
        AND PS.invoice_currency_code = DCR.FROM_CURRENCY()
        AND DCR.CONVERSION_DATE() = ps.gl_date
        AND ct.batch_source_id = ra_batch_sources_all.BATCH_SOURCE_ID()
        AND ct.org_id = ra_batch_sources_all.ORG_ID()
      GROUP BY
        ps.customer_id,
        ps.customer_site_use_id,
        ps.customer_trx_id,
        ps.class,
        ct.batch_source_id,
        ps.due_date,
        ps.trx_number,
        ps.amount_adjusted,
        ps.amount_applied,
        ps.AMOUNT_DUE_ORIGINAL,
        ps.amount_credited,
        ps.amount_adjusted_pending,
        ps.gl_date,
        ps.gl_date_closed,
        ps.cust_trx_type_id,
        ps.org_id,
        ps.invoice_currency_code,
        ra_batch_sources_all.name,
        COALESCE(ps.exchange_rate, 1),
        ps.payment_schedule_id,
        CAST(COALESCE(ps.AMOUNT_DUE_ORIGINAL, 0) * COALESCE(DCR.conversion_rate, 1) AS DECIMAL(18, 2)),
        CAST(COALESCE(adj.amount, 0) * COALESCE(DCR.conversion_rate, 1) AS DECIMAL(18, 2))
      UNION ALL
      SELECT
        ps.customer_id,
        ps.customer_site_use_id,
        ps.customer_trx_id,
        ps.payment_schedule_id,
        ps.class,
        0 AS primary_salesrep_id,
        ct.batch_source_id,
        ps.due_date,
        COALESCE(
          SUM(
            (
              CASE
                WHEN ps.class = 'CM'
                THEN CASE
                  WHEN app.application_type = 'CM'
                  THEN app.acctd_amount_applied_from
                  ELSE app.acctd_amount_applied_to
                END
                ELSE app.acctd_amount_applied_to
              END + COALESCE(app.acctd_earned_discount_taken, 0) + COALESCE(app.acctd_unearned_discount_taken, 0)
            ) * CASE
              WHEN ps.class = 'CM'
              THEN CASE WHEN app.application_type = 'CM' THEN -1 ELSE 1 END
              ELSE 1
            END
          ),
          0
        ) AS acct_amount_due_remaining,
        COALESCE(
          SUM(
            (
              app.amount_applied + COALESCE(app.earned_discount_taken, 0) + COALESCE(app.unearned_discount_taken, 0)
            ) * CASE
              WHEN ps.class = 'CM'
              THEN CASE WHEN app.application_type = 'CM' THEN -1 ELSE 1 END
              ELSE 1
            END
          ),
          0
        ) AS trns_amount_due_remaining,
        ps.trx_number,
        ps.amount_adjusted,
        ps.amount_applied,
        ps.AMOUNT_DUE_ORIGINAL,
        ps.amount_credited,
        ps.amount_adjusted_pending,
        ps.gl_date AS gl_date_inv,
        ps.gl_date_closed,
        ps.cust_trx_type_id,
        ps.org_id,
        ps.invoice_currency_code,
        ra_batch_sources_all.name AS source_name,
        COALESCE(ps.exchange_rate, 1) AS exchange_rate,
        CAST(COALESCE(ps.AMOUNT_DUE_ORIGINAL, 0) * COALESCE(DCR.conversion_rate, 1) AS DECIMAL(18, 2)) AS GBL_TOTAL_AMOUNT,
        CAST(COALESCE(
          SUM(
            (
              app.amount_applied + COALESCE(app.earned_discount_taken, 0) + COALESCE(app.unearned_discount_taken, 0)
            ) * CASE
              WHEN ps.class = 'CM'
              THEN CASE WHEN app.application_type = 'CM' THEN -1 ELSE 1 END
              ELSE 1
            END
          ),
          0
        ) * COALESCE(DCR.conversion_rate, 1) AS DECIMAL(18, 2)) AS GBL_AMOUNT_DUE_REMAINING
      FROM (
        SELECT
          *
        FROM silver_bec_ods.ar_payment_schedules_all
        WHERE
          is_deleted_flg <> 'Y'
      ) AS ps, (
        SELECT
          *
        FROM silver_bec_ods.ar_receivable_applications_all
        WHERE
          is_deleted_flg <> 'Y'
      ) AS app, (
        SELECT
          *
        FROM silver_bec_ods.ra_customer_trx_all
        WHERE
          is_deleted_flg <> 'Y'
      ) AS ct, (
        SELECT
          *
        FROM silver_bec_ods.ra_batch_sources_all
        WHERE
          is_deleted_flg <> 'Y'
      ) AS ra_batch_sources_all, (
        SELECT
          *
        FROM (
          SELECT
            *
          FROM silver_bec_ods.GL_DAILY_RATES
          WHERE
            is_deleted_flg <> 'Y'
        )
        WHERE
          to_currency = 'USD' AND conversion_type = 'Corporate'
      ) AS DCR
      WHERE
        1 = 1
        AND ps.customer_id > 0
        AND (
          app.applied_payment_schedule_id = ps.payment_schedule_id
          OR app.payment_schedule_id = ps.payment_schedule_id
        )
        AND app.status = 'APP'
        AND COALESCE(app.confirmed_flag, 'Y') = 'Y'
        AND ps.customer_trx_id = ct.CUSTOMER_TRX_ID()
        AND PS.invoice_currency_code = DCR.FROM_CURRENCY()
        AND DCR.CONVERSION_DATE() = ps.gl_date
        AND ct.batch_source_id = ra_batch_sources_all.BATCH_SOURCE_ID()
        AND ct.org_id = ra_batch_sources_all.ORG_ID()
      GROUP BY
        ps.customer_id,
        ps.customer_site_use_id,
        ps.customer_trx_id,
        ps.class,
        ct.batch_source_id,
        ps.due_date,
        ps.trx_number,
        ps.amount_adjusted,
        ps.amount_applied,
        ps.AMOUNT_DUE_ORIGINAL,
        ps.amount_credited,
        ps.amount_adjusted_pending,
        ps.gl_date,
        ps.gl_date_closed,
        ps.cust_trx_type_id,
        ps.org_id,
        ps.invoice_currency_code,
        ra_batch_sources_all.name,
        COALESCE(ps.exchange_rate, 1),
        ps.payment_schedule_id,
        dcr.conversion_rate
      UNION ALL
      SELECT
        ps.customer_id,
        ps.customer_site_use_id,
        ps.customer_trx_id,
        ps.payment_schedule_id,
        ps.class AS class_inv,
        COALESCE(ct.primary_salesrep_id, 0) AS primary_salesrep_id,
        ct.batch_source_id,
        ps.due_date AS due_date_inv,
        ps.acctd_amount_due_remaining AS acct_amount_due_remaining,
        ps.amount_due_remaining AS trns_amount_due_remaining,
        ps.trx_number,
        ps.amount_adjusted,
        ps.amount_applied,
        ps.AMOUNT_DUE_ORIGINAL,
        ps.amount_credited,
        ps.amount_adjusted_pending,
        ps.gl_date,
        ps.gl_date_closed,
        ps.cust_trx_type_id,
        ps.org_id,
        ps.invoice_currency_code,
        ra_batch_sources_all.name AS source_name,
        COALESCE(ps.exchange_rate, 1) AS exchange_rate,
        CAST(COALESCE(ps.AMOUNT_DUE_ORIGINAL, 0) * COALESCE(DCR.conversion_rate, 1) AS DECIMAL(18, 2)) AS GBL_TOTAL_AMOUNT,
        CAST(COALESCE(ps.amount_due_remaining, 0) * COALESCE(DCR.conversion_rate, 1) AS DECIMAL(18, 2)) AS GBL_AMOUNT_DUE_REMAINING
      FROM (
        SELECT
          *
        FROM silver_bec_ods.ar_payment_schedules_all
        WHERE
          is_deleted_flg <> 'Y'
      ) AS ps, (
        SELECT
          *
        FROM silver_bec_ods.ra_customer_trx_all
        WHERE
          is_deleted_flg <> 'Y'
      ) AS ct, (
        SELECT
          *
        FROM silver_bec_ods.ra_batch_sources_all
        WHERE
          is_deleted_flg <> 'Y'
      ) AS ra_batch_sources_all, (
        SELECT
          *
        FROM silver_bec_ods.GL_DAILY_RATES
        WHERE
          to_currency = 'USD' AND conversion_type = 'Corporate' AND is_deleted_flg <> 'Y'
      ) AS DCR
      WHERE
        1 = 1
        AND ps.customer_id > 0
        AND ps.customer_trx_id = ct.CUSTOMER_TRX_ID()
        AND PS.invoice_currency_code = DCR.FROM_CURRENCY()
        AND DCR.CONVERSION_DATE() = ps.gl_date
        AND ct.batch_source_id = ra_batch_sources_all.BATCH_SOURCE_ID()
        AND ct.org_id = ra_batch_sources_all.ORG_ID()
      UNION ALL
      SELECT
        ps.customer_id,
        ps.customer_site_use_id,
        ps.customer_trx_id,
        ps.payment_schedule_id,
        ps.class AS class_inv,
        ct.primary_salesrep_id,
        ct.batch_source_id,
        ps.due_date AS due_date_inv,
        ps.acctd_amount_due_remaining AS acct_amount_due_remaining,
        ps.amount_due_remaining AS trns_amount_due_remaining,
        ps.trx_number,
        ps.amount_adjusted,
        ps.amount_applied,
        ps.AMOUNT_DUE_ORIGINAL,
        ps.amount_credited,
        ps.amount_adjusted_pending,
        ps.gl_date,
        ps.gl_date_closed,
        ps.cust_trx_type_id,
        ps.org_id,
        ps.invoice_currency_code,
        ra_batch_sources_all.name AS source_name,
        COALESCE(ps.exchange_rate, 1) AS exchange_rate,
        CAST(COALESCE(ps.AMOUNT_DUE_ORIGINAL, 0) * COALESCE(DCR.conversion_rate, 1) AS DECIMAL(18, 2)) AS GBL_TOTAL_AMOUNT,
        CAST(COALESCE(ps.amount_due_remaining, 0) * COALESCE(DCR.conversion_rate, 1) AS DECIMAL(18, 2)) AS GBL_AMOUNT_DUE_REMAINING
      FROM (
        SELECT
          *
        FROM silver_bec_ods.ar_payment_schedules_all
        WHERE
          is_deleted_flg <> 'Y'
      ) AS ps, (
        SELECT
          *
        FROM silver_bec_ods.ra_customer_trx_all
        WHERE
          is_deleted_flg <> 'Y'
      ) AS ct, (
        SELECT
          *
        FROM silver_bec_ods.ar_adjustments_all
        WHERE
          is_deleted_flg <> 'Y'
      ) AS adj, (
        SELECT
          *
        FROM silver_bec_ods.ra_batch_sources_all
        WHERE
          is_deleted_flg <> 'Y'
      ) AS ra_batch_sources_all, (
        SELECT
          *
        FROM (
          SELECT
            *
          FROM silver_bec_ods.GL_DAILY_RATES
          WHERE
            is_deleted_flg <> 'Y'
        )
        WHERE
          to_currency = 'USD' AND conversion_type = 'Corporate'
      ) AS DCR
      WHERE
        1 = 1
        AND ps.customer_id > 0
        AND ps.class = 'CB'
        AND ps.customer_trx_id = adj.chargeback_customer_trx_id
        AND ps.customer_trx_id = ct.CUSTOMER_TRX_ID()
        AND PS.invoice_currency_code = DCR.FROM_CURRENCY()
        AND DCR.CONVERSION_DATE() = ps.gl_date
        AND ct.batch_source_id = ra_batch_sources_all.BATCH_SOURCE_ID()
        AND ct.org_id = ra_batch_sources_all.ORG_ID()
    ) AS a
    GROUP BY
      a.customer_id,
      a.customer_site_use_id,
      a.customer_trx_id,
      a.payment_schedule_id,
      a.class,
      a.batch_source_id,
      a.due_date,
      a.trx_number,
      a.amount_adjusted,
      a.amount_applied,
      a.AMOUNT_DUE_ORIGINAL,
      a.amount_credited,
      a.amount_adjusted_pending,
      a.gl_date,
      a.gl_date_closed,
      a.cust_trx_type_id,
      a.org_id,
      a.invoice_currency_code,
      a.source_name,
      a.exchange_rate,
      a.GBL_TOTAL_AMOUNT
  )
);
UPDATE bec_etl_ctrl.batch_dw_info SET load_type = 'I', last_refresh_date = CURRENT_TIMESTAMP()
WHERE
  dw_table_name = 'fact_ar_aging' AND batch_name = 'ar';