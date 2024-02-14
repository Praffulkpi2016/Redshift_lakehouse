/* Delete Records */
DELETE FROM gold_bec_dwh.FACT_REVENUE_AMORTIZATION
WHERE
  (CAST(COALESCE(site_id, '0') AS STRING), COALESCE(ledger_id, 0), COALESCE(CONTRACT_ID, 0), COALESCE(customer_trx_line_id, 0), COALESCE(ACTUAL_FORECAST, '0'), COALESCE(REVENUE_TYPE, '0'), COALESCE(period_name, '0'), COALESCE(start_date, '1900-01-01 12:00:00'), COALESCE(end_date, '1900-01-01 12:00:00')) IN (
    SELECT
      CAST(COALESCE(ODS.site_id, '0') AS STRING) AS site_id,
      COALESCE(ODS.ledger_id, 0) AS ledger_id,
      COALESCE(ODS.contract_id, 0) AS contract_id,
      COALESCE(ODS.customer_trx_line_id, 0) AS customer_trx_line_id,
      COALESCE(ODS.ACTUAL_FORECAST, '0') AS ACTUAL_FORECAST,
      COALESCE(ODS.REVENUE_TYPE, '0') AS REVENUE_TYPE,
      COALESCE(ODS.period_name, '0') AS period_name,
      COALESCE(ODS.start_date, '1900-01-01 12:00:00') AS start_date,
      COALESCE(ODS.end_date, '1900-01-01 12:00:00') AS end_date
    FROM gold_bec_dwh.FACT_REVENUE_AMORTIZATION AS dw, (
      SELECT
        master.ledger_id,
        master.site_id,
        serv_rev.customer_trx_line_id,
        serv_rev.ACTUAL_FORECAST,
        serv_rev.CONTRACT_ID,
        serv_rev.PERIOD_NAME,
        serv_rev.REVENUE_TYPE,
        master.start_date,
        master.end_date
      FROM (
        SELECT DISTINCT
          source,
          contract_group,
          ledger_name,
          ledger_id,
          org_name,
          org_id,
          site_id,
          contract_id,
          start_date,
          end_date,
          last_update_date,
          kca_seq_date,
          IS_DELETED_FLG
        FROM silver_bec_ods.xxbec_srvcrev_cdw_master_stg
      ) AS master, (
        SELECT DISTINCT
          accounted_currency,
          actual_forecast,
          contract_group,
          contract_id,
          conversion_rate, /* contract_line_id, */
          customer_trx_line_id,
          event_id,
          extract_date,
          from_date,
          gl_dist_id,
          last_update_date,
          ledger_id,
          ledger_name,
          no_days,
          org_id,
          org_name,
          period_end_date,
          period_name,
          query_name,
          revenue_amount_acctd_curr,
          revenue_amount_trans_curr,
          revenue_type,
          site_id,
          `source`,
          to_date,
          transaction_currency
        FROM (
          SELECT DISTINCT
            accounted_currency,
            actual_forecast,
            contract_group,
            contract_id,
            conversion_rate, /* contract_line_id, */
            customer_trx_line_id,
            event_id,
            extract_date,
            from_date,
            gl_dist_id,
            last_update_date,
            ledger_id,
            ledger_name,
            no_days,
            org_id,
            org_name,
            period_end_date,
            period_name,
            query_name,
            revenue_amount_acctd_curr,
            revenue_amount_trans_curr,
            revenue_type,
            site_id,
            `source`,
            to_date,
            transaction_currency
          FROM silver_bec_ods.xxbec_srvcrev_cdw_fixed_stg AS xf
          UNION ALL
          SELECT DISTINCT
            accounted_currency,
            actual_forecast,
            contract_group,
            contract_id,
            conversion_rate, /* contract_line_id, */
            customer_trx_line_id,
            event_id,
            extract_date,
            from_date,
            gl_dist_id,
            last_update_date,
            ledger_id,
            ledger_name,
            no_days,
            org_id,
            org_name,
            period_end_date,
            period_name,
            query_name,
            revenue_amount_acctd_curr,
            revenue_amount_trans_curr,
            revenue_type,
            site_id,
            `source`,
            to_date,
            transaction_currency
          FROM silver_bec_ods.xxbec_srvcrev_cdw_variable_stg AS xv
        )
      ) AS serv_rev
      WHERE
        master.ledger_id = serv_rev.ledger_id
        AND master.site_id = serv_rev.site_id
        AND master.contract_id = serv_rev.contract_id
        AND (
          master.kca_seq_date > (
            SELECT
              (
                executebegints - prune_days
              )
            FROM bec_etl_ctrl.batch_dw_info
            WHERE
              dw_table_name = 'fact_revenue_amortization' AND batch_name = 'ar'
          )
          OR master.is_deleted_flg = 'Y'
        )
      GROUP BY
        master.ledger_id,
        master.site_id,
        serv_rev.customer_trx_line_id,
        serv_rev.ACTUAL_FORECAST,
        serv_rev.CONTRACT_ID,
        serv_rev.PERIOD_NAME,
        serv_rev.REVENUE_TYPE,
        master.start_date,
        master.end_date
      ORDER BY
        master.Ledger_id NULLS LAST,
        master.SITE_ID NULLS LAST
    ) AS ODS
    WHERE
      dw.dw_load_id = (
        SELECT
          system_id
        FROM bec_etl_ctrl.etlsourceappid
        WHERE
          source_system = 'EBS'
      ) || '-' || CAST(COALESCE(ODS.site_id, '0') AS STRING) || '-' || COALESCE(ODS.ledger_id, 0) || '-' || COALESCE(ODS.contract_id, 0) || '-' || COALESCE(ODS.customer_trx_line_id, 0) || '-' || COALESCE(ODS.ACTUAL_FORECAST, '0') || '-' || COALESCE(ODS.REVENUE_TYPE, '0') || '-' || COALESCE(ODS.period_name, '0') || '-' || COALESCE(ODS.start_date, '1900-01-01 12:00:00') || '-' || COALESCE(ODS.end_date, '1900-01-01 12:00:00')
  );
/* Insert records */
INSERT INTO gold_bec_dwh.FACT_REVENUE_AMORTIZATION (
  `source`,
  contract_group,
  ledger_name,
  ledger_id,
  operating_unit,
  org_id,
  site_id,
  start_date,
  end_date,
  customer_trx_line_id,
  actual_forecast,
  contract_id,
  conversion_rate,
  period_end_date,
  no_days,
  from_date,
  to_date,
  period_name,
  revenue_amount_trans_curr,
  revenue_amount_acctd_curr,
  revenue_type,
  last_update_date,
  is_deleted_flg,
  source_app_id,
  dw_load_id,
  dw_insert_date,
  dw_update_date
)
(
  SELECT
    master.source,
    master.contract_group,
    master.ledger_name,
    master.ledger_id,
    master.org_name AS Operating_Unit,
    master.org_id,
    master.site_id,
    master.start_date,
    master.end_date,
    serv_rev.customer_trx_line_id,
    serv_rev.ACTUAL_FORECAST,
    serv_rev.CONTRACT_ID,
    serv_rev.CONVERSION_RATE,
    serv_rev.PERIOD_END_DATE,
    SUM(serv_rev.NO_DAYS) AS NO_DAYS, /*	serv_rev.NO_DAYS, */
    MIN(serv_rev.FROM_DATE) AS FROM_DATE,
    MAX(serv_rev.TO_DATE) AS TO_DATE,
    serv_rev.PERIOD_NAME,
    CAST(SUM(serv_rev.revenue_amount_acctd_curr) AS DECIMAL(32, 2)) AS REVENUE_AMOUNT_ACCTD_CURR, /* serv_rev.REVENUE_AMOUNT_ACCTD_CURR, */
    CAST(SUM(serv_rev.revenue_amount_trans_curr) AS DECIMAL(32, 2)) AS REVENUE_AMOUNT_TRANS_CURR, /* serv_rev.REVENUE_AMOUNT_TRANS_CURR, */
    serv_rev.REVENUE_TYPE,
    master.last_update_date,
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
    ) || '-' || CAST(COALESCE(master.site_id, '0') AS STRING) || '-' || COALESCE(master.ledger_id, 0) || '-' || COALESCE(serv_rev.CONTRACT_ID, 0) || '-' || COALESCE(serv_rev.customer_trx_line_id, 0) || '-' || COALESCE(serv_rev.ACTUAL_FORECAST, '0') || '-' || COALESCE(serv_rev.REVENUE_TYPE, '0') || '-' || COALESCE(serv_rev.period_name, '0') || '-' || COALESCE(master.start_date, '1900-01-01 12:00:00') || '-' || COALESCE(master.end_date, '1900-01-01 12:00:00') AS dw_load_id,
    CURRENT_TIMESTAMP() AS dw_insert_date,
    CURRENT_TIMESTAMP() AS dw_update_date
  FROM (
    SELECT DISTINCT
      source,
      contract_group,
      ledger_name,
      ledger_id,
      org_name,
      org_id,
      site_id,
      contract_id,
      start_date,
      end_date,
      last_update_date,
      kca_seq_date
    FROM silver_bec_ods.xxbec_srvcrev_cdw_master_stg
    WHERE
      is_deleted_flg <> 'Y'
  ) AS master, (
    SELECT DISTINCT
      accounted_currency,
      actual_forecast,
      contract_group,
      contract_id,
      conversion_rate, /* contract_line_id, */
      customer_trx_line_id,
      event_id,
      extract_date,
      from_date,
      gl_dist_id,
      last_update_date,
      ledger_id,
      ledger_name,
      no_days,
      org_id,
      org_name,
      period_end_date,
      period_name,
      query_name,
      revenue_amount_acctd_curr,
      revenue_amount_trans_curr,
      revenue_type,
      site_id,
      `source`,
      to_date,
      transaction_currency
    FROM (
      SELECT DISTINCT
        accounted_currency,
        actual_forecast,
        contract_group,
        contract_id,
        conversion_rate, /* contract_line_id, */
        customer_trx_line_id,
        event_id,
        extract_date,
        from_date,
        gl_dist_id,
        last_update_date,
        ledger_id,
        ledger_name,
        no_days,
        org_id,
        org_name,
        period_end_date,
        period_name,
        query_name,
        revenue_amount_acctd_curr,
        revenue_amount_trans_curr,
        revenue_type,
        site_id,
        `source`,
        to_date,
        transaction_currency
      FROM (
        SELECT
          *
        FROM silver_bec_ods.xxbec_srvcrev_cdw_fixed_stg
        WHERE
          is_deleted_flg <> 'Y'
      ) AS xf
      UNION ALL
      SELECT DISTINCT
        accounted_currency,
        actual_forecast,
        contract_group,
        contract_id,
        conversion_rate, /* contract_line_id, */
        customer_trx_line_id,
        event_id,
        extract_date,
        from_date,
        gl_dist_id,
        last_update_date,
        ledger_id,
        ledger_name,
        no_days,
        org_id,
        org_name,
        period_end_date,
        period_name,
        query_name,
        revenue_amount_acctd_curr,
        revenue_amount_trans_curr,
        revenue_type,
        site_id,
        `source`,
        to_date,
        transaction_currency
      FROM (
        SELECT
          *
        FROM silver_bec_ods.xxbec_srvcrev_cdw_variable_stg
        WHERE
          is_deleted_flg <> 'Y'
      ) AS xv
    )
  ) AS serv_rev
  WHERE
    master.ledger_id = serv_rev.ledger_id
    AND master.site_id = serv_rev.site_id
    AND master.contract_id = serv_rev.contract_id /* added to handle duplicates */
    AND (
      master.kca_seq_date > (
        SELECT
          (
            executebegints - prune_days
          )
        FROM bec_etl_ctrl.batch_dw_info
        WHERE
          dw_table_name = 'fact_revenue_amortization' AND batch_name = 'ar'
      )
    )
  GROUP BY
    master.source,
    master.contract_group,
    master.ledger_name,
    master.ledger_id,
    master.org_name,
    master.org_id,
    master.site_id,
    master.start_date,
    master.end_date,
    serv_rev.customer_trx_line_id,
    serv_rev.ACTUAL_FORECAST,
    serv_rev.CONTRACT_ID,
    serv_rev.CONVERSION_RATE,
    serv_rev.PERIOD_END_DATE,
    serv_rev.PERIOD_NAME,
    serv_rev.REVENUE_TYPE,
    master.last_update_date
  ORDER BY
    master.source NULLS LAST,
    master.CONTRACT_GROUP NULLS LAST,
    master.Ledger_id NULLS LAST,
    master.SITE_ID NULLS LAST
);
UPDATE bec_etl_ctrl.batch_dw_info SET load_type = 'I', last_refresh_date = CURRENT_TIMESTAMP()
WHERE
  dw_table_name = 'fact_revenue_amortization' AND batch_name = 'ar';