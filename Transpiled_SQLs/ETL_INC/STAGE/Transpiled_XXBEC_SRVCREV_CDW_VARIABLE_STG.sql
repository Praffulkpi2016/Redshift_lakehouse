TRUNCATE table bronze_bec_ods_stg.XXBEC_SRVCREV_CDW_VARIABLE_STG;
INSERT INTO bronze_bec_ods_stg.XXBEC_SRVCREV_CDW_VARIABLE_STG (
  `source`,
  contract_group,
  contract_id,
  customer_trx_line_id,
  gl_dist_id,
  event_id,
  actual_forecast,
  ledger_id,
  ledger_name,
  org_id,
  org_name,
  site_id,
  revenue_type,
  transaction_currency,
  accounted_currency,
  period_name,
  revenue_amount_trans_curr,
  revenue_amount_acctd_curr,
  conversion_rate,
  tolling_rates_trans_curr,
  tolling_rates_acctd_curr,
  tmo_prcntge,
  last_update_date,
  extract_date,
  from_date,
  to_date,
  period_end_date,
  no_days,
  query_name,
  kwh,
  kwh_type,
  kca_operation,
  kca_seq_id,
  KCA_SEQ_DATE
)
(
  SELECT
    `source`,
    contract_group,
    contract_id,
    customer_trx_line_id,
    gl_dist_id,
    event_id,
    actual_forecast,
    ledger_id,
    ledger_name,
    org_id,
    org_name,
    site_id,
    revenue_type,
    transaction_currency,
    accounted_currency,
    period_name,
    revenue_amount_trans_curr,
    revenue_amount_acctd_curr,
    conversion_rate,
    tolling_rates_trans_curr,
    tolling_rates_acctd_curr,
    tmo_prcntge,
    last_update_date,
    extract_date,
    from_date,
    to_date,
    period_end_date,
    no_days,
    query_name,
    kwh,
    kwh_type,
    kca_operation,
    kca_seq_id,
    KCA_SEQ_DATE
  FROM bec_raw_dl_ext.XXBEC_SRVCREV_CDW_VARIABLE_STG
);