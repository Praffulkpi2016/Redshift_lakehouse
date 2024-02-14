/* Temp1 */
DROP table IF EXISTS gold_bec_dwh.cte_xla_ae_headers;
CREATE TABLE gold_bec_dwh.CTE_xla_ae_headers AS
(
  SELECT
    aeh.accounting_date AS GL_DATE,
    aeh.creation_date AS CREATION_DATE,
    aeh.last_update_date AS LAST_UPDATE_DATE,
    aeh.gl_transfer_date AS GL_TRANSFER_DATE,
    aeh.reference_date AS REFERENCE_DATE,
    aeh.completed_date AS COMPLETED_DATE,
    aeh.completion_acct_seq_value AS ACCOUNTING_SEQUENCE_NUMBER,
    aeh.close_acct_seq_value AS REPORTING_SEQUENCE_NUMBER,
    aeh.doc_sequence_value AS DOCUMENT_SEQUENCE_NUMBER,
    aeh.application_id AS APPLICATION_ID,
    aeh.ae_header_id AS HEADER_ID,
    aeh.description AS HEADER_DESCRIPTION,
    aeh.funds_status_code AS FUND_STATUS,
    aeh.ae_header_id,
    aeh.je_category_name,
    aeh.Event_type_code AS Event_TYPE_CODE,
    aeh.accounting_entry_status_code,
    aeh.gl_transfer_status_code,
    aeh.balance_type_code,
    aeh.budget_version_id,
    aeh.Event_id,
    aeh.entity_id,
    aeh.completion_acct_seq_version_id,
    aeh.close_acct_seq_version_id,
    aeh.doc_sequence_id
  FROM (
    SELECT
      *
    FROM silver_bec_ods.xla_ae_headers
    WHERE
      is_deleted_flg <> 'Y'
      AND accounting_entry_status_code = 'F'
      AND gl_transfer_status_code = 'Y'
      AND last_update_date >= TO_DATE('2020-01-01 12:00:00', 'YYYY-MM-DD HH:MI:SS')
  ) AS aeh
);
/* Temp2 */
DROP table IF EXISTS gold_bec_dwh.cte_xla_ae_lines;
CREATE TABLE gold_bec_dwh.cte_xla_ae_lines AS
(
  SELECT
    ael.gl_sl_link_table,
    ael.application_id,
    ael.ae_header_id,
    ael.gl_sl_link_id,
    ael.encumbrance_type_id,
    ael.accounting_class_code,
    ael.currency_conversion_type,
    ael.ae_header_id AS ael_ae_header_id,
    ael.ae_line_num,
    ael.displayed_line_number AS LINE_NUMBER,
    ael.ae_line_num AS ORIG_LINE_NUMBER,
    ael.description AS LINE_DESCRIPTION,
    ael.currency_code AS ENTERED_CURRENCY,
    ael.currency_conversion_rate AS CONVERSION_RATE,
    ael.currency_conversion_date AS CONVERSION_RATE_DATE,
    ael.currency_conversion_type AS CONVERSION_RATE_TYPE_CODE,
    ael.ENTERED_dr AS ENTERED_DR,
    ael.ENTERED_cr AS ENTERED_CR,
    ael.unrounded_accounted_dr AS UNROUNDED_ACCOUNTED_DR,
    ael.unrounded_accounted_cr AS UNROUNDED_ACCOUNTED_CR,
    ael.statistical_amount AS STATISTICAL_AMOUNT,
    ael.jgzz_recon_ref AS RECONCILIATION_REFERENCE,
    ael.party_type_code AS PARTY_TYPE_CODE
  FROM (
    SELECT
      *
    FROM silver_bec_ods.xla_ae_lines
    WHERE
      is_deleted_flg <> 'Y'
      AND last_update_date >= TO_DATE('2020-01-01 12:00:00', 'YYYY-MM-DD HH:MI:SS')
  ) AS ael
);
DROP table IF EXISTS gold_bec_dwh.cte_xla_event_types_tl;
CREATE TABLE gold_bec_dwh.cte_xla_event_types_tl AS
(
  SELECT
    xet.Event_class_code AS Event_CLASS_CODE,
    xet.NAME AS Event_TYPE_NAME,
    xet.entity_code,
    xet.application_id,
    xet.Event_type_code
  FROM (
    SELECT
      *
    FROM silver_bec_ods.xla_Event_types_tl
    WHERE
      is_deleted_flg <> 'Y' AND LANGUAGE = 'US'
  ) AS xet
);
DROP table IF EXISTS gold_bec_dwh.CTE_xla_Event_classes_tl;
CREATE TABLE gold_bec_dwh.CTE_xla_Event_classes_tl AS
(
  SELECT
    xect.NAME AS Event_CLASS_NAME,
    xect.application_id,
    xect.entity_code,
    xect.Event_class_code
  FROM (
    SELECT
      *
    FROM silver_bec_ods.xla_Event_classes_tl
    WHERE
      is_deleted_flg <> 'Y' AND LANGUAGE = 'US'
  ) AS xect
);
DROP table IF EXISTS gold_bec_dwh.cte_xla_events;
CREATE TABLE gold_bec_dwh.cte_xla_events AS
(
  SELECT
    xle.transaction_date,
    xle.Event_id AS xle_Event_ID,
    xle.Event_date,
    xle.Event_number AS Event_NUMBER,
    xle.application_id,
    xle.Event_id
  FROM (
    SELECT
      *
    FROM silver_bec_ods.xla_Events
    WHERE
      is_deleted_flg <> 'Y'
      AND last_update_date >= TO_DATE('2020-01-01 12:00:00', 'YYYY-MM-DD HH:MI:SS')
  ) AS xle
);
DROP table IF EXISTS gold_bec_dwh.CTE_xla_transaction_entities;
CREATE TABLE gold_bec_dwh.CTE_xla_transaction_entities AS
(
  SELECT
    ent.transaction_number AS TRANSACTION_NUMBER,
    ent.entity_code AS ent_entity_code,
    ent.application_id AS ent_application_id,
    ent.entity_id AS ent_entity_id,
    ent.source_id_int_1
  FROM (
    SELECT
      *
    FROM silver_bec_ods.xla_transaction_entities
    WHERE
      is_deleted_flg <> 'Y'
      AND last_update_date >= TO_DATE('2020-01-01 12:00:00', 'YYYY-MM-DD HH:MI:SS')
  ) AS ent
);
DROP table IF EXISTS gold_bec_dwh.CTE_xla_distribution_links;
CREATE TABLE gold_bec_dwh.CTE_xla_distribution_links AS
(
  SELECT
    xla_dl1.alloc_to_dist_id_num_1,
    xla_dl1.ae_header_id AS xla_dl1_ae_header_id,
    xla_dl1.ae_line_num AS xla_dl1_ae_line_num,
    xla_dl1.source_distribution_id_num_1,
    xla_dl1.UNROUNDED_ACCOUNTED_DR AS ACCOUNTED_DR,
    xla_dl1.UNROUNDED_ACCOUNTED_CR AS ACCOUNTED_CR,
    xla_dl1.TEMP_LINE_NUM
  FROM silver_bec_ods.xla_distribution_links AS xla_dl1
  WHERE
    xla_dl1.is_deleted_flg <> 'Y'
);
DROP table IF EXISTS gold_bec_dwh.CTE_fnd_lookup_values;
CREATE TABLE gold_bec_dwh.CTE_fnd_lookup_values AS
(
  SELECT
    xlk2.lookup_type,
    xlk2.lookup_code,
    xlk2.meaning AS ACCOUNTING_CLASS_NAME
  FROM silver_bec_ods.fnd_lookup_values AS xlk2
  WHERE
    xlk2.lookup_type = 'XLA_ACCOUNTING_CLASS' AND xlk2.is_deleted_flg <> 'Y'
);
DROP table IF EXISTS gold_bec_dwh.CTE_fun_seq_versions1;
CREATE TABLE gold_bec_dwh.CTE_fun_seq_versions1 AS
(
  SELECT
    fsv1.header_name AS ACCOUNTING_SEQUENCE_NAME,
    fsv1.version_name AS ACCOUNTING_SEQUENCE_VERSION,
    fsv1.seq_version_id
  FROM silver_bec_ods.fun_seq_versions AS fsv1
  WHERE
    is_deleted_flg <> 'Y'
);
DROP table IF EXISTS gold_bec_dwh.CTE_OM;
CREATE TABLE gold_bec_dwh.CTE_OM AS
(
  SELECT
    CAST(order_Number AS STRING) AS order_Number,
    bec.address1,
    mmt.transaction_id
  FROM (
    SELECT
      *
    FROM silver_bec_ods.mtl_material_transactions
    WHERE
      is_deleted_flg <> 'Y'
  ) AS mmt, (
    SELECT
      *
    FROM silver_bec_ods.oe_order_headers_all
    WHERE
      is_deleted_flg <> 'Y'
  ) AS oeh, (
    SELECT
      *
    FROM silver_bec_ods.oe_order_lines_all
    WHERE
      is_deleted_flg <> 'Y'
  ) AS oel, (
    SELECT
      *
    FROM silver_bec_ods.bec_customer_details_view
    WHERE
      is_deleted_flg <> 'Y'
  ) AS bec
  WHERE
    1 = 1
    AND mmt.trx_source_line_id = oel.line_id
    AND oeh.header_id = oel.header_id
    AND oel.ship_to_org_id = bec.site_use_id
  LIMIT 1
);
DROP table IF EXISTS gold_bec_dwh.CTE_REC;
CREATE TABLE gold_bec_dwh.CTE_REC AS
(
  SELECT
    hl.address1,
    rctld.cust_trx_line_gl_dist_id
  FROM (
    SELECT
      *
    FROM silver_bec_ods.ra_cust_trx_line_gl_dist_all
    WHERE
      is_deleted_flg <> 'Y'
  ) AS rctld, (
    SELECT
      *
    FROM silver_bec_ods.ra_customer_trx_lines_all
    WHERE
      is_deleted_flg <> 'Y'
  ) AS rctl, (
    SELECT
      *
    FROM silver_bec_ods.ra_customer_trx_all
    WHERE
      is_deleted_flg <> 'Y'
  ) AS trx, (
    SELECT
      *
    FROM silver_bec_ods.hz_cust_site_uses_all
    WHERE
      is_deleted_flg <> 'Y'
  ) AS hcsu, (
    SELECT
      *
    FROM silver_bec_ods.hz_cust_acct_sites_all
    WHERE
      is_deleted_flg <> 'Y'
  ) AS hccs, (
    SELECT
      *
    FROM silver_bec_ods.hz_party_sites
    WHERE
      is_deleted_flg <> 'Y'
  ) AS hps, (
    SELECT
      *
    FROM silver_bec_ods.hz_locations
    WHERE
      is_deleted_flg <> 'Y'
  ) AS hl
  WHERE
    1 = 1
    AND rctld.customer_trx_line_id = rctl.customer_trx_line_id
    AND rctl.customer_trx_id = trx.customer_trx_id
    AND COALESCE(rctl.ship_to_site_use_id, trx.ship_to_site_use_id) = hcsu.site_use_id
    AND hcsu.cust_acct_site_id = hccs.cust_acct_site_id
    AND hccs.party_site_id = hps.party_site_id
    AND hps.location_id = hl.location_id
);
DROP table IF EXISTS gold_bec_dwh.CTE_ADJ;
CREATE TABLE gold_bec_dwh.CTE_ADJ AS
(
  SELECT
    hl.address1,
    adj.adjustment_id
  FROM (
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
  ) AS trx, (
    SELECT
      *
    FROM silver_bec_ods.hz_cust_site_uses_all
    WHERE
      is_deleted_flg <> 'Y'
  ) AS hcsu, (
    SELECT
      *
    FROM silver_bec_ods.hz_cust_acct_sites_all
    WHERE
      is_deleted_flg <> 'Y'
  ) AS hccs, (
    SELECT
      *
    FROM silver_bec_ods.hz_party_sites
    WHERE
      is_deleted_flg <> 'Y'
  ) AS hps, (
    SELECT
      *
    FROM silver_bec_ods.hz_locations
    WHERE
      is_deleted_flg <> 'Y'
  ) AS hl
  WHERE
    1 = 1
    AND adj.customer_trx_id = trx.CUSTOMER_TRX_ID()
    AND trx.ship_to_site_use_id = hcsu.SITE_USE_ID()
    AND hcsu.cust_acct_site_id = hccs.CUST_ACCT_SITE_ID()
    AND hccs.party_site_id = hps.PARTY_SITE_ID()
    AND hps.location_id = hl.LOCATION_ID()
);
DROP table IF EXISTS gold_bec_dwh.FACT_GL_XLA_STG;
CREATE TABLE gold_bec_dwh.FACT_GL_XLA_STG AS
(
  WITH CTE_fun_seq_versions2 AS (
    SELECT
      fsv2.header_name AS REPORTING_SEQUENCE_NAME,
      fsv2.version_name AS REPORTING_SEQUENCE_VERSION,
      fsv2.seq_version_id
    FROM silver_bec_ods.fun_seq_versions AS fsv2
  ), CTE_fnd_document_sequences AS (
    SELECT
      fns.name AS DOCUMENT_SEQUENCE_NAME,
      fns.application_id,
      fns.doc_sequence_id
    FROM silver_bec_ods.fnd_document_sequences AS fns
    WHERE
      is_deleted_flg <> 'Y'
  ), CTE_gl_daily_conversion_types AS (
    SELECT
      gdct.user_conversion_type AS CONVERSION_RATE_TYPE,
      gdct.conversion_type
    FROM silver_bec_ods.gl_daily_conversion_types AS gdct
    WHERE
      gdct.is_deleted_flg <> 'Y'
  )
  SELECT DISTINCT
    aeh.GL_DATE,
    aeh.CREATION_DATE,
    aeh.LAST_UPDATE_DATE,
    aeh.GL_TRANSFER_DATE,
    aeh.REFERENCE_DATE,
    aeh.COMPLETED_DATE,
    aeh.ACCOUNTING_SEQUENCE_NUMBER,
    aeh.REPORTING_SEQUENCE_NUMBER,
    aeh.DOCUMENT_SEQUENCE_NUMBER,
    aeh.APPLICATION_ID,
    aeh.HEADER_ID,
    aeh.HEADER_DESCRIPTION,
    aeh.FUND_STATUS,
    aeh.je_category_name,
    aeh.Event_TYPE_CODE,
    aeh.accounting_entry_status_code,
    aeh.gl_transfer_status_code,
    aeh.balance_type_code,
    aeh.budget_version_id,
    aeh.Event_id,
    aeh.entity_id,
    aeh.completion_acct_seq_version_id,
    aeh.close_acct_seq_version_id,
    aeh.doc_sequence_id,
    ael.gl_sl_link_table,
    ael.gl_sl_link_id,
    ael.encumbrance_type_id,
    ael.accounting_class_code,
    ael.currency_conversion_type,
    ael.ae_header_id,
    ael.ae_line_num,
    ael.LINE_NUMBER,
    ael.ORIG_LINE_NUMBER,
    ael.LINE_DESCRIPTION,
    ael.ENTERED_CURRENCY,
    ael.CONVERSION_RATE,
    ael.CONVERSION_RATE_DATE,
    ael.CONVERSION_RATE_TYPE_CODE,
    ael.ENTERED_DR,
    ael.ENTERED_CR,
    ael.UNROUNDED_ACCOUNTED_DR,
    ael.UNROUNDED_ACCOUNTED_CR,
    ael.STATISTICAL_AMOUNT,
    ael.RECONCILIATION_REFERENCE,
    ael.PARTY_TYPE_CODE,
    xet.Event_CLASS_CODE,
    xet.Event_TYPE_NAME,
    xet.entity_code,
    xect.Event_CLASS_NAME,
    xle.TRANSACTION_DATE,
    xle.xle_Event_ID,
    xle.Event_DATE,
    xle.Event_NUMBER,
    ent.TRANSACTION_NUMBER,
    ent.ent_entity_code,
    ent.ent_application_id,
    ent.ent_entity_id,
    ent.source_id_int_1,
    xla_dl1.alloc_to_dist_id_num_1,
    xla_dl1.xla_dl1_ae_header_id,
    xla_dl1.xla_dl1_ae_line_num,
    xla_dl1.source_distribution_id_num_1,
    xla_dl1.ACCOUNTED_DR,
    xla_dl1.ACCOUNTED_CR,
    xlk2.lookup_type,
    xlk2.lookup_code,
    xlk2.ACCOUNTING_CLASS_NAME,
    fns.DOCUMENT_SEQUENCE_NAME,
    fsv1.ACCOUNTING_SEQUENCE_NAME,
    fsv1.ACCOUNTING_SEQUENCE_VERSION,
    fsv2.REPORTING_SEQUENCE_NAME,
    fsv2.REPORTING_SEQUENCE_VERSION,
    OM.order_Number,
    OM.address1,
    REC.address1 AS REC_address1,
    ADJ.address1 AS ADJ_address1,
    gdct.conversion_type,
    gdct.CONVERSION_RATE_TYPE,
    xla_dl1.TEMP_LINE_NUM, /* audit columns */
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
    ) || '-' || COALESCE(ael.AE_HEADER_ID, 0) || '-' || COALESCE(ael.ae_line_num, 0) || '-' || COALESCE(xla_dl1.TEMP_LINE_NUM, 0) AS dw_load_id,
    CURRENT_TIMESTAMP() AS dw_insert_date,
    CURRENT_TIMESTAMP() AS dw_update_date
  FROM gold_bec_dwh.CTE_xla_ae_headers AS aeh, gold_bec_dwh.CTE_xla_ae_lines AS ael, gold_bec_dwh.CTE_xla_Event_types_tl AS xet, gold_bec_dwh.CTE_xla_Event_classes_tl AS xect, gold_bec_dwh.CTE_xla_Events AS xle, gold_bec_dwh.CTE_xla_transaction_entities AS ent, gold_bec_dwh.CTE_xla_distribution_links AS xla_dl1, gold_bec_dwh.CTE_fnd_lookup_values AS xlk2, gold_bec_dwh.CTE_fun_seq_versions1 AS fsv1, CTE_fun_seq_versions2 AS fsv2, CTE_fnd_document_sequences AS fns, CTE_gl_daily_conversion_types AS gdct, gold_bec_dwh.CTE_OM AS OM, gold_bec_dwh.CTE_REC AS REC, gold_bec_dwh.CTE_ADJ AS ADJ
  WHERE
    1 = 1
    AND ael.application_id = aeh.application_id
    AND ael.ae_header_id = aeh.ae_header_id
    AND xet.application_id = aeh.application_id
    AND xet.Event_type_code = aeh.Event_type_code
    AND xect.application_id = xet.application_id
    AND xect.entity_code = xet.entity_code
    AND xect.Event_class_code = xet.Event_class_code
    AND xle.application_id = aeh.application_id
    AND xle.Event_id = aeh.Event_id
    AND ent.ent_application_id = aeh.application_id
    AND ent.ent_entity_id = aeh.entity_id
    AND xla_dl1.xla_dl1_ae_header_id = ael.ae_header_id
    AND xla_dl1.xla_dl1_ae_line_num = ael.ae_line_num
    AND xlk2.lookup_code = ael.accounting_class_code
    AND fsv1.SEQ_VERSION_ID() = aeh.completion_acct_seq_version_id
    AND fsv2.SEQ_VERSION_ID() = aeh.close_acct_seq_version_id
    AND fns.APPLICATION_ID() = aeh.application_id
    AND fns.DOC_SEQUENCE_ID() = aeh.doc_sequence_id
    AND ent.source_id_int_1 = OM.TRANSACTION_ID()
    AND xla_dl1.source_distribution_id_num_1 = REC.CUST_TRX_LINE_GL_DIST_ID()
    AND ent.SOURCE_ID_INT_1 = adj.ADJUSTMENT_ID()
    AND gdct.CONVERSION_TYPE() = ael.currency_conversion_type
);
UPDATE bec_etl_ctrl.batch_dw_info SET load_type = 'I', last_refresh_date = CURRENT_TIMESTAMP()
WHERE
  dw_table_name = 'fact_gl_xla_stg' AND batch_name = 'gl';