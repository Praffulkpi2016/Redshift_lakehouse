DROP table IF EXISTS gold_bec_dwh.FACT_OM_TRANS_TYPES;
CREATE TABLE gold_bec_dwh.FACT_OM_TRANS_TYPES AS
(
  SELECT
    otta.org_id,
    otta.TRANSACTION_TYPE_ID,
    otta.ORDER_CATEGORY_CODE, /* Lookups dimension lookup type: ORDER_CATEGORY */
    OWA_H.PROCESS_NAME AS ORDER_PROCESS_NAME, /* wf_activities. */
    OTTA.START_DATE_ACTIVE AS PROCESS_START_DATE,
    OWA_H.WF_ITEM_TYPE AS HDR_ITEM_TYPE,
    OTTA.PRICE_LIST_ID,
    OTTA.WAREHOUSE_ID,
    OTTA.SHIPPING_METHOD_CODE, /* SHIPPING_METHOD */
    OTTA.SHIPMENT_PRIORITY_CODE, /* SHIPMENT_PRIORITY */
    OTTA.FREIGHT_TERMS_CODE, /* FREIGHT TERMS */
    OTTA.INVOICE_SOURCE_ID, /* RA_BATCH_SOURCES_ALL */
    RABSA.NAME AS INVOICE_SOURCE,
    OTTA.INVOICING_RULE_ID, /* dim_ar_invoice_rules */
    OTTA.ACCOUNTING_RULE_ID, /* dim_ar_invoice_rules */
    OTTA.CUST_TRX_TYPE_ID, /* dim_ar_cust_trx_type */
    OTTA.COST_OF_GOODS_SOLD_ACCOUNT,
    OTTA.CONVERSION_TYPE_CODE,
    OTTA.CURRENCY_CODE,
    OWA_L.LINE_TYPE_ID AS ASSIGNED_LINE_TYPE_ID,
    'OEOL' AS LINE_ITEM_TYPE,
    OWA_L.ITEM_TYPE_CODE AS ITEM_TYPE,
    OWA_L.PROCESS_NAME AS LINE_PROCESS_NAME,
    OWA_L.START_DATE_ACTIVE,
    OWA_L.END_DATE_ACTIVE,
    OTTA.CREATION_DATE,
    OTTA.CREATED_BY,
    OTTA.LAST_UPDATE_DATE,
    OTTA.LAST_UPDATED_BY,
    OWA_H.ASSIGNMENT_ID,
    (
      SELECT
        system_id
      FROM bec_etl_ctrl.etlsourceappid
      WHERE
        source_system = 'EBS'
    ) || '-' || otta.org_id AS ORG_ID_key,
    'N' AS is_deleted_flg, /* audit columns */
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
    ) || '-' || COALESCE(otta.TRANSACTION_TYPE_ID, 0) || '-' || COALESCE(OWA_L.LINE_TYPE_ID, 0) || '-' || COALESCE(OWA_L.ITEM_TYPE_CODE, 'NA') || '-' || COALESCE(OWA_L.START_DATE_ACTIVE, '1900-01-01 12:00:00') || '-' || COALESCE(OWA_L.END_DATE_ACTIVE, '1900-01-01 12:00:00') AS dw_load_id,
    CURRENT_TIMESTAMP() AS dw_insert_date,
    CURRENT_TIMESTAMP() AS dw_update_date
  FROM (
    SELECT
      *
    FROM silver_bec_ods.OE_TRANSACTION_TYPES_ALL
    WHERE
      is_deleted_flg <> 'Y'
  ) AS OTTA, (
    SELECT
      *
    FROM silver_bec_ods.OE_WORKFLOW_ASSIGNMENTS
    WHERE
      is_deleted_flg <> 'Y'
  ) AS OWA_H, (
    SELECT
      *
    FROM silver_bec_ods.OE_WORKFLOW_ASSIGNMENTS
    WHERE
      is_deleted_flg <> 'Y'
  ) AS OWA_L, (
    SELECT
      *
    FROM silver_bec_ods.RA_BATCH_SOURCES_ALL
    WHERE
      is_deleted_flg <> 'Y'
  ) AS RABSA
  WHERE
    1 = 1
    AND OTTA.TRANSACTION_TYPE_ID = OWA_H.ORDER_TYPE_ID()
    AND (
      OWA_H.END_DATE_ACTIVE IS NULL OR OWA_H.END_DATE_ACTIVE >= CURRENT_TIMESTAMP()
    ) /* AND OTTA.TRANSACTION_TYPE_ID = 1005 */
    AND OWA_H.LINE_TYPE_ID IS NULL
    AND OWA_H.order_type_id = OWA_L.ORDER_TYPE_ID()
    AND COALESCE(OWA_L.LINE_TYPE_ID(), -1) > 0
    AND OTTA.INVOICE_SOURCE_ID = RABSA.BATCH_SOURCE_ID()
    AND OTTA.ORG_ID = RABSA.ORG_ID()
);
UPDATE bec_etl_ctrl.batch_dw_info SET load_type = 'I', last_refresh_date = CURRENT_TIMESTAMP()
WHERE
  dw_table_name = 'fact_om_trans_types' AND batch_name = 'om';