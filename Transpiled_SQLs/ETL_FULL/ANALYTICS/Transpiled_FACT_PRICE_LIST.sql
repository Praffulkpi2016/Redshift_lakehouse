DROP table IF EXISTS gold_bec_dwh.FACT_PRICE_LIST;
CREATE TABLE gold_bec_dwh.FACT_PRICE_LIST AS
(
  SELECT
    COALESCE(SPLL.ORGANIZATION_ID, 90) AS ORGANIZATION_ID,
    (
      SELECT
        system_id
      FROM bec_etl_ctrl.etlsourceappid
      WHERE
        source_system = 'EBS'
    ) || '-' || COALESCE(SPLL.ORGANIZATION_ID, 90) AS ORGANIZATION_ID_KEY,
    SPLL.INVENTORY_ITEM_ID,
    SPLL.LIST_LINE_ID,
    SPLL.LIST_HEADER_ID,
    SPL.TERMS_ID,
    QPLT.NAME AS PRICE_LIST_NAME,
    QPLT.DESCRIPTION,
    SPL.CURRENCY_CODE AS CURRENCY,
    SPL.START_DATE_ACTIVE AS EFFECTIVITY_DATE_FROM,
    QPA.PRODUCT_ATTRIBUTE_CONTEXT AS PRODUCT_CONTEXT,
    CASE
      WHEN QPA.product_attr_value = 'ALL'
      THEN '-999999'
      ELSE QPA.product_attr_value
    END AS PRODUCT_ATTR_ID, /* Added to fix UAT issue */
    QPA.PRODUCT_UOM_CODE AS UOM,
    SPLL.OPERAND AS VALUE,
    SPLL.ARITHMETIC_OPERATOR AS APPLICATION_METHOD,
    SPLL.START_DATE_ACTIVE AS START_DATE,
    SPL.ORIG_ORG_ID,
    QPA.PRICING_ATTRIBUTE_ID,
    SPLL.LIST_LINE_TYPE_CODE,
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
    ) || '-' || COALESCE(QPLT.LIST_HEADER_ID, 0) || '-' || COALESCE(SPLL.LIST_LINE_ID, 0) || '-' || COALESCE(QPA.PRICING_ATTRIBUTE_ID, 0) AS dw_load_id,
    CURRENT_TIMESTAMP() AS dw_insert_date,
    CURRENT_TIMESTAMP() AS dw_update_date
  FROM (
    SELECT
      *
    FROM silver_bec_ods.QP_LIST_LINES
    WHERE
      is_deleted_flg <> 'Y'
  ) AS SPLL
  LEFT OUTER JOIN (
    SELECT
      *
    FROM silver_bec_ods.QP_PRICING_ATTRIBUTES
    WHERE
      is_deleted_flg <> 'Y'
  ) AS QPA
    ON SPLL.LIST_LINE_ID = QPA.LIST_LINE_ID
  LEFT OUTER JOIN (
    SELECT
      *
    FROM silver_bec_ods.QP_LIST_HEADERS_B
    WHERE
      is_deleted_flg <> 'Y'
  ) AS SPL
    ON SPLL.LIST_HEADER_ID = SPL.LIST_HEADER_ID
  LEFT OUTER JOIN (
    SELECT
      *
    FROM silver_bec_ods.QP_LIST_HEADERS_TL
    WHERE
      is_deleted_flg <> 'Y'
  ) AS QPLT
    ON SPLL.LIST_HEADER_ID = QPLT.LIST_HEADER_ID
    AND SPL.LIST_HEADER_ID = QPLT.LIST_HEADER_ID
    AND QPLT.LANGUAGE = 'US'
);
UPDATE bec_etl_ctrl.batch_dw_info SET load_type = 'I', last_refresh_date = CURRENT_TIMESTAMP()
WHERE
  dw_table_name = 'fact_price_list' AND batch_name = 'om';