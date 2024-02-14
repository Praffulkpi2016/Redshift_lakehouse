/* Delete Records */
DELETE FROM gold_bec_dwh.FACT_PRICE_LIST
WHERE
  (COALESCE(LIST_HEADER_ID, 0), COALESCE(LIST_LINE_ID, 0), COALESCE(PRICING_ATTRIBUTE_ID, 0)) IN (
    SELECT
      COALESCE(ods.LIST_HEADER_ID, 0) AS LIST_HEADER_ID,
      COALESCE(ods.LIST_LINE_ID, 0) AS LIST_LINE_ID,
      COALESCE(ods.PRICING_ATTRIBUTE_ID, 0) AS PRICING_ATTRIBUTE_ID
    FROM gold_bec_dwh.FACT_PRICE_LIST AS dw, (
      SELECT
        QPLT.LIST_HEADER_ID,
        SPLL.LIST_LINE_ID,
        QPA.PRICING_ATTRIBUTE_ID
      FROM silver_bec_ods.QP_LIST_LINES AS SPLL
      LEFT OUTER JOIN silver_bec_ods.QP_PRICING_ATTRIBUTES AS QPA
        ON SPLL.LIST_LINE_ID = QPA.LIST_LINE_ID
      LEFT OUTER JOIN silver_bec_ods.QP_LIST_HEADERS_B AS SPL
        ON SPLL.LIST_HEADER_ID = SPL.LIST_HEADER_ID
      LEFT OUTER JOIN silver_bec_ods.QP_LIST_HEADERS_TL AS QPLT
        ON SPLL.LIST_HEADER_ID = QPLT.LIST_HEADER_ID
        AND SPL.LIST_HEADER_ID = QPLT.LIST_HEADER_ID
        AND QPLT.LANGUAGE = 'US'
      WHERE
        (
          QPLT.kca_seq_date >= (
            SELECT
              (
                executebegints - prune_days
              )
            FROM bec_etl_ctrl.batch_dw_info
            WHERE
              dw_table_name = 'fact_price_list' AND batch_name = 'om'
          )
          OR SPLL.kca_seq_date >= (
            SELECT
              (
                executebegints - prune_days
              )
            FROM bec_etl_ctrl.batch_dw_info
            WHERE
              dw_table_name = 'fact_price_list' AND batch_name = 'om'
          )
          OR QPA.kca_seq_date >= (
            SELECT
              (
                executebegints - prune_days
              )
            FROM bec_etl_ctrl.batch_dw_info
            WHERE
              dw_table_name = 'fact_price_list' AND batch_name = 'om'
          )
          OR QPLT.IS_DELETED_FLG = 'Y'
          OR SPLL.IS_DELETED_FLG = 'Y'
          OR QPA.IS_DELETED_FLG = 'Y'
          OR SPL.IS_DELETED_FLG = 'Y'
        )
    ) AS ods
    WHERE
      1 = 1
      AND dw.dw_load_id = (
        SELECT
          system_id
        FROM bec_etl_ctrl.etlsourceappid
        WHERE
          source_system = 'EBS'
      ) || '-' || COALESCE(ODS.LIST_HEADER_ID, 0) || '-' || COALESCE(ODS.LIST_LINE_ID, 0) || '-' || COALESCE(ODS.PRICING_ATTRIBUTE_ID, 0)
  );
/* Insert Records */
INSERT INTO gold_bec_dwh.fact_price_list (
  organization_id,
  organization_id_key,
  inventory_item_id,
  list_line_id,
  list_header_id,
  terms_id,
  price_list_name,
  description,
  currency,
  effectivity_date_from,
  product_context,
  PRODUCT_ATTR_ID,
  uom,
  value,
  application_method,
  start_date,
  orig_org_id,
  pricing_attribute_id,
  LIST_LINE_TYPE_CODE,
  is_deleted_flg,
  source_app_id,
  dw_load_id,
  dw_insert_date,
  dw_update_date
)
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
  WHERE
    (
      QPLT.kca_seq_date >= (
        SELECT
          (
            executebegints - prune_days
          )
        FROM bec_etl_ctrl.batch_dw_info
        WHERE
          dw_table_name = 'fact_price_list' AND batch_name = 'om'
      )
      OR SPLL.kca_seq_date >= (
        SELECT
          (
            executebegints - prune_days
          )
        FROM bec_etl_ctrl.batch_dw_info
        WHERE
          dw_table_name = 'fact_price_list' AND batch_name = 'om'
      )
      OR QPA.kca_seq_date >= (
        SELECT
          (
            executebegints - prune_days
          )
        FROM bec_etl_ctrl.batch_dw_info
        WHERE
          dw_table_name = 'fact_price_list' AND batch_name = 'om'
      )
    )
);
UPDATE bec_etl_ctrl.batch_dw_info SET load_type = 'I', last_refresh_date = CURRENT_TIMESTAMP()
WHERE
  dw_table_name = 'fact_price_list' AND batch_name = 'om';