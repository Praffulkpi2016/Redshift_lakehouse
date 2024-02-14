DROP table IF EXISTS gold_bec_dwh.DIM_OM_PRICE_LIST;
CREATE TABLE gold_bec_dwh.DIM_OM_PRICE_LIST AS
(
  SELECT
    SPL.LIST_HEADER_ID,
    SPL.TERMS_ID,
    QPLT.NAME AS PRICE_LIST_NAME,
    QPLT.DESCRIPTION,
    SPL.CURRENCY_CODE AS CURRENCY,
    SPL.START_DATE_ACTIVE AS EFFECTIVITY_DATE_FROM,
    SPL.END_DATE_ACTIVE AS EFFECTIVITY_DATE_TO,
    SPL.ORIG_ORG_ID,
    SPL.AUTOMATIC_FLAG,
    SPL.ROUNDING_FACTOR,
    SPL.SOURCE_SYSTEM_CODE,
    SPL.GLOBAL_FLAG,
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
    ) || '-' || COALESCE(SPL.LIST_HEADER_ID, 0) AS dw_load_id,
    CURRENT_TIMESTAMP() AS dw_insert_date,
    CURRENT_TIMESTAMP() AS dw_update_date
  FROM BEC_ODS.QP_LIST_HEADERS_B AS SPL, BEC_ODS.QP_LIST_HEADERS_TL AS QPLT
  WHERE
    1 = 1 AND SPL.LIST_HEADER_ID = QPLT.LIST_HEADER_ID AND QPLT.LANGUAGE = 'US'
);
UPDATE bec_etl_ctrl.batch_dw_info SET load_type = 'I', last_refresh_date = CURRENT_TIMESTAMP()
WHERE
  dw_table_name = 'dim_om_price_list' AND batch_name = 'om';