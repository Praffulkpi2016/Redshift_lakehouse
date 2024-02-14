/* delete records */
DELETE FROM gold_bec_dwh.DIM_OM_PRICE_LIST
WHERE
  (
    COALESCE(LIST_HEADER_ID, 0)
  ) IN (
    SELECT
      COALESCE(ODS.LIST_HEADER_ID, 0) AS LIST_HEADER_ID
    FROM gold_bec_dwh.DIM_OM_PRICE_LIST AS DW, (
      SELECT
        SPL.LIST_HEADER_ID
      FROM BEC_ODS.QP_LIST_HEADERS_B AS SPL, BEC_ODS.QP_LIST_HEADERS_TL AS QPLT
      WHERE
        1 = 1
        AND SPL.LIST_HEADER_ID = QPLT.LIST_HEADER_ID
        AND QPLT.LANGUAGE = 'US'
        AND (
          QPLT.kca_seq_date > (
            SELECT
              (
                executebegints - prune_days
              )
            FROM bec_etl_ctrl.batch_dw_info
            WHERE
              dw_table_name = 'dim_om_price_list' AND batch_name = 'om'
          )
        )
    ) AS ODS
    WHERE
      1 = 1
      AND DW.DW_LOAD_ID = (
        SELECT
          system_id
        FROM bec_etl_ctrl.etlsourceappid
        WHERE
          source_system = 'EBS'
      ) || '-' || COALESCE(ods.LIST_HEADER_ID, 0)
  );
/* Insert Records */
INSERT INTO gold_bec_dwh.DIM_OM_PRICE_LIST (
  list_header_id,
  terms_id,
  price_list_name,
  description,
  currency,
  effectivity_date_from,
  effectivity_date_to,
  orig_org_id,
  automatic_flag,
  rounding_factor,
  source_system_code,
  global_flag,
  is_deleted_flg,
  source_app_id,
  dw_load_id,
  dw_insert_date,
  dw_update_date
)
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
    1 = 1
    AND SPL.LIST_HEADER_ID = QPLT.LIST_HEADER_ID
    AND QPLT.LANGUAGE = 'US'
    AND (
      QPLT.kca_seq_date > (
        SELECT
          (
            executebegints - prune_days
          )
        FROM bec_etl_ctrl.batch_dw_info
        WHERE
          dw_table_name = 'dim_om_price_list' AND batch_name = 'om'
      )
    )
);
/* Soft delete */
UPDATE gold_bec_dwh.DIM_OM_PRICE_LIST SET is_deleted_flg = 'Y'
WHERE
  NOT (
    COALESCE(LIST_HEADER_ID, 0)
  ) IN (
    SELECT
      COALESCE(ODS.LIST_HEADER_ID, 0) AS LIST_HEADER_ID
    FROM gold_bec_dwh.DIM_OM_PRICE_LIST AS DW, (
      SELECT
        SPL.LIST_HEADER_ID
      FROM (
        SELECT
          *
        FROM BEC_ODS.QP_LIST_HEADERS_B
        WHERE
          is_deleted_flg <> 'Y'
      ) AS SPL, (
        SELECT
          *
        FROM BEC_ODS.QP_LIST_HEADERS_TL
        WHERE
          is_deleted_flg <> 'Y'
      ) AS QPLT
      WHERE
        1 = 1 AND SPL.LIST_HEADER_ID = QPLT.LIST_HEADER_ID AND QPLT.LANGUAGE = 'US'
    ) AS ODS
    WHERE
      1 = 1
      AND DW.DW_LOAD_ID = (
        SELECT
          system_id
        FROM bec_etl_ctrl.etlsourceappid
        WHERE
          source_system = 'EBS'
      ) || '-' || COALESCE(ods.LIST_HEADER_ID, 0)
  );