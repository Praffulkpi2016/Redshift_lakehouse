DROP table IF EXISTS gold_bec_dwh.DIM_OM_PO_NUM;
CREATE TABLE gold_bec_dwh.DIM_OM_PO_NUM AS
(
  SELECT
    ODSS.LINE_ID,
    ODSS.PO_HEADER_ID,
    POH.SEGMENT1 AS PO_NUMBER,
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
    ) || '-' || COALESCE(ODSS.LINE_ID, 0) AS dw_load_id,
    CURRENT_TIMESTAMP() AS dw_insert_date,
    CURRENT_TIMESTAMP() AS dw_update_date
  FROM BEC_ODS.OE_DROP_SHIP_SOURCES AS ODSS, BEC_ODS.PO_HEADERS_ALL AS POH
  WHERE
    1 = 1 AND ODSS.PO_HEADER_ID = POH.PO_HEADER_ID()
);
UPDATE bec_etl_ctrl.batch_dw_info SET load_type = 'I', last_refresh_date = CURRENT_TIMESTAMP()
WHERE
  dw_table_name = 'dim_om_po_num' AND batch_name = 'om';