DROP table IF EXISTS gold_bec_dwh.DIM_PO_LINE_TYPE;
CREATE TABLE gold_bec_dwh.DIM_PO_LINE_TYPE AS
(
  SELECT
    line_type_id,
    line_type,
    description AS line_type_desc,
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
    ) || '-' || COALESCE(LINE_TYPE_ID, 0) AS dw_load_id,
    CURRENT_TIMESTAMP() AS dw_insert_date,
    CURRENT_TIMESTAMP() AS dw_update_date
  FROM silver_bec_ods.PO_LINE_TYPES_TL
  WHERE
    COALESCE(Language, 'US') = 'US'
);
UPDATE bec_etl_ctrl.batch_dw_info SET load_type = 'I', last_refresh_date = CURRENT_TIMESTAMP()
WHERE
  dw_table_name = 'dim_po_line_type' AND batch_name = 'po';