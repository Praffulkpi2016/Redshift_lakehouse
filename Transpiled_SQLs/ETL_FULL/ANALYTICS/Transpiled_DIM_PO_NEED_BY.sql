DROP table IF EXISTS gold_bec_dwh.DIM_PO_NEED_BY;
CREATE TABLE gold_bec_dwh.DIM_PO_NEED_BY AS
(
  SELECT
    po_header_id,
    po_line_id,
    MAX(need_by_date) AS max_need_by,
    MIN(need_by_date) AS min_need_by,
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
    ) || '-' || COALESCE(po_header_id, 0) || '-' || COALESCE(po_line_id, 0) AS dw_load_id,
    CURRENT_TIMESTAMP() AS dw_insert_date,
    CURRENT_TIMESTAMP() AS dw_update_date
  FROM silver_bec_ods.po_line_locations_all
  WHERE
    is_deleted_flg <> 'Y'
  GROUP BY
    po_header_id,
    po_line_id
);
UPDATE bec_etl_ctrl.batch_dw_info SET load_type = 'I', last_refresh_date = CURRENT_TIMESTAMP()
WHERE
  dw_table_name = 'dim_po_need_by' AND batch_name = 'po';