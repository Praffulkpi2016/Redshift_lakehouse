DROP table IF EXISTS gold_bec_dwh.DIM_PO_NAME CASCADE;
CREATE TABLE gold_bec_dwh.dim_po_name AS
(
  SELECT
    poh.po_header_id,
    pol.po_line_id,
    poh.segment1 AS po_number,
    pol.line_num,
    poh.creation_date,
    poh.last_update_date,
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
    ) || '-' || COALESCE(poh.po_header_id, 0) || '-' || COALESCE(pol.po_line_id, 0) AS dw_load_id,
    CURRENT_TIMESTAMP() AS dw_insert_date,
    CURRENT_TIMESTAMP() AS dw_update_date
  FROM silver_bec_ods.po_headers_all AS poh, silver_bec_ods.po_lines_all AS pol
  WHERE
    1 = 1
    AND poh.po_header_id = pol.po_header_id
    AND (
      poh.is_deleted_flg <> 'Y' OR pol.is_deleted_flg <> 'Y'
    )
);
UPDATE bec_etl_ctrl.batch_dw_info SET load_type = 'I', last_refresh_date = CURRENT_TIMESTAMP()
WHERE
  dw_table_name = 'dim_po_name' AND batch_name = 'po';