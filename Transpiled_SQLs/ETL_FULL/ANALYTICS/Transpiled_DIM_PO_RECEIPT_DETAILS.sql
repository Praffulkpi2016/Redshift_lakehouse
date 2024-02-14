DROP table IF EXISTS gold_bec_dwh.DIM_PO_RECEIPT_DETAILS;
CREATE TABLE gold_bec_dwh.DIM_PO_RECEIPT_DETAILS AS
(
  SELECT
    rct.transaction_id AS rcv_txn_id,
    rct.destination_type_code,
    poh.po_header_id,
    pol.po_line_id,
    rch.shipment_header_id,
    poh.segment1 AS po_number,
    pol.line_num AS po_line_number,
    rch.receipt_num,
    rch.creation_date AS receipt_date,
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
    ) || '-' || COALESCE(rcv_txn_id, 0) AS dw_load_id,
    CURRENT_TIMESTAMP() AS dw_insert_date,
    CURRENT_TIMESTAMP() AS dw_update_date
  FROM silver_bec_ods.rcv_transactions AS rct, silver_bec_ods.po_headers_all AS poh, silver_bec_ods.po_lines_all AS pol, silver_bec_ods.rcv_shipment_headers AS rch
  WHERE
    rct.po_header_id = poh.po_header_id
    AND rct.shipment_header_id = rch.shipment_header_id
    AND rct.po_line_id = pol.po_line_id
    AND rct.destination_type_code = 'INVENTORY'
);
UPDATE bec_etl_ctrl.batch_dw_info SET load_type = 'I', last_refresh_date = CURRENT_TIMESTAMP()
WHERE
  dw_table_name = 'dim_po_receipt_details' AND batch_name = 'po';