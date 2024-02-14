/* Delete Records */
DELETE FROM gold_bec_dwh.DIM_PO_RECEIPT_DETAILS
WHERE
  EXISTS(
    SELECT
      1
    FROM silver_bec_ods.rcv_transactions AS rct
    INNER JOIN silver_bec_ods.po_headers_all AS poh
      ON rct.po_header_id = poh.po_header_id
    INNER JOIN silver_bec_ods.po_lines_all AS pol
      ON rct.po_line_id = pol.po_line_id
    INNER JOIN silver_bec_ods.rcv_shipment_headers AS rch
      ON rct.shipment_header_id = rch.shipment_header_id
    WHERE
      rct.destination_type_code = 'INVENTORY'
      AND rct.kca_seq_date > (
        SELECT
          (
            executebegints - prune_days
          )
        FROM bec_etl_ctrl.batch_dw_info
        WHERE
          dw_table_name = 'dim_po_receipt_details' AND batch_name = 'po'
      )
      AND COALESCE(DIM_PO_RECEIPT_DETAILS.rcv_txn_id, 0) = COALESCE(rct.transaction_id, 0)
  );
/* Insert Records */
INSERT INTO gold_bec_dwh.DIM_PO_RECEIPT_DETAILS (
  rcv_txn_id,
  destination_type_code,
  po_header_id,
  po_line_id,
  shipment_header_id,
  po_number,
  po_line_number,
  receipt_num,
  receipt_date,
  is_deleted_flg,
  source_app_id,
  dw_load_id,
  dw_insert_date,
  dw_update_date
)
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
    AND (
      rct.kca_seq_date > (
        SELECT
          (
            executebegints - prune_days
          )
        FROM bec_etl_ctrl.batch_dw_info
        WHERE
          dw_table_name = 'dim_po_receipt_details' AND batch_name = 'po'
      )
    )
);
/* Soft Delete */
UPDATE gold_bec_dwh.DIM_PO_RECEIPT_DETAILS SET is_deleted_flg = 'Y'
WHERE
  NOT EXISTS(
    SELECT
      1
    FROM silver_bec_ods.rcv_transactions AS rct
    INNER JOIN silver_bec_ods.po_headers_all AS poh
      ON rct.po_header_id = poh.po_header_id
    INNER JOIN silver_bec_ods.po_lines_all AS pol
      ON rct.po_line_id = pol.po_line_id
    INNER JOIN silver_bec_ods.rcv_shipment_headers AS rch
      ON rct.shipment_header_id = rch.shipment_header_id
    WHERE
      rct.destination_type_code = 'INVENTORY'
      AND rct.is_deleted_flg <> 'Y'
      AND COALESCE(DIM_PO_RECEIPT_DETAILS.rcv_txn_id, 0) = COALESCE(rct.transaction_id, 0)
  );
UPDATE bec_etl_ctrl.batch_dw_info SET load_type = 'I', last_refresh_date = CURRENT_TIMESTAMP()
WHERE
  dw_table_name = 'dim_po_receipt_details' AND batch_name = 'po';