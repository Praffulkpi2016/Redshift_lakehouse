UPDATE gold_bec_dwh.fact_po_details SET asn_po_header_id = shp.po_header_id
FROM (
  SELECT DISTINCT
    po_header_id
  FROM gold_bec_dwh.fact_po_shipment
) AS shp
WHERE
  fact_po_details.po_header_id = shp.po_header_id;
UPDATE bec_etl_ctrl.batch_dw_info SET load_type = 'I', last_refresh_date = CURRENT_TIMESTAMP()
WHERE
  dw_table_name = 'fact_po_details_asn_update' AND batch_name = 'po';