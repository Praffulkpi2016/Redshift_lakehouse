DROP table IF EXISTS gold_bec_dwh.FACT_INV_IOT;
CREATE TABLE gold_bec_dwh.FACT_INV_IOT AS
(
  SELECT
    rch.shipment_header_id,
    rcl.shipment_line_id,
    rcl.item_id,
    rch.organization_id,
    rcl.mmt_transaction_id,
    rch.shipment_num AS iot, /*	 ,a.rid rid */
    msib.segment1 AS part_number,
    REGEXP_REPLACE(msib.description, '[^0-9A-Za-z]', ' ') AS part_description,
    rcl.line_num AS line_num,
    ood.organization_code AS from_location,
    ood1.organization_code AS to_location,
    rcl.quantity_shipped AS ship_qty,
    mmt.transaction_date AS ship_date,
    rcl.quantity_received AS receive_quantity,
    rcl.last_update_date AS receive_Date,
    rcl.shipment_line_status_code AS iot_status,
    (
      SELECT
        system_id
      FROM bec_etl_ctrl.etlsourceappid
      WHERE
        source_system = 'EBS'
    ) || '-' || rcl.item_id AS ITEM_ID_KEY,
    (
      SELECT
        system_id
      FROM bec_etl_ctrl.etlsourceappid
      WHERE
        source_system = 'EBS'
    ) || '-' || rch.organization_id AS ORGANIZATION_ID_KEY,
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
    ) || '-' || COALESCE(rcl.shipment_line_id, 0) AS dw_load_id,
    CURRENT_TIMESTAMP() AS dw_insert_date,
    CURRENT_TIMESTAMP() AS dw_update_date
  FROM (
    SELECT
      *
    FROM silver_bec_ods.rcv_shipment_headers
    WHERE
      is_deleted_flg <> 'Y'
  ) AS rch, (
    SELECT
      *
    FROM silver_bec_ods.rcv_shipment_lines
    WHERE
      is_deleted_flg <> 'Y'
  ) AS rcl, (
    SELECT
      *
    FROM silver_bec_ods.mtl_system_items_b
    WHERE
      is_deleted_flg <> 'Y'
  ) AS msib, (
    SELECT
      *
    FROM silver_bec_ods.org_organization_definitions
    WHERE
      is_deleted_flg <> 'Y'
  ) AS ood, (
    SELECT
      *
    FROM silver_bec_ods.org_organization_definitions
    WHERE
      is_deleted_flg <> 'Y'
  ) AS ood1, (
    SELECT
      *
    FROM silver_bec_ods.mtl_material_transactions
    WHERE
      is_deleted_flg <> 'Y'
  ) AS mmt
  /*	 ,xxbec_iot_number a */
  WHERE
    1 = 1
    AND rch.shipment_header_id = rcl.shipment_header_id
    AND rcl.item_id = msib.inventory_item_id
    AND rch.organization_id = msib.organization_id
    AND rcl.from_organization_id = ood.organization_id
    AND rcl.to_organization_id = ood1.organization_id
    AND rcl.mmt_transaction_id = mmt.transaction_id
    AND source_document_code = 'INVENTORY'
);
UPDATE bec_etl_ctrl.batch_dw_info SET load_type = 'I', last_refresh_date = CURRENT_TIMESTAMP()
WHERE
  dw_table_name = 'fact_inv_iot' AND batch_name = 'inv';