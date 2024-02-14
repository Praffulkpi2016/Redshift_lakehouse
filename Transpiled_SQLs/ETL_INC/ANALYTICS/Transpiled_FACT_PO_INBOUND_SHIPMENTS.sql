/* Delete Records */
DELETE FROM gold_bec_dwh.FACT_PO_INBOUND_SHIPMENTS
WHERE
  EXISTS(
    SELECT
      1
    FROM silver_bec_ods.rcv_shipment_headers AS a
    JOIN silver_bec_ods.rcv_shipment_lines AS b
      ON a.SHIPMENT_HEADER_ID = b.SHIPMENT_HEADER_ID
    JOIN silver_bec_ods.org_organization_definitions AS OD
      ON a.SHIP_TO_ORG_ID = OD.ORGANIZATION_ID
    JOIN silver_bec_ods.RCV_TRANSACTIONS AS T
      ON b.SHIPMENT_LINE_ID = T.SHIPMENT_LINE_ID
    WHERE
      T.TRANSACTION_TYPE = 'RECEIVE'
      AND (
        a.kca_seq_date > (
          SELECT
            (
              executebegints - prune_days
            )
          FROM bec_etl_ctrl.batch_dw_info
          WHERE
            dw_table_name = 'fact_po_inbound_shipments' AND batch_name = 'po'
        )
        OR b.kca_seq_date > (
          SELECT
            (
              executebegints - prune_days
            )
          FROM bec_etl_ctrl.batch_dw_info
          WHERE
            dw_table_name = 'fact_po_inbound_shipments' AND batch_name = 'po'
        )
        OR od.kca_seq_date > (
          SELECT
            (
              executebegints - prune_days
            )
          FROM bec_etl_ctrl.batch_dw_info
          WHERE
            dw_table_name = 'fact_po_inbound_shipments' AND batch_name = 'po'
        )
        OR t.kca_seq_date > (
          SELECT
            (
              executebegints - prune_days
            )
          FROM bec_etl_ctrl.batch_dw_info
          WHERE
            dw_table_name = 'fact_po_inbound_shipments' AND batch_name = 'po'
        )
      )
      AND FACT_PO_INBOUND_SHIPMENTS.dw_load_id = (
        SELECT
          system_id
        FROM bec_etl_ctrl.etlsourceappid
        WHERE
          source_system = 'EBS'
      ) || '-' || COALESCE(a.SHIPMENT_HEADER_ID, 0) || '-' || COALESCE(b.SHIPMENT_LINE_ID, 0) || '-' || COALESCE(OD.ORGANIZATION_ID, 0) || '-' || COALESCE(T.TRANSACTION_ID, 0)
  );
/* Insert records */
INSERT INTO gold_bec_dwh.FACT_PO_INBOUND_SHIPMENTS (
  transaction_id,
  transaction_date,
  shipment_header_id,
  header_creation_date,
  ORGANIZATION_ID_KEY,
  receipt_source_code,
  vendor_id,
  vendor_site_id,
  vendor_id_key,
  vendor_site_id_key,
  org_id,
  ORG_ID_KEY,
  source_org_id,
  organization_id,
  shipment_num,
  receipt_num,
  bill_of_lading,
  packing_slip,
  shipped_date,
  freight_carrier_code,
  num_of_containers,
  waybill_airbill_num,
  government_context,
  edi_control_num,
  hazard_code,
  invoice_num,
  invoice_date,
  invoice_amount,
  header_tax_amount,
  freight_amount,
  currency_code,
  payment_terms_id,
  ship_to_org_id,
  ship_to_location_id,
  shipment_line_id,
  line_last_update,
  line_updated_by,
  line_creation_date,
  line_created_by,
  line_num,
  category_id,
  quantity_shipped,
  quantity_received,
  unit_of_measure,
  item_description,
  item_id,
  inventory_org_id,
  inventory_item_id,
  item_revision,
  vendor_item_num,
  vendor_lot_num,
  shipment_line_status_code,
  source_document_code,
  po_header_id,
  po_release_id,
  po_line_id,
  to_organization_id,
  po_line_location_id,
  po_distribution_id,
  requisition_line_id,
  req_distribution_id,
  routing_header_id,
  from_organization_id,
  deliver_to_person_id,
  employee_id,
  destination_type_code,
  to_subinventory,
  locator_id,
  deliver_to_location_id,
  comments,
  reason_id,
  ussgl_transaction_code,
  primary_unit_of_measure,
  vendor_cum_shipped_quantity,
  notice_unit_price,
  tax_name,
  tax_amount,
  container_num,
  truck_num,
  transaction_quantity,
  AMOUNT_RECEIVED,
  expected_receipt_date,
  GBL_INVOICE_AMOUNT,
  GBL_AMOUNT_PAID,
  is_deleted_flg,
  source_app_id,
  dw_load_id,
  dw_insert_date,
  dw_update_date
)
(
  SELECT
    T.TRANSACTION_ID,
    T.TRANSACTION_DATE,
    a.shipment_header_id,
    a.creation_date AS header_creation_date,
    (
      SELECT
        system_id
      FROM bec_etl_ctrl.etlsourceappid
      WHERE
        source_system = 'EBS'
    ) || '-' || OD.ORGANIZATION_ID AS ORGANIZATION_ID_KEY,
    a.receipt_source_code,
    a.vendor_id,
    a.vendor_site_id,
    (
      SELECT
        system_id
      FROM bec_etl_ctrl.etlsourceappid
      WHERE
        source_system = 'EBS'
    ) || '-' || a.vendor_id AS VENDOR_ID_KEY,
    (
      SELECT
        system_id
      FROM bec_etl_ctrl.etlsourceappid
      WHERE
        source_system = 'EBS'
    ) || '-' || a.vendor_site_id AS VENDOR_SITE_ID_KEY,
    OD.OPERATING_UNIT AS ORG_ID,
    (
      SELECT
        system_id
      FROM bec_etl_ctrl.etlsourceappid
      WHERE
        source_system = 'EBS'
    ) || '-' || OD.OPERATING_UNIT AS ORG_ID_KEY,
    A.organization_id AS `SOURCE_ORG_ID`,
    OD.ORGANIZATION_ID,
    a.shipment_num,
    a.receipt_num,
    a.bill_of_lading,
    a.packing_slip,
    a.shipped_date,
    a.freight_carrier_code,
    a.num_of_containers,
    a.waybill_airbill_num,
    a.government_context,
    a.edi_control_num,
    a.hazard_code,
    a.invoice_num,
    a.invoice_date,
    a.invoice_amount,
    a.tax_amount AS header_tax_amount,
    a.freight_amount,
    a.currency_code,
    a.payment_terms_id,
    a.ship_to_org_id,
    a.ship_to_location_id,
    b.shipment_line_id,
    b.last_update_date AS line_last_update,
    b.last_updated_by AS line_updated_by,
    b.creation_date AS line_creation_date,
    b.created_by AS line_created_by,
    b.line_num AS line_num,
    b.category_id,
    b.quantity_shipped,
    b.quantity_received,
    b.unit_of_measure,
    b.item_description,
    b.item_id,
    b.to_organization_id AS `INVENTORY_ORG_ID`,
    B.ITEM_ID AS `INVENTORY_ITEM_ID`,
    b.item_revision,
    b.vendor_item_num,
    b.vendor_lot_num,
    b.shipment_line_status_code,
    b.source_document_code,
    b.po_header_id,
    b.po_release_id,
    b.po_line_id,
    b.to_organization_id,
    b.po_line_location_id,
    b.po_distribution_id,
    b.requisition_line_id,
    b.req_distribution_id,
    b.routing_header_id,
    b.from_organization_id,
    b.deliver_to_person_id,
    b.employee_id,
    b.destination_type_code,
    b.to_subinventory,
    b.locator_id,
    b.deliver_to_location_id,
    b.comments,
    b.reason_id,
    b.ussgl_transaction_code,
    b.primary_unit_of_measure,
    b.vendor_cum_shipped_quantity,
    b.notice_unit_price,
    b.tax_name,
    b.tax_amount,
    b.container_num,
    b.truck_num,
    T.QUANTITY AS `TRANSACTION_QUANTITY`,
    b.AMOUNT_RECEIVED,
    a.expected_receipt_date,
    CAST(COALESCE(a.invoice_amount, 0) * COALESCE(DCR.conversion_rate, 1) AS DECIMAL(18, 2)) AS GBL_INVOICE_AMOUNT,
    CAST(COALESCE(b.AMOUNT_RECEIVED, 0) * COALESCE(DCR.conversion_rate, 1) AS DECIMAL(18, 2)) AS GBL_AMOUNT_PAID,
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
    ) || '-' || COALESCE(a.SHIPMENT_HEADER_ID, 0) || '-' || COALESCE(b.SHIPMENT_LINE_ID, 0) || '-' || COALESCE(OD.ORGANIZATION_ID, 0) || '-' || COALESCE(T.TRANSACTION_ID, 0) AS dw_load_id,
    CURRENT_TIMESTAMP() AS dw_insert_date,
    CURRENT_TIMESTAMP() AS dw_update_date
  FROM (
    SELECT
      *
    FROM silver_bec_ods.rcv_shipment_headers
    WHERE
      is_deleted_flg <> 'Y'
  ) AS a, (
    SELECT
      *
    FROM silver_bec_ods.rcv_shipment_lines
    WHERE
      is_deleted_flg <> 'Y'
  ) AS b, (
    SELECT
      *
    FROM silver_bec_ods.org_organization_definitions
    WHERE
      is_deleted_flg <> 'Y'
  ) AS OD, (
    SELECT
      *
    FROM silver_bec_ods.RCV_TRANSACTIONS
    WHERE
      is_deleted_flg <> 'Y'
  ) AS T, (
    SELECT
      *
    FROM silver_bec_ods.GL_DAILY_RATES
    WHERE
      is_deleted_flg <> 'Y'
  ) AS DCR
  WHERE
    1 = 1
    AND (
      a.kca_seq_date > (
        SELECT
          (
            executebegints - prune_days
          )
        FROM bec_etl_ctrl.batch_dw_info
        WHERE
          dw_table_name = 'fact_po_inbound_shipments' AND batch_name = 'po'
      )
      OR b.kca_seq_date > (
        SELECT
          (
            executebegints - prune_days
          )
        FROM bec_etl_ctrl.batch_dw_info
        WHERE
          dw_table_name = 'fact_po_inbound_shipments' AND batch_name = 'po'
      )
      OR od.kca_seq_date > (
        SELECT
          (
            executebegints - prune_days
          )
        FROM bec_etl_ctrl.batch_dw_info
        WHERE
          dw_table_name = 'fact_po_inbound_shipments' AND batch_name = 'po'
      )
      OR t.kca_seq_date > (
        SELECT
          (
            executebegints - prune_days
          )
        FROM bec_etl_ctrl.batch_dw_info
        WHERE
          dw_table_name = 'fact_po_inbound_shipments' AND batch_name = 'po'
      )
    )
    AND A.SHIP_TO_ORG_ID = OD.ORGANIZATION_ID
    AND A.SHIPMENT_HEADER_ID = B.SHIPMENT_HEADER_ID
    AND B.SHIPMENT_LINE_ID = T.SHIPMENT_LINE_ID
    AND T.TRANSACTION_TYPE = 'RECEIVE'
    AND DCR.TO_CURRENCY() = 'USD'
    AND DCR.CONVERSION_TYPE() = 'Corporate'
    AND A.currency_code = DCR.FROM_CURRENCY()
    AND DCR.CONVERSION_DATE() = A.invoice_date
);
UPDATE bec_etl_ctrl.batch_dw_info SET load_type = 'I', last_refresh_date = CURRENT_TIMESTAMP()
WHERE
  dw_table_name = 'fact_po_inbound_shipments' AND batch_name = 'po';