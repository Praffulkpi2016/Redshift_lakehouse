TRUNCATE table gold_bec_dwh.FACT_OM_IR_ISO;
INSERT INTO gold_bec_dwh.FACT_OM_IR_ISO
(
  SELECT DISTINCT
    ooh.org_id,
    ooh.header_id,
    ool.line_id,
    rql.item_id,
    rqh.requisition_header_id,
    rql.requisition_line_id,
    rql.source_organization_id,
    rqd.distribution_id,
    mmt.transaction_id,
    mta.accounting_line_type,
    mp.organization_code AS source_organization_code,
    rql.destination_organization_id,
    rql.deliver_to_location_id AS destination_location,
    hp.party_name AS customer,
    hca.account_number AS customer_acc_number,
    rqh.segment1 AS requistion_number,
    ooh.order_number AS ISO_NUMBER,
    ooh.flow_status_code AS header_status,
    ool.flow_status_code AS iso_line_status,
    CASE
      WHEN item_type_code = 'MODEL'
      THEN ool.line_number || '.' || ool.shipment_number
      WHEN item_type_code = 'STANDARD'
      THEN ool.line_number || '.' || ool.shipment_number
      WHEN item_type_code = 'INCLUDED'
      THEN ool.line_number || '.' || ool.shipment_number || '.' || CASE WHEN component_number IS NULL THEN NULL ELSE '.' || component_number END
      WHEN item_type_code = 'SERVICE'
      THEN ool.line_number || '.' || ool.shipment_number || '.' || component_number || '.' || CASE WHEN service_number IS NULL THEN NULL ELSE '.' || service_number END
    END AS ISO_LINE_NUMBER,
    ool.ordered_item,
    mta.rate_or_amount AS requisition_unit_price,
    ool.unit_selling_price AS order_selling_price,
    rql.quantity AS requisition_quantity,
    MTRH.REQUEST_NUMBER AS move_order_number,
    MTRL.PICK_SLIP_NUMBER AS pick_slip_number,
    cta.trx_number AS ar_invoice_num,
    ctl.revenue_amount AS ar_invoice_amount,
    aps.gl_date AS ar_gl_date,
    aia.invoice_num AS ap_invoice_num,
    aial.amount AS ap_invoice_amount,
    aia.gl_date AS ap_gl_date,
    rsh.shipment_num AS shipment_num,
    rsh.shipped_date AS shipped_date,
    rsl.quantity_shipped AS shipped_quantity,
    rsh.receipt_num AS receipt_num,
    rcv.transaction_date AS receipt_date,
    rcv.quantity AS receipt_quantity,
    rcv.subinventory AS subinventory,
    mp.material_account AS inv_gl_account_id,
    rsl.mmt_transaction_id,
    'Account' AS cogs_acct_type,
    'Profit in inventory' AS markup_acct_type,
    'Intransit Inventory' AS intransit_acct_type,
    rqh.creation_date,
    rqh.AUTHORIZATION_STATUS,
    rqh.created_by,
    rqh.last_update_date,
    rqh.last_updated_by,
    (
      SELECT
        system_id
      FROM bec_etl_ctrl.etlsourceappid
      WHERE
        source_system = 'EBS'
    ) || '-' || rqh.org_id AS org_id_KEY,
    (
      SELECT
        system_id
      FROM bec_etl_ctrl.etlsourceappid
      WHERE
        source_system = 'EBS'
    ) || '-' || rql.source_organization_id AS source_organization_id_KEY,
    (
      SELECT
        system_id
      FROM bec_etl_ctrl.etlsourceappid
      WHERE
        source_system = 'EBS'
    ) || '-' || rql.destination_organization_id AS destination_organization_id_KEY,
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
    ) || '-' || COALESCE(rqh.requisition_header_id, 0) || '-' || COALESCE(rqd.distribution_id, 0) || '-' || COALESCE(mmt.transaction_id, 0) || '-' || COALESCE(mta.accounting_line_type, 0) || '-' || COALESCE(rcv.quantity, 0) || '-' || COALESCE(rcv.transaction_date, '1900-01-01') || '-' || COALESCE(MTRH.REQUEST_NUMBER, 'NA') AS dw_load_id,
    CURRENT_TIMESTAMP() AS dw_insert_date,
    CURRENT_TIMESTAMP() AS dw_update_date
  FROM (
    SELECT
      *
    FROM silver_bec_ods.po_requisition_headers_all
    WHERE
      is_deleted_flg <> 'Y'
  ) AS rqh
  INNER JOIN (
    SELECT
      *
    FROM silver_bec_ods.po_requisition_lines_all
    WHERE
      is_deleted_flg <> 'Y'
  ) AS rql
    ON rqh.requisition_header_id = rql.requisition_header_id
  INNER JOIN (
    SELECT
      *
    FROM silver_bec_ods.mtl_parameters
    WHERE
      is_deleted_flg <> 'Y'
  ) AS mp
    ON rql.source_organization_id = MP.organization_id
  INNER JOIN (
    SELECT
      *
    FROM silver_bec_ods.po_req_distributions_all
    WHERE
      is_deleted_flg <> 'Y'
  ) AS rqd
    ON rql.requisition_line_id = rqd.requisition_line_id
  LEFT OUTER JOIN (
    SELECT
      *
    FROM silver_bec_ods.RCV_SHIPMENT_LINES
    WHERE
      is_deleted_flg <> 'Y'
  ) AS RSL
    ON rqd.requisition_line_id = rsl.requisition_line_id
    AND rqd.distribution_id = rsl.req_distribution_id
    AND rql.source_type_code = rsl.destination_type_code
  LEFT OUTER JOIN (
    SELECT
      *
    FROM silver_bec_ods.RCV_SHIPMENT_HEADERS
    WHERE
      is_deleted_flg <> 'Y'
  ) AS RSH
    ON rsl.shipment_header_id = rsh.shipment_header_id
  LEFT OUTER JOIN (
    SELECT
      *
    FROM silver_bec_ods.RCV_TRANSACTIONS
    WHERE
      is_deleted_flg <> 'Y'
  ) AS RCV
    ON rsl.shipment_header_id = rcv.shipment_header_id
    AND rsl.shipment_line_id = rcv.shipment_line_id
    AND rsl.requisition_line_id = rcv.requisition_line_id
    AND rsl.req_distribution_id = rcv.req_distribution_id
    AND rsl.destination_type_code = rcv.destination_type_code
  LEFT OUTER JOIN (
    SELECT
      *
    FROM silver_bec_ods.mtl_material_transactions
    WHERE
      is_deleted_flg <> 'Y'
  ) AS mmt
    ON rsl.mmt_transaction_id = mmt.transaction_id
  LEFT OUTER JOIN (
    SELECT
      *
    FROM silver_bec_ods.mtl_transaction_accounts
    WHERE
      is_deleted_flg <> 'Y'
  ) AS mta
    ON mmt.transaction_id = mta.transaction_id
    AND mmt.intransit_account = mta.reference_account
  LEFT OUTER JOIN (
    SELECT
      *
    FROM silver_bec_ods.ap_invoice_lines_all
    WHERE
      is_deleted_flg <> 'Y'
  ) AS aial
    ON rsl.mmt_transaction_id = aial.reference_2
  LEFT OUTER JOIN (
    SELECT
      *
    FROM silver_bec_ods.ap_invoices_all
    WHERE
      is_deleted_flg <> 'Y'
  ) AS aia
    ON aial.invoice_id = aia.invoice_id
  LEFT OUTER JOIN (
    SELECT
      *
    FROM silver_bec_ods.ra_customer_trx_lines_all
    WHERE
      is_deleted_flg <> 'Y'
  ) AS ctl
    ON rql.item_id = ctl.inventory_item_id
    AND aial.source_trx_id = ctl.customer_trx_id
    AND aial.source_line_id = ctl.customer_trx_line_id
  LEFT OUTER JOIN (
    SELECT
      *
    FROM silver_bec_ods.ra_customer_trx_all
    WHERE
      is_deleted_flg <> 'Y'
  ) AS cta
    ON ctl.customer_trx_id = cta.customer_trx_id
  LEFT OUTER JOIN (
    SELECT
      *
    FROM silver_bec_ods.ar_payment_schedules_all
    WHERE
      is_deleted_flg <> 'Y'
  ) AS aps
    ON cta.customer_trx_id = aps.customer_trx_id AND cta.trx_number = aps.trx_number
  LEFT OUTER JOIN (
    SELECT
      *
    FROM silver_bec_ods.oe_order_lines_all
    WHERE
      is_deleted_flg <> 'Y'
  ) AS ool
    ON ctl.interface_line_attribute9 = CAST(ool.header_id AS STRING)
    AND ctl.interface_line_attribute6 = CAST(ool.line_id AS STRING)
    AND rql.item_id = ool.inventory_item_id
    AND rql.requisition_line_id = ool.source_document_line_id
  LEFT OUTER JOIN (
    SELECT
      *
    FROM silver_bec_ods.oe_order_headers_all
    WHERE
      is_deleted_flg <> 'Y'
  ) AS ooh
    ON ool.header_id = ooh.header_id AND ool.org_id = ooh.org_id
  LEFT OUTER JOIN (
    SELECT
      *
    FROM silver_bec_ods.HZ_CUST_ACCOUNTS
    WHERE
      is_deleted_flg <> 'Y'
  ) AS HCA
    ON OOH.SOLD_TO_ORG_ID = HCA.CUST_ACCOUNT_ID
  LEFT OUTER JOIN (
    SELECT
      *
    FROM silver_bec_ods.HZ_PARTIES
    WHERE
      is_deleted_flg <> 'Y'
  ) AS HP
    ON HCA.PARTY_ID = HP.PARTY_ID
  LEFT OUTER JOIN (
    SELECT
      *
    FROM silver_bec_ods.WSH_DELIVERY_DETAILS
    WHERE
      is_deleted_flg <> 'Y'
  ) AS WDD
    ON OOL.HEADER_ID = WDD.SOURCE_HEADER_ID AND OOL.LINE_ID = WDD.SOURCE_LINE_ID
  LEFT OUTER JOIN (
    SELECT
      *
    FROM silver_bec_ods.MTL_TXN_REQUEST_LINES
    WHERE
      is_deleted_flg <> 'Y'
  ) AS MTRL
    ON WDD.MOVE_ORDER_LINE_ID = MTRL.LINE_ID
  LEFT OUTER JOIN (
    SELECT
      *
    FROM silver_bec_ods.MTL_TXN_REQUEST_HEADERS
    WHERE
      is_deleted_flg <> 'Y'
  ) AS MTRH
    ON MTRL.HEADER_ID = MTRH.HEADER_ID
  WHERE
    1 = 1
    AND rqh.type_lookup_code = 'INTERNAL'
    AND rql.source_type_code = 'INVENTORY'
    AND ooh.order_source_id = 10
    AND ool.order_source_id = 10
);
UPDATE bec_etl_ctrl.batch_dw_info SET load_type = 'I', last_refresh_date = CURRENT_TIMESTAMP()
WHERE
  dw_table_name = 'fact_om_ir_iso' AND batch_name = 'om';