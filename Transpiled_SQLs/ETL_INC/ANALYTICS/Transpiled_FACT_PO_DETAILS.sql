/* Delete Records */
DELETE FROM gold_bec_dwh.FACT_PO_DETAILS
WHERE
  EXISTS(
    SELECT
      1
    FROM silver_bec_ods.po_headers_all AS a
    LEFT OUTER JOIN silver_bec_ods.po_lines_all AS b
      ON a.po_header_id = b.po_header_id
    WHERE
      FACT_PO_DETAILS.dw_load_id = (
        SELECT
          system_id
        FROM bec_etl_ctrl.etlsourceappid
        WHERE
          source_system = 'EBS'
      ) || '-' || COALESCE(a.po_header_id, 0) || '-' || COALESCE(b.po_line_id, 0)
      AND (
        a.kca_seq_date > (
          SELECT
            executebegints - prune_days
          FROM bec_etl_ctrl.batch_dw_info
          WHERE
            dw_table_name = 'fact_po_details' AND batch_name = 'po'
        )
        OR b.kca_seq_date > (
          SELECT
            executebegints - prune_days
          FROM bec_etl_ctrl.batch_dw_info
          WHERE
            dw_table_name = 'fact_po_details' AND batch_name = 'po'
        )
      )
  );
/* Insert records */
INSERT INTO gold_bec_dwh.FACT_PO_DETAILS (
  po_header_id,
  agent_id,
  type_lookup_code,
  last_updated_by,
  po_number,
  summary_flag,
  enabled_flag,
  segment2,
  segment3,
  segment4,
  segment5,
  start_date_active,
  end_date_active,
  creation_date,
  po_creation_month,
  vendor_id,
  vendor_site_id,
  created_by,
  po_supplier_id_key,
  po_supplier_site_id_key,
  vendor_contact_id,
  ship_to_location_id,
  bill_to_location_id,
  terms_id,
  ship_via_lookup_code,
  fob_lookup_code,
  freight_terms_lookup_code,
  currency_code,
  start_date,
  end_date,
  blanket_total_amount,
  authorization_status,
  revision_num,
  revised_date,
  approved_flag,
  approved_date,
  amount_limit,
  print_count,
  printed_date,
  vendor_order_num,
  rfq_close_date,
  closed_date,
  cancel_flag,
  closed_code,
  line_closed_code,
  location_closed_code,
  org_id,
  po_line_id,
  line_last_update,
  line_updated_by,
  line_type_id,
  line_num,
  line_creation_date,
  line_created_by,
  item_id,
  inventory_organization_id,
  inventory_item_id,
  item_revision,
  category_id,
  item_description,
  unit_meas_lookup_code,
  quantity_committed,
  committed_amount,
  list_price_per_unit,
  unit_price,
  quantity,
  po_amont,
  un_number_id,
  hazard_class_id,
  qty_rcv_tolerance,
  over_tolerance_error_flag,
  market_price,
  line_cancel_flag,
  line_cancelled_by,
  line_cancel_date,
  line_cancel_reason,
  taxable_flag,
  type_1099,
  capital_expense_flag,
  attribute1,
  attribute2,
  attribute3,
  attribute4,
  attribute5,
  attribute6,
  attribute7,
  attribute8,
  attribute9,
  attribute10,
  line_closed_date,
  line_closed_reason,
  promised_date,
  cvmi_flag,
  po_date,
  release_num,
  document_num,
  status_lookup_code,
  asn_po_header_id,
  is_deleted_flg,
  source_app_id,
  dw_load_id,
  dw_insert_date,
  dw_update_date
)
(
  SELECT
    a.po_header_id AS po_header_id,
    a.agent_id,
    a.type_lookup_code,
    a.last_updated_by,
    a.segment1 AS po_number,
    a.summary_flag,
    a.enabled_flag,
    a.segment2,
    a.segment3,
    a.segment4,
    a.segment5,
    a.start_date_active,
    A.END_DATE_ACTIVE,
    FLOOR(a.creation_date) AS creation_date,
    DATE_FORMAT(a.creation_date, 'MON-yyyy') AS po_creation_month,
    vendor_id,
    VENDOR_SITE_ID,
    a.created_by,
    (
      SELECT
        system_id
      FROM bec_etl_ctrl.etlsourceappid
      WHERE
        source_system = 'EBS'
    ) || '-' || COALESCE(a.vendor_id, 0) AS PO_SUPPLIER_ID_KEY,
    (
      SELECT
        system_id
      FROM bec_etl_ctrl.etlsourceappid
      WHERE
        source_system = 'EBS'
    ) || '-' || COALESCE(a.vendor_site_id, 0) AS PO_SUPPLIER_SITE_ID_KEY,
    a.vendor_contact_id,
    a.ship_to_location_id,
    a.bill_to_location_id,
    a.terms_id,
    a.ship_via_lookup_code,
    a.fob_lookup_code,
    a.freight_terms_lookup_code,
    a.currency_code,
    a.start_date,
    a.end_date,
    a.blanket_total_amount,
    a.authorization_status,
    a.revision_num,
    a.revised_date,
    a.approved_flag,
    a.approved_date,
    a.amount_limit,
    a.print_count,
    a.printed_date,
    a.vendor_order_num,
    a.rfq_close_date,
    a.closed_date,
    a.cancel_flag,
    a.closed_code AS closed_code,
    b.closed_code AS line_closed_code,
    poll.closed_code AS location_closed_code,
    a.org_id,
    b.po_line_id AS po_line_id,
    b.last_update_date AS line_last_update,
    b.last_updated_by AS line_updated_by,
    b.line_type_id,
    (
      CAST(b.line_num AS STRING)
    ) AS line_num,
    b.creation_date AS line_creation_date,
    b.created_by AS line_created_by,
    b.item_id,
    fsp.inventory_organization_id,
    B.ITEM_ID AS `INVENTORY_ITEM_ID`,
    b.item_revision,
    b.category_id,
    b.item_description,
    b.unit_meas_lookup_code,
    b.quantity_committed,
    b.committed_amount,
    b.list_price_per_unit,
    b.unit_price,
    b.quantity,
    (
      b.quantity * b.unit_price
    ) AS po_amont,
    b.un_number_id,
    b.hazard_class_id,
    b.qty_rcv_tolerance,
    b.over_tolerance_error_flag,
    b.market_price,
    b.cancel_flag AS line_cancel_flag,
    b.cancelled_by AS line_cancelled_by,
    b.cancel_date AS line_cancel_date,
    b.cancel_reason AS line_cancel_reason,
    b.taxable_flag AS taxable_flag,
    b.type_1099,
    b.capital_expense_flag,
    b.attribute1,
    b.attribute2,
    b.attribute3,
    b.attribute4,
    b.attribute5,
    b.attribute6,
    b.attribute7,
    b.attribute8,
    b.attribute9,
    b.attribute10,
    b.closed_date AS line_closed_date,
    b.closed_reason AS line_closed_reason,
    poll.promised_date,
    poll.CVMI_FLAG,
    pr.release_date AS po_date,
    pr.release_num,
    a.clm_document_number AS document_num,
    a.status_lookup_code,
    0 AS asn_po_header_id,
    'N' AS IS_DELETED_FLG,
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
    ) || '-' || COALESCE(a.po_header_id, 0) || '-' || COALESCE(b.po_line_id, 0) AS dw_load_id,
    CURRENT_TIMESTAMP() AS dw_insert_date,
    CURRENT_TIMESTAMP() AS dw_update_date
  FROM (
    SELECT
      *
    FROM silver_bec_ods.po_headers_all
    WHERE
      is_deleted_flg <> 'Y'
  ) AS a
  LEFT OUTER JOIN (
    SELECT
      *
    FROM silver_bec_ods.po_lines_all
    WHERE
      is_deleted_flg <> 'Y'
  ) AS b
    ON a.po_header_id = b.po_header_id
  LEFT OUTER JOIN (
    SELECT
      *
    FROM silver_bec_ods.financials_system_params_all
    WHERE
      is_deleted_flg <> 'Y'
  ) AS fsp
    ON a.org_id = fsp.org_id
  LEFT OUTER JOIN (
    SELECT
      MAX(promised_date) AS promised_date,
      po_header_id,
      po_line_id,
      MAX(closed_code) AS closed_code,
      COALESCE(consigned_flag, 'N') AS CVMI_FLAG,
      MAX(po_release_id) AS po_release_id
    FROM (
      SELECT
        *
      FROM silver_bec_ods.po_line_locations_all
      WHERE
        is_deleted_flg <> 'Y'
    )
    GROUP BY
      po_header_id,
      po_line_id,
      COALESCE(consigned_flag, 'N')
  ) AS poll
    ON (
      a.po_header_id = poll.po_header_id AND b.po_line_id = poll.po_line_id
    )
  LEFT OUTER JOIN (
    SELECT
      *
    FROM silver_bec_ods.po_releases_all
    WHERE
      is_deleted_flg <> 'Y'
  ) AS pr
    ON (
      poll.po_release_id = pr.po_release_id AND poll.po_header_id = pr.po_header_id
    )
  WHERE
    1 = 1
    AND (
      a.kca_seq_date > (
        SELECT
          executebegints - prune_days
        FROM bec_etl_ctrl.batch_dw_info
        WHERE
          dw_table_name = 'fact_po_details' AND batch_name = 'po'
      )
      OR b.kca_seq_date > (
        SELECT
          executebegints - prune_days
        FROM bec_etl_ctrl.batch_dw_info
        WHERE
          dw_table_name = 'fact_po_details' AND batch_name = 'po'
      )
    )
);
UPDATE bec_etl_ctrl.batch_dw_info SET load_type = 'I', last_refresh_date = CURRENT_TIMESTAMP()
WHERE
  dw_table_name = 'fact_po_details' AND batch_name = 'po';