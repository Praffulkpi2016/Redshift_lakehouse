DROP table IF EXISTS gold_bec_dwh.FACT_PO_SHIPMENT_STG;
CREATE TABLE gold_bec_dwh.FACT_PO_SHIPMENT_STG AS
(
  SELECT
    shp.SOURCE_TYPE,
    shp.CREATED_BY,
    shp.CREATION_DATE,
    shp.LAST_UPDATED_BY,
    shp.LAST_UPDATE_DATE,
    shp.LAST_UPDATE_LOGIN,
    shp.REQUEST_ID,
    shp.PROGRAM_APPLICATION_ID,
    shp.PROGRAM_ID,
    shp.PROGRAM_UPDATE_DATE,
    shp.asn_type,
    shp.rsh_packing_slip,
    shp.shipment_num,
    shp.waybill_airbill_num,
    shp.receipt_num,
    shp.RSH_creation_date,
    shp.shipped_date,
    shp.expected_receipt_date,
    shp.rsh_comments,
    shp.bill_of_lading,
    shp.freight_carrier_code,
    shp.num_of_containers,
    shp.receipt_source_code,
    shp.CHARGE_ACCOUNT_ID,
    shp.rsl_comments,
    shp.DELIVER_TO_LOCATION_ID,
    shp.DELIVER_TO_PERSON_ID,
    shp.DESTINATION_CONTEXT,
    shp.DESTINATION_TYPE_CODE,
    shp.DESTINATION_TYPE,
    shp.EMPLOYEE_ID,
    shp.FROM_ORGANIZATION_ID,
    shp.ITEM_DESCRIPTION,
    shp.ITEM_ID,
    shp.ITEM_REVISION,
    shp.HAZARD_CLASS,
    shp.UN_NUMBER,
    shp.LINE_NUM,
    shp.ITEM_CATEGORY_ID,
    shp.LOCATOR_ID,
    shp.NEED_BY_DATE,
    shp.rsl_packing_slip,
    shp.QUANTITY_RECEIVED,
    shp.QUANTITY_SHIPPED,
    shp.REQUISITION_LINE_ID,
    shp.REQUISITION_HEADER_ID,
    shp.ORDER_NUM,
    shp.ORDER_LINE_NUM,
    shp.REQ_DISTRIBUTION_ID,
    shp.SHIPMENT_HEADER_ID,
    shp.SHIPMENT_LINE_ID,
    shp.SHIPMENT_LINE_STATUS_CODE,
    shp.SOURCE_DOCUMENT_CODE,
    shp.SOURCE_DOCUMENT_TYPE,
    shp.TO_ORGANIZATION_ID,
    shp.TO_SUBINVENTORY,
    shp.TRANSFER_COST,
    shp.TRANSPORTATION_ACCOUNT_ID,
    shp.TRANSPORTATION_COST,
    shp.UNIT_OF_MEASURE,
    shp.UOM_CONVERSION_RATE,
    shp.ROUTING_HEADER_ID,
    shp.ROUTING_NAME,
    shp.REASON_ID,
    shp.REASON_NAME,
    shp.LOCATION_CODE,
    shp.DELIVER_TO_PERSON,
    shp.PO_HEADER_ID,
    shp.PO_LINE_ID,
    shp.PO_LINE_LOCATION_ID,
    shp.PO_RELEASE_ID,
    shp.RELEASE_NUM,
    shp.VENDOR_NAME,
    shp.VENDOR_SITE_CODE,
    shp.SHIP_TO_LOCATION_ID,
    shp.PRIMARY_UNIT_OF_MEASURE,
    shp.VENDOR_ID,
    shp.BAR_CODE_LABEL,
    shp.TRUCK_NUM,
    shp.CONTAINER_NUM,
    shp.VENDOR_LOT_NUM,
    shp.SECONDARY_QUANTITY_RECEIVED,
    shp.SECONDARY_QUANTITY_SHIPPED,
    shp.SECONDARY_UNIT_OF_MEASURE,
    shp.QC_GRADE,
    shp.ORG_ID,
    'N' AS IS_DELETED_FLG,
    shp.source_app_id,
    shp.dw_load_id,
    shp.dw_insert_date,
    shp.dw_update_date
  FROM (
    SELECT
      ' INTERNAL' AS SOURCE_TYPE,
      rsl.created_by,
      rsl.creation_date,
      rsl.last_updated_by,
      rsl.last_update_date,
      rsl.last_update_login,
      rsl.request_id,
      rsl.program_application_id,
      rsl.program_id,
      rsl.program_update_date,
      rsh.asn_type,
      rsh.packing_slip AS rsh_packing_slip,
      rsh.shipment_num,
      rsh.waybill_airbill_num,
      rsh.receipt_num,
      rsh.creation_date AS RSH_creation_date,
      rsh.shipped_date,
      rsh.expected_receipt_date,
      rsh.comments AS rsh_comments,
      rsh.bill_of_lading,
      rsh.freight_carrier_code,
      rsh.num_of_containers,
      rsh.receipt_source_code,
      rsl.charge_account_id,
      rsl.comments AS rsl_comments,
      rsl.deliver_to_location_id,
      rsl.deliver_to_person_id,
      rsl.destination_context,
      rsl.destination_type_code,
      polc.meaning AS destination_type,
      rsl.employee_id,
      rsl.from_organization_id,
      rsl.item_description,
      rsl.item_id,
      rsl.item_revision,
      pohc.hazard_class,
      poun.un_number,
      rsl.line_num,
      rsl.category_id AS item_category_id,
      rsl.locator_id,
      CASE WHEN rsl.source_document_code = 'REQ' THEN reql.need_by_date ELSE NULL END AS need_by_date,
      rsl.packing_slip AS rsl_packing_slip,
      rsl.quantity_received,
      rsl.quantity_shipped,
      rsl.requisition_line_id, /* kv */
      reqh.requisition_header_id, /* kv */
      CASE WHEN rsl.source_document_code = 'REQ' THEN reqh.segment1 ELSE NULL END AS order_num,
      CASE WHEN rsl.source_document_code = 'REQ' THEN reql.line_num ELSE NULL END AS ORDER_LINE_NUM,
      rsl.req_distribution_id, /* kv */
      rsl.shipment_header_id,
      rsl.shipment_line_id,
      rsl.shipment_line_status_code,
      rsl.source_document_code,
      polc2.meaning AS source_document_type,
      rsl.to_organization_id,
      rsl.to_subinventory,
      rsl.transfer_cost,
      rsl.transportation_account_id,
      rsl.transportation_cost,
      rsl.unit_of_measure,
      rsl.uom_conversion_rate,
      rsl.routing_header_id,
      rrh.routing_name,
      rsl.reason_id,
      mtr.reason_name,
      hl.location_code,
      he.full_name AS deliver_to_person,
      NULL AS PO_HEADER_ID, /* to_number(NULL), */
      NULL AS PO_LINE_ID, /* to_number(NULL), */
      NULL AS PO_LINE_LOCATION_ID, /* to_number(NULL), */
      NULL AS PO_RELEASE_ID, /* to_number(NULL), */
      NULL AS RELEASE_NUM, /*  to_number(NULL), */
      NULL AS VENDOR_NAME,
      NULL AS VENDOR_SITE_CODE, /* povs */
      NULL AS SHIP_TO_LOCATION_ID, /* to_number(NULL), */
      NULL AS PRIMARY_UNIT_OF_MEASURE,
      rsh.vendor_id,
      NULL AS BAR_CODE_LABEL,
      NULL AS TRUCK_NUM,
      NULL AS CONTAINER_NUM,
      NULL AS VENDOR_LOT_NUM,
      rsl.secondary_quantity_received,
      rsl.secondary_quantity_shipped,
      rsl.secondary_unit_of_measure,
      rsl.qc_grade,
      reql.org_id,
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
      ) || '-' || COALESCE(rsh.vendor_id, 0) || '-' || COALESCE(RSL.SHIPMENT_HEADER_ID, 0) || '-' || COALESCE(RSL.SHIPMENT_LINE_ID, '0') || '-' || COALESCE(rsl.routing_header_id, '0') AS dw_load_id,
      CURRENT_TIMESTAMP() AS dw_insert_date,
      CURRENT_TIMESTAMP() AS dw_update_date
    FROM (
      SELECT
        *
      FROM silver_bec_ods.rcv_shipment_headers
      WHERE
        is_deleted_flg <> 'Y'
    ) AS rsh, (
      SELECT
        *
      FROM silver_bec_ods.rcv_shipment_lines
      WHERE
        is_deleted_flg <> 'Y'
    ) AS rsl, (
      SELECT
        *
      FROM silver_bec_ods.fnd_lookup_values
      WHERE
        is_deleted_flg <> 'Y'
    ) AS polc, (
      SELECT
        *
      FROM silver_bec_ods.fnd_lookup_values
      WHERE
        is_deleted_flg <> 'Y'
    ) AS polc2, (
      SELECT
        *
      FROM silver_bec_ods.po_requisition_lines_all
      WHERE
        is_deleted_flg <> 'Y'
    ) AS reql, (
      SELECT
        *
      FROM silver_bec_ods.po_requisition_headers_all
      WHERE
        is_deleted_flg <> 'Y'
    ) AS reqh, (
      SELECT
        *
      FROM silver_bec_ods.rcv_routing_headers
      WHERE
        is_deleted_flg <> 'Y'
    ) AS rrh, (
      SELECT
        *
      FROM silver_bec_ods.mtl_transaction_reasons
      WHERE
        is_deleted_flg <> 'Y'
    ) AS mtr, (
      SELECT
        *
      FROM silver_bec_ods.po_hazard_classes_tl
      WHERE
        is_deleted_flg <> 'Y'
    ) AS pohc, (
      SELECT
        *
      FROM silver_bec_ods.po_un_numbers_tl
      WHERE
        is_deleted_flg <> 'Y'
    ) AS poun, (
      SELECT
        *
      FROM silver_bec_ods.mtl_system_items_b
      WHERE
        is_deleted_flg <> 'Y'
    ) AS msi, (
      SELECT
        *
      FROM silver_bec_ods.hr_locations_all_tl
      WHERE
        is_deleted_flg <> 'Y'
    ) AS hl, (
      SELECT
        *
      FROM silver_bec_ods.per_all_people_f
      WHERE
        is_deleted_flg <> 'Y'
    ) AS he
    WHERE
      1 = 1
      AND rsh.shipment_header_id = rsl.shipment_header_id
      AND rsl.source_document_code IN ('INVENTORY', 'REQ')
      AND polc.lookup_code = rsl.destination_type_code
      AND polc.lookup_type = 'SHIPMENT SOURCE DOCUMENT TYPE'
      AND polc2.lookup_code = rsl.source_document_code
      AND polc2.lookup_type = 'SHIPMENT SOURCE DOCUMENT TYPE'
      AND reql.REQUISITION_LINE_ID() = CASE WHEN rsl.source_document_code = 'REQ' THEN rsl.requisition_line_id ELSE -1 END
      AND reqh.REQUISITION_HEADER_ID() = reql.requisition_header_id
      AND rrh.ROUTING_HEADER_ID() = COALESCE(rsl.routing_header_id, -1)
      AND mtr.REASON_ID() = COALESCE(rsl.reason_id, -1)
      AND hl.LOCATION_ID() = COALESCE(rsl.deliver_to_location_id, -1)
      AND hl.LANGUAGE() = 'US' /* userenv('LANG') */
      AND he.PERSON_ID() = COALESCE(rsl.deliver_to_person_id, -1)
      AND FLOOR(CURRENT_TIMESTAMP()) BETWEEN he.EFFECTIVE_START_DATE() AND he.EFFECTIVE_END_DATE()
      AND msi.ORGANIZATION_ID() = rsl.to_organization_id
      AND msi.INVENTORY_ITEM_ID() = rsl.item_id
      AND msi.hazard_class_id = pohc.HAZARD_CLASS_ID()
      AND msi.un_number_id = poun.UN_NUMBER_ID() /* AND nvl(reql.org_id, - 99) = nvl(reqh.org_id, - 99) */
    UNION
    SELECT
      'ASN' AS SOURCE_TYPE,
      rsl.created_by,
      rsl.creation_date AS RSL_creation_date,
      rsl.last_updated_by,
      rsl.last_update_date,
      rsl.last_update_login,
      rsl.request_id,
      rsl.program_application_id,
      rsl.program_id,
      rsl.program_update_date,
      rsh.asn_type,
      rsh.packing_slip AS rsh_packing_slip,
      rsh.shipment_num,
      rsh.waybill_airbill_num,
      rsh.receipt_num,
      rsh.creation_date AS RSH_creation_date,
      rsh.shipped_date,
      rsh.expected_receipt_date,
      rsh.comments AS rsh_comments,
      rsh.bill_of_lading,
      rsh.freight_carrier_code,
      rsh.num_of_containers,
      rsh.receipt_source_code,
      rsl.charge_account_id,
      rsl.comments AS rsl_comments,
      rsl.deliver_to_location_id,
      rsl.deliver_to_person_id,
      rsl.destination_context,
      rsl.destination_type_code,
      NULL AS destination_type, /* polc.po_type_display    destination_type, */
      rsl.employee_id,
      rsl.from_organization_id,
      rsl.item_description,
      rsl.item_id,
      rsl.item_revision,
      pohc.hazard_class,
      poun.un_number,
      rsl.line_num,
      rsl.category_id AS item_category_id,
      rsl.locator_id,
      CASE WHEN rsl.source_document_code = 'PO' THEN poll.need_by_date ELSE NULL END AS need_by_date,
      rsl.packing_slip AS rsl_packing_slip,
      rsl.quantity_received,
      rsl.quantity_shipped,
      NULL AS requisition_line_id, /* to_number('NULL'), */
      NULL AS requisition_header_id, /* to_number('NULL'), */
      CASE WHEN rsl.source_document_code = 'PO' THEN poh.segment1 ELSE NULL END AS order_num,
      CASE WHEN rsl.source_document_code = 'PO' THEN pol.line_num ELSE NULL END AS ORDER_LINE_NUM,
      NULL AS req_distribution_id, /*  to_number('NULL'), */
      rsl.shipment_header_id,
      rsl.shipment_line_id,
      rsl.shipment_line_status_code,
      rsl.source_document_code,
      polc2.meaning AS source_document_type,
      rsl.to_organization_id,
      rsl.to_subinventory,
      rsl.transfer_cost,
      rsl.transportation_account_id,
      rsl.transportation_cost,
      rsl.unit_of_measure,
      rsl.uom_conversion_rate,
      rsl.routing_header_id,
      rrh.routing_name,
      rsl.reason_id,
      mtr.reason_name,
      hl.location_code,
      he.full_name AS deliver_to_person,
      rsl.po_header_id, /* -kv */
      rsl.po_line_id,
      rsl.po_line_location_id,
      rsl.po_release_id,
      por.release_num, /* -kv */
      pov.vendor_name,
      povs.vendor_site_code, /* kv */
      rsh.ship_to_location_id, /* kv */
      rsl.primary_unit_of_measure,
      rsh.vendor_id,
      rsl.bar_code_label,
      rsl.truck_num,
      rsl.container_num,
      rsl.vendor_lot_num,
      rsl.secondary_quantity_received,
      rsl.secondary_quantity_shipped,
      rsl.secondary_unit_of_measure,
      rsl.qc_grade,
      poll.org_id,
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
      ) || '-' || COALESCE(rsh.vendor_id, 0) || '-' || COALESCE(RSL.SHIPMENT_HEADER_ID, 0) || '-' || COALESCE(RSL.SHIPMENT_LINE_ID, '0') || '-' || COALESCE(rsl.routing_header_id, '0') AS dw_load_id,
      CURRENT_TIMESTAMP() AS dw_insert_date,
      CURRENT_TIMESTAMP() AS dw_update_date
    FROM (
      SELECT
        *
      FROM silver_bec_ods.rcv_shipment_headers
      WHERE
        is_deleted_flg <> 'Y'
    ) AS rsh, (
      SELECT
        *
      FROM silver_bec_ods.rcv_shipment_lines
      WHERE
        is_deleted_flg <> 'Y'
    ) AS rsl, (
      SELECT
        *
      FROM silver_bec_ods.fnd_lookup_values
      WHERE
        is_deleted_flg <> 'Y'
    ) AS polc, (
      SELECT
        *
      FROM silver_bec_ods.fnd_lookup_values
      WHERE
        is_deleted_flg <> 'Y'
    ) AS polc2, (
      SELECT
        *
      FROM silver_bec_ods.po_headers_all
      WHERE
        is_deleted_flg <> 'Y'
    ) AS poh, (
      SELECT
        *
      FROM silver_bec_ods.po_lines_all
      WHERE
        is_deleted_flg <> 'Y'
    ) AS pol, (
      SELECT
        *
      FROM silver_bec_ods.po_line_locations_all
      WHERE
        is_deleted_flg <> 'Y'
    ) AS poll, (
      SELECT
        *
      FROM silver_bec_ods.po_releases_all
      WHERE
        is_deleted_flg <> 'Y'
    ) AS por, (
      SELECT
        *
      FROM silver_bec_ods.po_vendors
      WHERE
        is_deleted_flg <> 'Y'
    ) AS pov, (
      SELECT
        *
      FROM silver_bec_ods.ap_supplier_sites_all
      WHERE
        is_deleted_flg <> 'Y'
    ) AS povs, (
      SELECT
        *
      FROM silver_bec_ods.rcv_routing_headers
      WHERE
        is_deleted_flg <> 'Y'
    ) AS rrh, (
      SELECT
        *
      FROM silver_bec_ods.mtl_transaction_reasons
      WHERE
        is_deleted_flg <> 'Y'
    ) AS mtr, (
      SELECT
        *
      FROM silver_bec_ods.po_hazard_classes_tl
      WHERE
        is_deleted_flg <> 'Y'
    ) AS pohc, (
      SELECT
        *
      FROM silver_bec_ods.po_un_numbers_tl
      WHERE
        is_deleted_flg <> 'Y'
    ) AS poun, (
      SELECT
        *
      FROM silver_bec_ods.hr_locations_all_tl
      WHERE
        is_deleted_flg <> 'Y'
    ) AS hl, (
      SELECT
        *
      FROM silver_bec_ods.per_all_people_f
      WHERE
        is_deleted_flg <> 'Y'
    ) AS he
    WHERE
      1 = 1
      AND COALESCE(poll.approved_flag, 'N') = 'Y'
      AND COALESCE(poll.cancel_flag, 'N') = 'N'
      AND COALESCE(poll.closed_code, 'OPEN') <> 'FINALLY CLOSED'
      AND polc.lookup_code = rsl.destination_type_code
      AND polc2.lookup_code = rsl.source_document_code
      AND polc2.lookup_type = 'SHIPMENT SOURCE DOCUMENT TYPE'
      AND polc.lookup_type = 'RCV DESTINATION TYPE'
      AND rsl.source_document_code = 'PO'
      AND rsl.po_header_id = poh.po_header_id
      AND rsl.po_line_id = pol.po_line_id
      AND pol.hazard_class_id = pohc.HAZARD_CLASS_ID()
      AND pol.un_number_id = poun.UN_NUMBER_ID()
      AND rsl.po_line_location_id = poll.line_location_id
      AND rsl.po_release_id = por.PO_RELEASE_ID()
      AND rrh.ROUTING_HEADER_ID() = COALESCE(rsl.routing_header_id, -1)
      AND mtr.REASON_ID() = COALESCE(rsl.reason_id, -1)
      AND hl.LOCATION_ID() = COALESCE(rsl.deliver_to_location_id, -1)
      AND hl.LANGUAGE() = 'US' /* userenv('LANG') */
      AND he.PERSON_ID() = COALESCE(rsl.deliver_to_person_id, -1)
      AND FLOOR(CURRENT_TIMESTAMP()) BETWEEN he.EFFECTIVE_START_DATE() AND he.EFFECTIVE_END_DATE()
      AND pov.VENDOR_ID() = rsh.vendor_id
      AND povs.VENDOR_SITE_ID() = rsh.vendor_site_id
      AND rsh.asn_type IN ('ASN', 'ASBN')
      AND rsl.shipment_line_status_code IN ('EXPECTED', 'PARTIALLY RECEIVED', 'FULLY RECEIVED')
      AND rsh.shipment_header_id = rsl.shipment_header_id
  ) AS shp
);
UPDATE bec_etl_ctrl.batch_dw_info SET load_type = 'I', last_refresh_date = CURRENT_TIMESTAMP()
WHERE
  dw_table_name = 'fact_po_shipment_stg' AND batch_name = 'po';