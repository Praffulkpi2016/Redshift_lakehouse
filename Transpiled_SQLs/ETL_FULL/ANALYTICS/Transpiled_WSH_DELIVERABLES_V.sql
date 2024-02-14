DROP TABLE IF EXISTS silver_bec_ods.WSH_DELIVERABLES_V;
CREATE TABLE silver_bec_ods.wsh_deliverables_v AS
SELECT
  NULL AS TRIP_ID,
  NULL AS STOP_ID,
  CASE
    WHEN wdd.container_flag = 'Y'
    THEN 'C'
    WHEN wdd.container_flag = 'C'
    THEN 'C'
    ELSE 'L'
  END AS TYPE, /* wdd.rowid, */
  wdd.delivery_detail_id AS DELIVERY_DETAIL_ID,
  CAST((
    CASE WHEN wdd.container_flag = 'N' THEN wdd.delivery_detail_id ELSE NULL END
  ) AS INT) AS DELIVERY_LINE_ID,
  CAST((
    CASE
      WHEN wdd.container_flag = 'Y'
      THEN wdd.delivery_detail_id
      WHEN wdd.container_flag = 'C'
      THEN wdd.delivery_detail_id
      ELSE NULL
    END
  ) AS INT) AS CONTAINER_INSTANCE_ID,
  wdd.container_name,
  wda.delivery_id,
  wdd.source_code,
  flv_source.meaning AS SOURCE_NAME,
  wdd.source_header_id,
  wdd.source_line_id,
  wdd.source_header_number,
  wdd.source_header_type_id,
  wdd.source_header_type_name,
  wdd.source_line_number,
  wdd.src_requested_quantity,
  wdd.src_requested_quantity_uom,
  wdd.customer_id,
  wdd.sold_to_contact_id,
  wdd.inventory_item_id,
  wdd.item_description,
  wdd.country_of_origin,
  wdd.classification,
  wdd.ship_from_location_id,
  wdd.ship_to_location_id,
  wdd.ship_to_contact_id,
  wdd.deliver_to_location_id,
  wdd.deliver_to_contact_id,
  wdd.intmed_ship_to_location_id,
  wdd.intmed_ship_to_contact_id,
  wdd.ship_tolerance_above,
  wdd.ship_tolerance_below,
  wdd.requested_quantity,
  wdd.shipped_quantity,
  wdd.delivered_quantity,
  wdd.cancelled_quantity,
  wdd.requested_quantity_uom,
  wdd.subinventory,
  wdd.revision,
  wdd.lot_number,
  wdd.customer_requested_lot_flag,
  wdd.serial_number,
  wdd.locator_id,
  wdd.date_requested,
  wdd.date_scheduled,
  wdd.master_container_item_id,
  wdd.detail_container_item_id,
  wdd.load_seq_number,
  wdd.ship_method_code,
  wdd.carrier_id,
  wdd.freight_terms_code,
  wdd.shipment_priority_code,
  wdd.fob_code,
  wdd.customer_item_id,
  wdd.dep_plan_required_flag,
  wdd.customer_prod_seq,
  wdd.customer_dock_code,
  wdd.unit_weight,
  wdd.unit_volume,
  wdd.wv_frozen_flag,
  wdd.gross_weight,
  (
    wdd.gross_weight - COALESCE(wdd.net_weight, 0)
  ) AS TARE_WEIGHT,
  wdd.net_weight,
  wdd.weight_uom_code,
  wdd.filled_volume,
  wdd.volume,
  wdd.volume_uom_code,
  wdd.fill_percent,
  wdd.maximum_load_weight,
  wdd.maximum_volume,
  wdd.minimum_fill_percent,
  wdd.mvt_stat_status,
  wdd.released_status,
  CASE
    WHEN wdd.released_status = 'X'
    THEN CASE
      WHEN wlpn.lpn_context = '9'
      THEN flv_wms.meaning
      WHEN wlpn.lpn_context = '11'
      THEN flv_wms.meaning
      WHEN wlpn.lpn_context = '12'
      THEN flv_wms.meaning
      ELSE (
        SELECT
          meaning
        FROM silver_bec_ods.fnd_lookup_values AS flv_released
        WHERE
          flv_released.lookup_type = 'PICK_STATUS'
          AND flv_released.lookup_code = wdd.released_status
          AND flv_released.LANGUAGE = 'US'
          AND flv_released.VIEW_APPLICATION_ID = 665
          AND flv_released.SECURITY_GROUP_ID = 0
      )
    END
    ELSE (
      SELECT
        meaning
      FROM silver_bec_ods.fnd_lookup_values AS flv_released
      WHERE
        flv_released.lookup_type = 'PICK_STATUS'
        AND flv_released.lookup_code = wdd.released_status
        AND flv_released.LANGUAGE = 'US'
        AND flv_released.VIEW_APPLICATION_ID = 665
        AND flv_released.SECURITY_GROUP_ID = 0
    )
  END AS RELEASED_STATUS_NAME,
  wdd.organization_id,
  wdd.transaction_temp_id,
  wdd.ship_set_id,
  wdd.arrival_set_id,
  wdd.ship_model_complete_flag,
  wdd.top_model_line_id,
  wdd.hold_code,
  wdd.cust_po_number,
  wdd.ato_line_id,
  wdd.move_order_line_id,
  wdd.hazard_class_id,
  wdd.quality_control_quantity,
  wdd.cycle_count_quantity,
  wdd.tracking_number,
  wdd.movement_id,
  wdd.shipping_instructions,
  wdd.packing_instructions,
  wdd.project_id,
  wdd.task_id,
  wdd.org_id,
  wdd.oe_interfaced_flag,
  wdd.split_from_delivery_detail_id,
  wdd.inv_interfaced_flag,
  wda.parent_delivery_detail_id,
  wdd.master_serial_number,
  wdd.container_flag,
  wdd.container_type_code,
  wdd.seal_code,
  NULL AS ACTIVITY_CODE,
  wdd.attribute_category,
  wdd.attribute1,
  wdd.attribute2,
  wdd.attribute3,
  wdd.attribute4,
  wdd.attribute5,
  wdd.attribute6,
  wdd.attribute7,
  wdd.attribute8,
  wdd.attribute9,
  wdd.attribute10,
  wdd.attribute11,
  wdd.attribute12,
  wdd.attribute13,
  wdd.attribute14,
  wdd.attribute15,
  wdd.tp_attribute_category,
  wdd.tp_attribute1,
  wdd.tp_attribute2,
  wdd.tp_attribute3,
  wdd.tp_attribute4,
  wdd.tp_attribute5,
  wdd.tp_attribute6,
  wdd.tp_attribute7,
  wdd.tp_attribute8,
  wdd.tp_attribute9,
  wdd.tp_attribute10,
  wdd.tp_attribute11,
  wdd.tp_attribute12,
  wdd.tp_attribute13,
  wdd.tp_attribute14,
  wdd.tp_attribute15,
  wdd.creation_date,
  wdd.created_by,
  wdd.last_update_date,
  wdd.last_updated_by,
  wdd.last_update_login,
  wdd.program_application_id,
  wdd.program_id,
  wdd.program_update_date,
  wdd.request_id,
  wdd.preferred_grade,
  wdd.shipped_quantity2,
  wdd.delivered_quantity2,
  wdd.cancelled_quantity2,
  wdd.requested_quantity2,
  wdd.requested_quantity_uom2,
  wdd.src_requested_quantity2,
  wdd.src_requested_quantity_uom2,
  wdd.quality_control_quantity2,
  wdd.cycle_count_quantity2,
  wdd.unit_number,
  wdd.unit_price,
  wdd.currency_code,
  wdd.freight_class_cat_id,
  wdd.commodity_code_cat_id,
  wdd.inspection_flag,
  wdd.pickable_flag,
  wdd.ship_to_site_use_id,
  wdd.deliver_to_site_use_id,
  wdd.to_serial_number,
  wdd.picked_quantity,
  wdd.picked_quantity2,
  wdd.customer_production_line,
  wdd.customer_job,
  wdd.cust_model_serial_number,
  wdd.received_quantity,
  wdd.received_quantity2,
  wdd.source_line_set_id,
  wdd.batch_id,
  wdd.shipment_batch_id,
  wdd.lpn_id,
  wdd.original_subinventory,
  wdd.service_level,
  wdd.mode_of_transport,
  wdd.ignore_for_planning,
  COALESCE(wdd.line_direction, 'O') AS LINE_DIRECTION,
  wdd.party_id,
  wdd.routing_req_id,
  wdd.shipping_control,
  wdd.source_blanket_reference_id,
  wdd.source_blanket_reference_num,
  wdd.po_shipment_line_id,
  wdd.po_shipment_line_number,
  wdd.returned_quantity,
  wdd.returned_quantity2,
  wdd.source_line_type_code,
  wdd.rcv_shipment_line_id,
  wdd.supplier_item_number,
  wdd.vendor_id,
  wdd.ship_from_site_id,
  wdd.earliest_pickup_date,
  wdd.latest_pickup_date,
  wdd.earliest_dropoff_date,
  wdd.latest_dropoff_date,
  wdd.request_date_type_code,
  wdd.tp_delivery_detail_id,
  wdd.source_document_type_id,
  wdd.po_revision_number,
  wdd.release_revision_number,
  wdd.replenishment_status,
  wdd.reference_number,
  wdd.reference_line_number,
  wdd.reference_line_quantity,
  wdd.reference_line_quantity_uom,
  wdd.original_locator_id,
  wdd.original_revision,
  wdd.original_lot_number,
  wdd.client_id,
  wdd.consignee_flag,
  wdd.equipment_id,
  wdd.mcc_code,
  wdd.tms_sub_batch_id
FROM (
  SELECT
    *
  FROM silver_bec_ods.wsh_delivery_details
  WHERE
    is_deleted_flg <> 'Y'
) AS wdd, (
  SELECT
    *
  FROM silver_bec_ods.wsh_delivery_assignments
  WHERE
    is_deleted_flg <> 'Y'
) AS wda, (
  SELECT
    *
  FROM silver_bec_ods.fnd_lookup_values
  WHERE
    is_deleted_flg <> 'Y'
) AS flv_source, (
  SELECT
    *
  FROM silver_bec_ods.wms_license_plate_numbers
  WHERE
    is_deleted_flg <> 'Y'
) AS wlpn, (
  SELECT
    *
  FROM silver_bec_ods.fnd_lookup_values
  WHERE
    is_deleted_flg <> 'Y'
) AS flv_wms
WHERE
  wda.delivery_detail_id = wdd.delivery_detail_id
  AND flv_source.lookup_type = 'SOURCE_SYSTEM'
  AND flv_source.lookup_code = wdd.source_code
  AND COALESCE(wdd.line_direction, 'O') IN ('O', 'IO')
  AND flv_source.LANGUAGE = 'US'
  AND flv_source.VIEW_APPLICATION_ID = 665
  AND flv_source.SECURITY_GROUP_ID = 0
  AND flv_wms.LOOKUP_TYPE() = 'WMS_LPN_CONTEXT'
  AND flv_wms.LANGUAGE() = 'US'
  AND wdd.lpn_id = wlpn.LPN_ID()
  AND flv_wms.LOOKUP_CODE() = CAST(wlpn.lpn_context AS STRING)
  AND flv_wms.VIEW_APPLICATION_ID() = 700
  AND flv_wms.SECURITY_GROUP_ID() = 0
  AND COALESCE(wda.type, 'S') IN ('S', 'C');
UPDATE bec_etl_ctrl.batch_dw_info SET load_type = 'I', last_refresh_date = CURRENT_TIMESTAMP()
WHERE
  dw_table_name = 'wsh_deliverables_v';