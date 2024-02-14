DROP table IF EXISTS gold_bec_dwh.FACT_CSF_DEBRIEF_LINES;
CREATE TABLE gold_bec_dwh.FACT_CSF_DEBRIEF_LINES AS
SELECT
  cdl.channel_code,
  cdl.debrief_line_id,
  cdl.debrief_header_id,
  cdl.service_date,
  cdl.inventory_item_id,
  COALESCE(cdl.issuing_inventory_org_id, cdl.receiving_inventory_org_id) AS inventory_org_id,
  COALESCE(cdl.issuing_sub_inventory_code, cdl.receiving_sub_inventory_code) AS sub_inventory_code,
  COALESCE(cdl.issuing_locator_id, cdl.receiving_locator_id) AS LOCATOR,
  msibk.segment1 AS item,
  msibk.description,
  cdl.uom_code,
  cdl.parent_product_id,
  cdl.removed_product_id,
  cdl.status_of_received_part,
  cdl.quantity,
  cdl.item_revision,
  cdl.item_lotnumber,
  cdl.item_serial_number,
  cdl.created_by,
  cdl.creation_date,
  cdl.last_updated_by,
  cdl.last_update_date,
  cdl.last_update_login,
  cdl.attribute1,
  cdl.attribute2,
  cdl.attribute3,
  cdl.attribute4,
  cdl.attribute5,
  cdl.attribute6,
  cdl.attribute7,
  cdl.attribute8,
  cdl.attribute9,
  cdl.attribute10,
  cdl.attribute11,
  cdl.attribute12,
  cdl.attribute13,
  cdl.attribute14,
  cdl.attribute15,
  cdl.attribute_category,
  cdl.material_reason_code,
  cdl.business_process_id,
  cdl.transaction_type_id,
  ctb.line_order_category_code,
  fl.meaning AS material_meaning,
  cdl.ib_update_status,
  cdl.ib_update_msg_code,
  cdl.ib_update_message,
  cdl.spare_update_status,
  cdl.spare_update_msg_code,
  cdl.spare_update_message,
  cdl.charge_upload_status,
  cdl.charge_upload_msg_code,
  cdl.charge_upload_message,
  cdl.return_reason_code,
  arc.meaning AS return_reason,
  cdl.return_date,
  cdl.error_text,
  cdl.material_transaction_id,
  cdl.usage_type,
  cdl.return_subinventory_name,
  cdl.return_organization_id,
  cdl.carrier_code,
  cdl.shipping_method,
  cdl.shipping_number,
  cdl.waybill,
  (
    SELECT
      system_id
    FROM bec_etl_ctrl.etlsourceappid
    WHERE
      source_system = 'EBS'
  ) || '-' || cdl.debrief_line_id AS debrief_line_id_KEY,
  (
    SELECT
      system_id
    FROM bec_etl_ctrl.etlsourceappid
    WHERE
      source_system = 'EBS'
  ) || '-' || cdl.debrief_header_id AS debrief_header_id_KEY,
  (
    SELECT
      system_id
    FROM bec_etl_ctrl.etlsourceappid
    WHERE
      source_system = 'EBS'
  ) || '-' || cdl.inventory_item_id AS inventory_item_id_KEY,
  (
    SELECT
      system_id
    FROM bec_etl_ctrl.etlsourceappid
    WHERE
      source_system = 'EBS'
  ) || '-' || cdl.transaction_type_id AS transaction_type_id_KEY,
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
  ) || '-' || COALESCE(cdl.debrief_header_id, 0) || '-' || COALESCE(cdl.debrief_line_id, 0) AS dw_load_id,
  CURRENT_TIMESTAMP() AS dw_insert_date,
  CURRENT_TIMESTAMP() AS dw_update_date
FROM (
  SELECT
    *
  FROM silver_bec_ods.csf_debrief_lines
  WHERE
    is_deleted_flg <> 'Y'
) AS cdl, (
  SELECT
    *
  FROM silver_bec_ods.CS_TRANSACTION_TYPES_B
  WHERE
    is_deleted_flg <> 'Y'
) AS ctb, (
  SELECT
    *
  FROM silver_bec_ods.fnd_lookup_values
  WHERE
    is_deleted_flg <> 'Y'
) AS fl, (
  SELECT
    *
  FROM silver_bec_ods.fnd_lookup_values
  WHERE
    is_deleted_flg <> 'Y'
) AS arc, (
  SELECT
    *
  FROM silver_bec_ods.cs_billing_type_categories
  WHERE
    is_deleted_flg <> 'Y'
) AS cbtc, (
  SELECT
    *
  FROM silver_bec_ods.mtl_system_items_b
  WHERE
    is_deleted_flg <> 'Y'
) AS msibk
WHERE
  1 = 1
  AND ctb.TRANSACTION_TYPE_ID() = cdl.transaction_type_id
  AND msibk.material_billable_flag = cbtc.billing_type
  AND cbtc.billing_category = 'M'
  AND msibk.inventory_item_id = cdl.inventory_item_id
  AND msibk.organization_id = COALESCE(cdl.issuing_inventory_org_id, cdl.receiving_inventory_org_id)
  AND fl.LOOKUP_TYPE() = 'CSF_MATERIAL_REASON'
  AND cdl.material_reason_code = fl.LOOKUP_CODE()
  AND cdl.return_reason_code = arc.LOOKUP_CODE()
  AND arc.LOOKUP_TYPE() = 'CREDIT_MEMO_REASON';
UPDATE bec_etl_ctrl.batch_dw_info SET load_type = 'I', last_refresh_date = CURRENT_TIMESTAMP()
WHERE
  dw_table_name = 'fact_csf_debrief_lines' AND batch_name = 'inv';