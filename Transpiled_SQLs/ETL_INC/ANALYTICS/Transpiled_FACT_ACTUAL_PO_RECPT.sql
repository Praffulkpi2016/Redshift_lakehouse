DROP table IF EXISTS gold_bec_dwh.FACT_ACTUAL_PO_RECPT;
CREATE TABLE gold_bec_dwh.FACT_ACTUAL_PO_RECPT AS
(
  SELECT
    a.`WIP_ENTITY_NAME`,
    a.`PO_TYPE`,
    a.`PO_NUMBER`,
    a.`PR_APPROVED_DATE`,
    a.`REACTION_TIME`,
    a.`PO_PLACED_ONTIME`,
    a.`BUYER_NAME`,
    a.`PO_RELEASE_NUMBER`,
    a.`PO_SHIPMENT_NUM`,
    a.`PO_DATE`,
    a.po_last_update_date,
    a.processing_leadtime,
    a.`PO_LINE_NUM`,
    a.`ITEM_NAME`,
    a.`ITEM_DESCRIPTION`,
    a.`LINE_QUANTITY`,
    a.`QUANTITY_SHIPPED`,
    a.`PO_UNIT_OF_MEASURE`,
    a.`PRIMARY_UOM_CODE`,
    a.`PRIMARY_UNIT_OF_MEASURE`,
    a.`NEED_BY_DATE`,
    a.`PROMISED_DATE`,
    a.`RCV_QUANTITY_RECEIVED`,
    a.primary_quantity,
    a.`PO_UNIT_PRICE`,
    a.`EXTENDED_PO_RCV_PRICE`,
    a.`RECEIPT_NUMBER`,
    a.`PACKING_SLIP`,
    a.`SHIPMENT_NUM`,
    a.waybill_airbill_num,
    a.bill_of_lading,
    a.`SHIPMENT_DATE`,
    a.`VENDOR_NUMBER`,
    a.`VENDOR_NAME`,
    a.`CLOSED_CODE`,
    a.`PO_CURRENCY_CODE`,
    a.`SHIPMENT_HEADER_ID`,
    'N' AS IS_DELETED_FLG,
    (
      SELECT
        system_id
      FROM bec_etl_ctrl.etlsourceappid
      WHERE
        source_system = 'EBS'
    ) || '-' || a.SHIPMENT_HEADER_ID AS SHIPMENT_HEADER_ID_KEY,
    a.`SHIPMENT_LINE_ID`,
    (
      SELECT
        system_id
      FROM bec_etl_ctrl.etlsourceappid
      WHERE
        source_system = 'EBS'
    ) || '-' || a.SHIPMENT_LINE_ID AS SHIPMENT_LINE_ID_KEY,
    a.`PO_LINE_TYPE`,
    a.`LINE_LOCATION_ID`,
    (
      SELECT
        system_id
      FROM bec_etl_ctrl.etlsourceappid
      WHERE
        source_system = 'EBS'
    ) || '-' || a.LINE_LOCATION_ID AS LINE_LOCATION_ID_KEY,
    a.`UNIT_OF_MEASURE`,
    a.`PO_CONVERSION`,
    a.`RCV_CONVERSION`,
    a.`ASN_TYPE`,
    a.`ASN_STATUS`,
    CAST(a.RECEIPT_DATE AS TIMESTAMP),
    a.`STD_ITEM_COST`,
    a.`MATERIAL_COST`,
    a.ext_material_cost,
    a.purchase_price_variance,
    a.`OUTSIDE_PROCESSING_COST`,
    a.`OVERHEAD_COST`,
    a.`MATERIAL_OVERHEAD_COST`,
    a.`RESOURCE_COST`,
    a.`FOB_CODE`,
    a.`SHIP_TO_LOCATION`,
    a.`SHIP_TO_ORGANIZATION`,
    a.organization_code,
    a.organization_id,
    COALESCE(b.vmi_flag, 'N') AS vmi_flag,
    (
      SELECT
        system_id
      FROM bec_etl_ctrl.etlsourceappid
      WHERE
        source_system = 'EBS'
    ) || '-' || a.organization_id AS organization_id_KEY,
    (
      SELECT
        system_id
      FROM bec_etl_ctrl.etlsourceappid
      WHERE
        source_system = 'EBS'
    ) || '-' || ood.operating_unit AS org_id_KEY,
    ood.operating_unit AS org_id,
    a.`CATEGORY`,
    a.`ITEM_REVISION`,
    a.`ATTRIBUTE10`,
    a.`ACTUAL_COMMIT_NEED_DATE`,
    a.`FREIGHT_TERMS`,
    a.`CVMI_FLAG`,
    a.`VENDOR_COUNTRY`,
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
    ) || '-' || COALESCE(a.po_number, '0') || '-' || COALESCE(a.po_line_num, 0) || '-' || COALESCE(a.line_location_id, 0) || '-' || COALESCE(a.shipment_header_id, 0) || '-' || COALESCE(a.shipment_line_id, 0) || '-' || COALESCE(a.wip_entity_name, '0') || '-' || COALESCE(DATE_FORMAT(receipt_date, 'yyyyMMdd'), 'NA') AS dw_load_id,
    CURRENT_TIMESTAMP() AS dw_insert_date,
    CURRENT_TIMESTAMP() AS dw_update_date
  FROM (
    SELECT
      *
    FROM silver_bec_ods.bec_actual_po_recpt1
    WHERE
      is_deleted_flg <> 'Y'
  ) AS a, (
    SELECT
      *
    FROM silver_bec_ods.po_line_locations_all
    WHERE
      is_deleted_flg <> 'Y'
  ) AS b, (
    SELECT
      *
    FROM silver_bec_ods.org_organization_definitions
    WHERE
      is_deleted_flg <> 'Y'
  ) AS ood
  WHERE
    a.line_location_id = b.LINE_LOCATION_ID() AND a.organization_id = ood.organization_id
);
UPDATE bec_etl_ctrl.batch_dw_info SET load_type = 'I', last_refresh_date = CURRENT_TIMESTAMP()
WHERE
  dw_table_name = 'fact_actual_po_recpt' AND batch_name = 'po';