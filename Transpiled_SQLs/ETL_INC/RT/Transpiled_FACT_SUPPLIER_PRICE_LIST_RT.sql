TRUNCATE table gold_bec_dwh_rpt.FACT_SUPPLIER_PRICE_LIST_RT;
WITH item_category AS (
  SELECT
    mc.segment1,
    mc.segment2,
    mic.inventory_item_id,
    mic.organization_id,
    (
      CASE
        WHEN STRUCTURE_ID = 3
        THEN SEGMENT1
        WHEN STRUCTURE_ID = 4
        THEN SEGMENT1
        WHEN STRUCTURE_ID = 5
        THEN SEGMENT1
        WHEN STRUCTURE_ID = 6
        THEN SEGMENT1
        WHEN STRUCTURE_ID = 101
        THEN SEGMENT3 || '.' || SEGMENT4 || '.' || SEGMENT1 || '.' || SEGMENT2 || '.' || SEGMENT5 || '.' || SEGMENT6
        WHEN STRUCTURE_ID = 201
        THEN SEGMENT1
        WHEN STRUCTURE_ID = 50136
        THEN SEGMENT1 || '.' || SEGMENT2 || '.' || SEGMENT3
        WHEN STRUCTURE_ID = 50152
        THEN SEGMENT1
        WHEN STRUCTURE_ID = 50153
        THEN SEGMENT1
        WHEN STRUCTURE_ID = 50168
        THEN SEGMENT1 || '.' || SEGMENT2
        WHEN STRUCTURE_ID = 50169
        THEN SEGMENT1
        WHEN STRUCTURE_ID = 50190
        THEN SEGMENT1 || '.' || SEGMENT2 || '.' || SEGMENT3 || '.' || SEGMENT4
        WHEN STRUCTURE_ID = 50208
        THEN SEGMENT1
        WHEN STRUCTURE_ID = 50229
        THEN SEGMENT1
        WHEN STRUCTURE_ID = 50268
        THEN SEGMENT1
        WHEN STRUCTURE_ID = 50272
        THEN SEGMENT1
        WHEN STRUCTURE_ID = 50309
        THEN SEGMENT1 || '.' || SEGMENT2
        WHEN STRUCTURE_ID = 50312
        THEN SEGMENT1 || '.' || SEGMENT2
        WHEN STRUCTURE_ID = 50328
        THEN SEGMENT1 || '.' || SEGMENT2
        WHEN STRUCTURE_ID = 50348
        THEN SEGMENT1
        WHEN STRUCTURE_ID = 50368
        THEN SEGMENT1
        WHEN STRUCTURE_ID = 50388
        THEN SEGMENT1 || '~' || SEGMENT2
        WHEN STRUCTURE_ID = 50409
        THEN SEGMENT1
        ELSE NULL
      END
    ) AS concatenated_segments
  FROM silver_bec_ods.mtl_item_categories AS mic, silver_bec_ods.MTL_CATEGORIES_B AS mc
  WHERE
    mic.category_id = mc.category_id AND mic.organization_id = 106 AND category_set_id = 1
)
INSERT INTO gold_bec_dwh_rpt.FACT_SUPPLIER_PRICE_LIST_RT
(
  SELECT
    aps.vendor_id,
    qph.list_header_id,
    qpl.list_line_id,
    cat.inventory_item_id,
    cat.organization_id,
    aps.vendor_name AS supplier,
    QPHT.NAME AS price_list_name,
    msi.segment1 AS part_number,
    msi.description AS part_description,
    qpl.list_price_uom_code AS uom_code,
    cat.segment1 AS subsystem,
    cat.segment2 AS category,
    QPL.operand AS unit_price,
    qpl.start_date_active AS effectivity_start_date,
    qpl.end_date_active AS effectivity_end_date,
    cic.material_cost,
    cat.concatenated_segments AS complete_category
  FROM silver_bec_ods.qp_list_headers_b AS qph, silver_bec_ods.qp_list_headers_tl AS qpht, silver_bec_ods.qp_list_lines AS qpl, silver_bec_ods.qp_qualifiers AS qpq, silver_bec_ods.ap_suppliers AS aps, silver_bec_ods.qp_pricing_attributes AS qpa, silver_bec_ods.mtl_system_items_b AS msi, item_category AS cat, silver_bec_ods.cst_item_costs AS cic
  WHERE
    qph.list_header_id = qpht.list_header_id
    AND qpht.LANGUAGE = 'US'
    AND qph.list_header_id = qpl.list_header_id
    AND qpl.LIST_LINE_ID = qpa.list_line_id
    AND qpa.product_attr_value = msi.inventory_item_id
    AND msi.organization_id = 90
    AND qph.source_system_code = 'PO'
    AND qph.end_date_active IS NULL
    AND qph.list_header_id = qpq.LIST_HEADER_ID()
    AND qpq.qualifier_attr_value = aps.VENDOR_ID()
    AND msi.inventory_item_id = cat.INVENTORY_ITEM_ID()
    AND cic.COST_TYPE_ID() = 1
    AND cic.ORGANIZATION_ID() = 106
    AND cic.INVENTORY_ITEM_ID() = msi.inventory_item_id
);
UPDATE bec_etl_ctrl.batch_dw_info SET load_type = 'I', last_refresh_date = CURRENT_TIMESTAMP()
WHERE
  dw_table_name = 'fact_supplier_price_list_rt' AND batch_name = 'ap';