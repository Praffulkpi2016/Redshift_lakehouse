DROP table IF EXISTS gold_bec_dwh_rpt.FACT_CST_ITEM_COST_RT;
CREATE TABLE gold_bec_dwh_rpt.FACT_CST_ITEM_COST_RT /* 3806330 */ AS
(
  WITH po_details AS (
    SELECT
      item_id,
      MAX(poh.segment1) AS po_number,
      MAX(pol.creation_date),
      MAX(UNIT_MEAS_LOOKUP_CODE) AS po_uom,
      MAX(unit_price) AS po_price
    FROM silver_bec_ods.po_lines_all AS pol, silver_bec_ods.po_headers_all AS poh
    WHERE
      pol.org_id = 85 AND poh.po_header_id = pol.po_header_id
    GROUP BY
      item_id
  ), rcv_details AS (
    SELECT
      rsl.item_id,
      rsl.to_organization_id,
      MAX(rsl.po_header_id) AS po_header_id,
      MAX(rsl.shipment_line_id) AS shipment_line_id
    FROM silver_bec_ods.rcv_shipment_headers AS rsh, silver_bec_ods.rcv_shipment_lines AS rsl
    WHERE
      rsh.SHIPMENT_HEADER_ID = rsl.SHIPMENT_HEADER_ID AND NOT rsh.RECEIPT_NUM IS NULL
    GROUP BY
      rsl.item_id,
      rsl.to_organization_id
  )
  SELECT
    cic.organization_id,
    cic.inventory_item_id,
    cic.cost_type_id,
    msi.segment1 AS part_number,
    msi.description,
    msi.creation_date AS item_creation_date,
    msi.primary_uom_code,
    msi.primary_unit_of_measure, /* MSI.COSTING_ENABLED_FLAG */
    msi.FULL_LEAD_TIME AS lead_time,
    lu1.meaning AS planning_make_buy_code,
    msi.inventory_item_status_code,
    cct.cost_type,
    cct.cost_type AS default_cost_type,
    msi.planner_code,
    CASE
      WHEN cic.based_on_rollup_flag = 1
      THEN 'Yes'
      WHEN cic.based_on_rollup_flag = 2
      THEN 'No'
      WHEN cic.based_on_rollup_flag = 3
      THEN 'Default'
      ELSE CAST(cic.based_on_rollup_flag AS CHAR)
    END AS based_on_rollup_flag, /* msi.organization_code, */ /* msi.organization_name, */
    cic.item_cost,
    COALESCE(cic.material_cost, 0) AS material_cost,
    cic.material_overhead_cost AS material_overhead,
    cic.resource_cost,
    cic.overhead_cost,
    cic.outside_processing_cost,
    mic.item_category_segment1,
    mic.item_category_segment2,
    msi.BUYER_ID,
    po.po_number AS last_po,
    po.po_uom AS last_po_uom,
    (
      po.po_price * (
        COALESCE(
          (
            SELECT
              conversion_rate
            FROM silver_bec_ods.mtl_uom_conversions AS muc
            WHERE
              unit_of_measure = po.po_uom AND muc.inventory_item_id = msi.inventory_item_id
          ),
          COALESCE(
            (
              SELECT
                conversion_rate
              FROM silver_bec_ods.mtl_uom_conversions
              WHERE
                unit_of_measure = po.po_uom AND inventory_item_id = 0
            ),
            1
          )
        ) / COALESCE(
          (
            SELECT
              conversion_rate
            FROM silver_bec_ods.mtl_uom_conversions AS muc
            WHERE
              unit_of_measure = msi.primary_unit_of_measure
              AND muc.inventory_item_id = msi.inventory_item_id
          ),
          COALESCE(
            (
              SELECT
                conversion_rate
              FROM silver_bec_ods.mtl_uom_conversions AS muc
              WHERE
                unit_of_measure = msi.primary_unit_of_measure AND muc.inventory_item_id = 0
            ),
            1
          )
        )
      )
    ) AS last_po_price,
    (
      SELECT
        segment1 /* into l_po_number */
      FROM silver_bec_ods.po_headers_all
      WHERE
        po_header_id = rcv.po_header_id
    ) AS last_receipt_po,
    (
      SELECT
        MAX((
          po_unit_price * rt.quantity
        ) / rt.primary_quantity) /* into l_unit_price */
      FROM silver_bec_ods.rcv_transactions AS rt
      WHERE
        shipment_line_id = rcv.shipment_line_id
        AND rt.organization_id = cic.organization_id
        AND rt.destination_type_code = 'INVENTORY'
    ) AS last_receipt_price,
    (
      SELECT
        MAX(rt.SOURCE_DOC_UNIT_OF_MEASURE) /* into l_recpt_uom */
      FROM silver_bec_ods.rcv_transactions AS rt
      WHERE
        shipment_line_id = rcv.shipment_line_id
        AND rt.organization_id = cic.organization_id
        AND rt.destination_type_code = 'INVENTORY'
    ) AS last_receipt_uom,
    (
      SELECT
        MAX(vendor_id)
      FROM silver_bec_ods.po_headers_all AS poh
      WHERE
        1 = 1 AND poh.SEGMENT1 = po.po_number
    ) AS vendor_id
  FROM silver_bec_ods.mtl_system_items_b AS msi, silver_bec_ods.cst_item_costs AS cic, gold_bec_dwh.dim_cost_types AS cct, gold_bec_dwh.dim_cost_types AS cct2, gold_bec_dwh.dim_inv_item_category_set AS mic, gold_bec_dwh.dim_lookups AS LU1, po_details AS PO, rcv_details AS rcv
  /* ,gold_bec_dwh.dim_inv_safty_stock  mss */
  WHERE
    msi.inventory_item_id = cic.inventory_item_id
    AND msi.organization_id = cic.organization_id
    AND MSI.COSTING_ENABLED_FLAG = 'Y'
    AND cic.cost_type_id = cct.cost_type_id
    AND cic.cost_type_id = 1
    AND CCT2.COST_TYPE_ID = CCT.DEFAULT_COST_TYPE_ID
    AND cic.ORGANIZATION_ID = mic.ORGANIZATION_ID()
    AND cic.INVENTORY_ITEM_ID = mic.INVENTORY_ITEM_ID()
    AND mic.CATEGORY_SET_ID() = 1
    AND LU1.LOOKUP_CODE() = MSI.PLANNING_MAKE_BUY_CODE
    AND LU1.LOOKUP_TYPE() = 'MTL_PLANNING_MAKE_BUY'
    AND cic.inventory_item_id = po.ITEM_ID()
    AND cic.inventory_item_id = rcv.ITEM_ID()
    AND cic.organization_id = rcv.TO_ORGANIZATION_ID()
);
UPDATE bec_etl_ctrl.batch_dw_info SET load_type = 'I', last_refresh_date = CURRENT_TIMESTAMP()
WHERE
  dw_table_name = 'fact_cst_item_cost_rt' AND batch_name = 'inv';