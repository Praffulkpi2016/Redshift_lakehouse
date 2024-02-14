DROP table IF EXISTS gold_bec_dwh.FACT_ASCP_SUPPLY_DEMAND;
CREATE TABLE gold_bec_dwh.FACT_ASCP_SUPPLY_DEMAND AS
(
  SELECT
    inventory_item_id,
    organization_id,
    plan_id,
    sr_instance_id,
    category_set_id,
    compile_designator,
    source_table,
    creation_date,
    rescheduled_flag,
    new_processing_days,
    ship_date,
    release_time_fence_code,
    within_rel_time_fence,
    purchasing_enabled_flag,
    old_need_by_date,
    buyer_name,
    action,
    transaction_id,
    count_action,
    new_due_date,
    order_type,
    order_type_text,
    organization_code,
    item_segments,
    description,
    category_name,
    order_number,
    comments,
    planner_code,
    supplier_name,
    new_dock_date,
    OLD_DOCK_DATE,
    promise_date,
    days_diff,
    quantity_rate,
    amount,
    old_due_date,
    reschedule_days,
    list_price,
    delivery_price,
    new_order_date,
    new_start_date,
    source_organization_id,
    source_vendor_site_code,
    schedule_compression_days,
    subinventory_code,
    vmi_flag,
    days_from_today,
    source_vendor_name,
    USING_ASSEMBLY_SEGMENTS,
    INTRANSIT_LEAD_TIME,
    line_number,
    po_number,
    po_uom,
    po_amount,
    release_number,
    shipment_num,
    po_type,
    cvmi_flag,
    material_cost,
    extended_material_cost,
    po_qty_due,
    po_qty,
    po_price,
    open_po_price,
    BUILD_IN_WIP_FLAG,
    LOT_NUMBER,
    REQUEST_DATE,
    NEED_BY_DATE,
    quantity,
    drill_order_number, /* columns added to support drilldown */
    drill_release_num,
    drill_line_num,
    drill_shipment_num,
    order_type_entity,
    forecast_set,
    forecast_designator,
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
    ) || '-' || COALESCE(PLAN_ID, 0) || '-' || COALESCE(TRANSACTION_ID, 0) AS dw_load_id,
    CURRENT_TIMESTAMP() AS dw_insert_date,
    CURRENT_TIMESTAMP() AS dw_update_date
  FROM (
    WITH cvmi_flag_t AS (
      SELECT
        MAX(asl.consigned_from_supplier_flag) AS cvmi_flag,
        msi.segment1 AS segment1,
        msi.organization_id AS organization_id,
        aps.vendor_name AS vendor_name
      FROM silver_bec_ods.po_asl_attributes AS asl, silver_bec_ods.mtl_system_items_b AS msi, silver_bec_ods.ap_suppliers AS aps
      WHERE
        asl.item_id = msi.inventory_item_id
        AND asl.using_organization_id = msi.organization_id
        AND asl.vendor_id = aps.vendor_id
      GROUP BY
        msi.segment1,
        msi.organization_id,
        aps.vendor_name
    ), cic_cost AS (
      SELECT
        cic.material_cost,
        cic.organization_id,
        msi.segment1
      FROM silver_bec_ods.cst_item_costs AS cic, silver_bec_ods.mtl_system_items_b AS msi
      WHERE
        cic.cost_type_id = 1
        AND cic.organization_id = msi.organization_id
        AND cic.inventory_item_id = msi.inventory_item_id
    ), po_quantity_details AS (
      SELECT
        poll.quantity AS po_qty,
        (
          poll.quantity - poll.quantity_cancelled - poll.quantity_received
        ) AS po_qty_due,
        pol.line_num,
        pol.unit_meas_lookup_code AS po_uom,
        poll.shipment_num,
        poh.segment1
      FROM silver_bec_ods.po_lines_all AS pol, silver_bec_ods.po_headers_all AS poh, silver_bec_ods.po_line_locations_all AS poll
      WHERE
        poh.po_header_id = pol.po_header_id AND pol.po_line_id = poll.po_line_id
    ), por_quantity_details AS (
      SELECT
        poll.quantity AS po_qty,
        (
          poll.quantity - poll.quantity_cancelled - poll.quantity_received
        ) AS po_qty_due,
        por.release_num,
        poll.shipment_num,
        poh.segment1,
        pol.line_num
      FROM silver_bec_ods.po_lines_all AS pol, silver_bec_ods.po_headers_all AS poh, silver_bec_ods.po_line_locations_all AS poll, silver_bec_ods.po_releases_all AS por
      WHERE
        poh.po_header_id = pol.po_header_id
        AND pol.po_line_id = poll.po_line_id
        AND poll.po_release_id = por.po_release_id
    )
    SELECT
      INVENTORY_ITEM_ID,
      mo.ORGANIZATION_ID,
      mo.PLAN_ID,
      mo.SR_INSTANCE_ID,
      category_set_id,
      mo.COMPILE_DESIGNATOR,
      source_table,
      creation_date,
      rescheduled_flag,
      new_processing_days,
      ship_date,
      release_time_fence_code,
      within_rel_time_fence,
      purchasing_enabled_flag,
      old_need_by_date,
      buyer_name,
      ACTION,
      TRANSACTION_ID,
      COUNT(*) OVER (PARTITION BY mo.PLAN_ID, mo.SR_INSTANCE_ID, category_set_id, mo.ORGANIZATION_ID, INVENTORY_ITEM_ID, BUYER_NAME, ACTION) AS count_action,
      NEW_DUE_DATE,
      ORDER_TYPE,
      order_type_text,
      organization_code,
      item_segments,
      mo.description,
      category_name,
      order_number,
      comments,
      planner_code,
      SUPPLIER_NAME,
      new_dock_date,
      promise_date,
      DATEDIFF(new_dock_date, promise_date) AS DAYS_DIFF, /* ABS(promise_date - new_dock_date)   DAYS_DIFF, */
      SUM(quantity_rate) AS quantity_rate,
      SUM(amount) AS amount,
      OLD_DUE_DATE, /* new columns -- */
      OLD_NEED_BY_DATE AS OLD_DOCK_DATE,
      reschedule_days,
      list_price,
      delivery_price,
      NEW_ORDER_DATE,
      NEW_START_DATE,
      SOURCE_ORGANIZATION_ID,
      SOURCE_VENDOR_SITE_CODE,
      SCHEDULE_COMPRESSION_DAYS,
      subinventory_code,
      vmi_flag,
      days_from_today,
      mo.source_vendor_name,
      mo.USING_ASSEMBLY_SEGMENTS,
      mo.INTRANSIT_LEAD_TIME,
      po_line_id AS line_number,
      po.segment1 AS po_number,
      po.po_uom,
      SUM(quantity_rate) * delivery_price AS po_amount,
      por.release_num AS release_number,
      COALESCE(po.shipment_num, por.shipment_num) AS shipment_num,
      CASE
        WHEN mo.order_type_text = 'Purchase order'
        THEN CASE
          WHEN SUBSTRING(
            mo.order_number,
            REGEXP_INSTR(mo.order_number, '[(]', 1, 1) + 1,
            REGEXP_INSTR(mo.order_number, '[)]', 1, 1) - REGEXP_INSTR(mo.order_number, '[(]', 1, 1) - 1
          ) = ' '
          THEN 'STANDARD'
          ELSE 'BLANKET'
        END
        ELSE NULL
      END AS po_type,
      COALESCE(cvmi.cvmi_flag, 'N') AS cvmi_flag,
      SUM(cic.material_cost) AS material_cost,
      SUM(cic.material_cost) * SUM(quantity) AS extended_material_cost,
      SUM(
        CASE
          WHEN SUBSTRING(mo.order_number, REGEXP_INSTR(mo.order_number, '[(]', 1, 1) + 1, 1) = ' '
          THEN COALESCE(po.po_qty_due, 0)
          ELSE COALESCE(por.po_qty_due, 0)
        END
      ) AS po_qty_due,
      SUM(
        CASE
          WHEN SUBSTRING(mo.order_number, REGEXP_INSTR(mo.order_number, '[(]', 1, 1) + 1, 1) = ' '
          THEN COALESCE(po.po_qty, 0)
          ELSE COALESCE(por.po_qty, 0)
        END
      ) AS po_qty,
      delivery_price * SUM(
        CASE
          WHEN SUBSTRING(mo.order_number, REGEXP_INSTR(mo.order_number, '[(]', 1, 1) + 1, 1) = ' '
          THEN COALESCE(po.po_qty, 0)
          ELSE COALESCE(por.po_qty, 0)
        END
      ) AS po_price,
      delivery_price * SUM(
        CASE
          WHEN SUBSTRING(mo.order_number, REGEXP_INSTR(mo.order_number, '[(]', 1, 1) + 1, 1) = ' '
          THEN COALESCE(po.po_qty_due, 0)
          ELSE COALESCE(por.po_qty_due, 0)
        END
      ) AS open_po_price,
      BUILD_IN_WIP_FLAG,
      LOT_NUMBER,
      REQUEST_DATE,
      NEED_BY_DATE,
      quantity,
      CASE
        WHEN order_type_text = 'Purchase order'
        THEN (
          SUBSTRING(mo.order_number, 1, REGEXP_INSTR(mo.order_number, '[(]', 1, 1) - 1)
        )
        WHEN order_type_text = 'Sales Orders'
        THEN (
          SUBSTRING(mo.order_number, 1, REGEXP_INSTR(mo.order_number, '[.]', 1, 1) - 1)
        )
        ELSE mo.order_number
      END AS drill_order_number, /* columns added to support drilldown */
      CASE
        WHEN order_type_text = 'Purchase order'
        THEN (
          SUBSTRING(
            mo.order_number,
            REGEXP_INSTR(mo.order_number, '[(]', 1, 1) + 1,
            (
              REGEXP_INSTR(mo.order_number, '[)]', 1, 1) - 1 - REGEXP_INSTR(mo.order_number, '[(]', 1, 1)
            )
          )
        )
      END AS drill_release_num,
      CASE
        WHEN order_type_text = 'Purchase order'
        THEN CAST(mo.po_line_id AS STRING)
        WHEN order_type_text = 'Sales Orders'
        THEN (
          SUBSTRING(
            mo.order_number,
            REGEXP_INSTR(mo.order_number, '[(]', 1, 1) + 1,
            (
              REGEXP_INSTR(mo.order_number, '[.]', 1, 3) - REGEXP_INSTR(mo.order_number, '[(]', 1, 1) - 1
            )
          )
        )
        ELSE CAST(mo.po_line_id AS STRING)
      END AS drill_line_num,
      CASE
        WHEN order_type_text = 'Purchase order'
        THEN SUBSTRING(mo.order_number, REGEXP_INSTR(mo.order_number, '[(]', 1, 3) + 1, 1)
        WHEN order_type_text = 'Sales Orders'
        THEN (
          SUBSTRING(
            mo.order_number,
            REGEXP_INSTR(mo.order_number, '[.]', 1, 3) + 1,
            (
              REGEXP_INSTR(mo.order_number, '[)]', 1, 1) - REGEXP_INSTR(mo.order_number, '[.]', 1, 3) - 1
            )
          )
        )
      END AS drill_shipment_num,
      CASE
        WHEN mo.order_type_text = 'Sales Orders'
        THEN 'Sales orders'
        WHEN mo.order_type_text = 'Forecast'
        THEN 'Forecast'
        WHEN mo.order_type_text = 'Planned order demand'
        THEN 'Dependent demand'
        WHEN mo.order_type_text = 'Work order demand'
        THEN 'Dependent demand'
        WHEN mo.order_type_text = 'Non-standard job demand'
        THEN 'Dependent demand'
        WHEN mo.order_type_text = 'Purchase requisition scrap'
        THEN 'Expected scrap'
        WHEN mo.order_type_text = 'Purchase order scrap'
        THEN 'Expected scrap'
        WHEN mo.order_type_text = 'Planned order scrap'
        THEN 'Expected scrap'
        WHEN mo.order_type_text = 'Intransit shipment scrap'
        THEN 'Expected scrap'
        WHEN mo.order_type_text = 'Work Order scrap'
        THEN 'Expected scrap'
        WHEN mo.order_type_text = 'Non-standard job'
        THEN 'Work orders'
        WHEN mo.order_type_text = 'Nonstandard job by-product'
        THEN 'Work orders'
        WHEN mo.order_type_text = 'Work order co-product/by-product'
        THEN 'Work orders'
        WHEN mo.order_type_text = 'Work order'
        THEN 'Work orders'
        WHEN mo.order_type_text = 'Purchase order'
        THEN 'Purchase orders'
        WHEN mo.order_type_text = 'Purchase requisition'
        THEN 'Requisitions/ CVMI Consumtion Plan'
        WHEN mo.order_type_text = 'Intransit shipment'
        THEN 'In Transit'
        WHEN mo.order_type_text = 'PO in receiving'
        THEN 'In Receiving'
        WHEN mo.order_type_text = 'Intransit receipt'
        THEN 'In Receiving'
        WHEN mo.order_type_text = 'Planned order'
        THEN 'Planned orders'
        WHEN mo.order_type_text = 'Planned order co-product/by-product'
        THEN 'Planned orders'
        WHEN mo.order_type_text = 'On Hand'
        THEN 'Beginning on hand'
      END AS order_type_entity,
      CASE
        WHEN mo.order_type_text = 'Forecast'
        THEN (
          SUBSTRING(mo.order_number, 1, REGEXP_INSTR(mo.order_number, '[/]', 1, 1) - 1)
        )
      END AS forecast_set,
      CASE
        WHEN mo.order_type_text = 'Forecast'
        THEN (
          SUBSTRING(mo.order_number, REGEXP_INSTR(mo.order_number, '[/]', 1, 1) + 1)
        )
      END AS forecast_designator
    FROM silver_bec_ods.msc_orders_v AS mo, cvmi_flag_t AS cvmi, cic_cost AS cic, po_quantity_details AS po, por_quantity_details AS por, gold_bec_dwh.DIM_ASCP_PLANS AS plans
    WHERE
      category_set_id = 9
      AND cvmi.SEGMENT1() = mo.item_segments
      AND cvmi.ORGANIZATION_ID() = mo.organization_id
      AND cvmi.VENDOR_NAME() = mo.source_vendor_name
      AND cic.ORGANIZATION_ID() = mo.organization_id
      AND cic.SEGMENT1() = mo.item_segments
      AND po.LINE_NUM() = mo.po_line_id
      AND CAST(po.SHIPMENT_NUM() AS STRING) = SUBSTRING(mo.order_number, REGEXP_INSTR(mo.order_number, '[(]', 1, 3) + 1, 1)
      AND CAST(po.SEGMENT1() AS STRING) = CASE
        WHEN (
          REGEXP_INSTR(mo.order_number, '[(]', 1, 1) - 1
        ) < 0
        THEN NULL
        ELSE (
          SUBSTRING(mo.order_number, 1, REGEXP_INSTR(mo.order_number, '[(]', 1, 1) - 1)
        )
      END
      AND por.LINE_NUM() = mo.po_line_id
      AND CAST(por.RELEASE_NUM() AS STRING) = CASE
        WHEN (
          (
            REGEXP_INSTR(mo.order_number, '[)]', 1, 1) - 1 - REGEXP_INSTR(mo.order_number, '[(]', 1, 1)
          )
        ) < 0
        THEN NULL
        ELSE (
          SUBSTRING(
            mo.order_number,
            REGEXP_INSTR(mo.order_number, '[(]', 1, 1) + 1,
            (
              REGEXP_INSTR(mo.order_number, '[)]', 1, 1) - 1 - REGEXP_INSTR(mo.order_number, '[(]', 1, 1)
            )
          )
        )
      END
      AND CAST(por.SHIPMENT_NUM() AS STRING) = SUBSTRING(mo.order_number, REGEXP_INSTR(mo.order_number, '[(]', 1, 3) + 1, 1)
      AND CAST(por.SEGMENT1() AS STRING) = CASE
        WHEN (
          REGEXP_INSTR(mo.order_number, '[(]', 1, 1) - 1
        ) < 0
        THEN NULL
        ELSE (
          SUBSTRING(mo.order_number, 1, REGEXP_INSTR(mo.order_number, '[(]', 1, 1) - 1)
        )
      END
      AND mo.plan_id = plans.plan_id
      AND mo.sr_instance_id = plans.sr_instance_id
      AND plans.LOAD_FLG = 'Y'
    GROUP BY
      INVENTORY_ITEM_ID,
      mo.ORGANIZATION_ID,
      mo.PLAN_ID,
      mo.SR_INSTANCE_ID,
      category_set_id,
      mo.COMPILE_DESIGNATOR,
      buyer_name,
      ACTION,
      transaction_id,
      NEW_DUE_DATE,
      ORDER_TYPE,
      order_type_text,
      organization_code,
      item_segments,
      mo.description,
      category_name,
      order_number,
      comments,
      planner_code,
      SUPPLIER_NAME,
      new_dock_date,
      promise_date,
      new_dock_date - promise_date,
      OLD_DUE_DATE,
      reschedule_days,
      new_processing_days,
      list_price,
      delivery_price,
      NEW_ORDER_DATE,
      NEW_START_DATE,
      OLD_NEED_BY_DATE,
      SOURCE_ORGANIZATION_ID,
      SOURCE_VENDOR_SITE_CODE,
      SCHEDULE_COMPRESSION_DAYS,
      subinventory_code,
      vmi_flag,
      days_from_today,
      mo.source_vendor_name,
      cvmi.cvmi_flag,
      mo.USING_ASSEMBLY_SEGMENTS,
      mo.INTRANSIT_LEAD_TIME,
      po_line_id,
      por.release_num,
      po.segment1,
      po.po_uom,
      COALESCE(po.shipment_num, por.shipment_num),
      source_table,
      creation_date,
      rescheduled_flag,
      ship_date,
      release_time_fence_code,
      within_rel_time_fence,
      purchasing_enabled_flag,
      old_need_by_date,
      BUILD_IN_WIP_FLAG,
      LOT_NUMBER,
      REQUEST_DATE,
      NEED_BY_DATE,
      quantity
  )
);
UPDATE bec_etl_ctrl.batch_dw_info SET load_type = 'I', last_refresh_date = CURRENT_TIMESTAMP()
WHERE
  dw_table_name = 'fact_ascp_supply_demand' AND batch_name = 'ascp';