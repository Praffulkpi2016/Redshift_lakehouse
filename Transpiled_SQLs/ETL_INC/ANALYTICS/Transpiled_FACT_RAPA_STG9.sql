TRUNCATE table gold_bec_dwh.FACT_RAPA_STG9;
INSERT INTO gold_bec_dwh.FACT_RAPA_STG9
(
  SELECT
    inventory_item_id,
    organization_id,
    cost_type,
    plan_name,
    data_type,
    order_group,
    order_type_text,
    po_number,
    promised_date1,
    aging_period,
    promised_date,
    purchase_item,
    item_description,
    planning_make_buy_code,
    category_name,
    po_open_qty,
    primary_quantity,
    unit_price,
    primary_unit_of_measure,
    po_open_amount,
    po_line_type,
    suggested_vendor_name,
    material_cost,
    ext_mtl_cost,
    VARIANCE,
    source_organization_id,
    planner_code,
    buyer_name,
    transactional_uom_code,
    release_num,
    (
      SELECT
        system_id
      FROM bec_etl_ctrl.etlsourceappid
      WHERE
        source_system = 'EBS'
    ) || '-' || inventory_item_id AS inventory_item_id_key,
    (
      SELECT
        system_id
      FROM bec_etl_ctrl.etlsourceappid
      WHERE
        source_system = 'EBS'
    ) || '-' || organization_id AS organization_id_key,
    (
      SELECT
        system_id
      FROM bec_etl_ctrl.etlsourceappid
      WHERE
        source_system = 'EBS'
    ) || '-' || source_organization_id AS source_organization_id_key,
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
    ) || '-' || COALESCE(cost_type, 'NA') || '-' || COALESCE(plan_name, 'NA') AS dw_load_id,
    CURRENT_TIMESTAMP() AS dw_insert_date,
    CURRENT_TIMESTAMP() AS dw_update_date
  FROM (
    SELECT
      inventory_item_id,
      ship_to_organization_id AS organization_id,
      cost_type,
      plan_name,
      data_type,
      order_group,
      order_type_text,
      po_number,
      promised_date1,
      aging_period,
      promised_date,
      purchase_item,
      item_description,
      planning_make_buy_code,
      category_name,
      po_open_qty,
      CAST(NULL AS DECIMAL(38, 10)) AS primary_quantity,
      unit_price,
      primary_unit_of_measure,
      po_open_amount1 * unit_price AS po_open_amount,
      po_line_type,
      suggested_vendor_name,
      material_cost,
      material_cost * po_open_qty AS ext_mtl_cost,
      COALESCE(po_open_qty * unit_price - material_cost * po_open_qty, 0) AS VARIANCE,
      source_organization_id,
      planner_code,
      buyer_name,
      transactional_uom_code,
      release_num
    FROM (
      SELECT
        CASE
          WHEN cic.cost_type_id = 1
          THEN 'Frozen'
          WHEN cic.cost_type_id = 3
          THEN 'Pending'
        END AS cost_type,
        NULL AS plan_name,
        'Receipt Forecast' AS data_type,
        'Open PR' AS order_group,
        'Purchase Requisitions' AS order_type_text,
        prh.segment1 AS po_number,
        destination_organization_id AS ship_to_organization_id,
        msi.inventory_item_id,
        FLOOR(prl.need_by_date) AS promised_date1,
        0 AS aging_period,
        FLOOR(need_by_date) AS promised_date,
        msi.segment1 AS purchase_item,
        msi.description AS item_description,
        CASE
          WHEN msi.planning_make_buy_code = 1
          THEN 'Make'
          WHEN msi.planning_make_buy_code = 2
          THEN 'Buy'
        END AS planning_make_buy_code,
        (
          SELECT
            mc.segment1 || '.' || mc.segment2
          FROM (
            SELECT
              *
            FROM silver_bec_ods.mtl_categories_b
            WHERE
              is_deleted_flg <> 'Y'
          ) AS mc, (
            SELECT
              *
            FROM silver_bec_ods.mtl_item_categories
            WHERE
              is_deleted_flg <> 'Y'
          ) AS mic
          WHERE
            mic.category_id = mc.category_id
            AND mic.category_set_id = 1
            AND mic.inventory_item_id = msi.inventory_item_id
            AND mic.organization_id = msi.organization_id
        ) AS category_name,
        COALESCE(prl.quantity, 0) - COALESCE(prl.quantity_cancelled, 0) - COALESCE(prl.quantity_received, 0) AS po_open_qty,
        CASE
          WHEN prl.unit_price = 0
          THEN COALESCE(
            (
              SELECT
                MAX(qpl.operand)
              FROM (
                SELECT
                  *
                FROM silver_bec_ods.qp_list_headers_b
                WHERE
                  is_deleted_flg <> 'Y'
              ) AS qph, (
                SELECT
                  *
                FROM silver_bec_ods.qp_list_headers_tl
                WHERE
                  is_deleted_flg <> 'Y'
              ) AS qpht, (
                SELECT
                  *
                FROM silver_bec_ods.qp_qualifiers
                WHERE
                  is_deleted_flg <> 'Y'
              ) AS qq, (
                SELECT
                  *
                FROM silver_bec_ods.qp_list_lines
                WHERE
                  is_deleted_flg <> 'Y'
              ) AS qpl, (
                SELECT
                  *
                FROM silver_bec_ods.qp_pricing_attributes
                WHERE
                  is_deleted_flg <> 'Y'
              ) AS qpa, (
                SELECT
                  *
                FROM silver_bec_ods.mtl_system_items_b
                WHERE
                  is_deleted_flg <> 'Y'
              ) AS msi1, (
                SELECT
                  *
                FROM silver_bec_ods.ap_suppliers
                WHERE
                  is_deleted_flg <> 'Y'
              ) AS aps1
              WHERE
                qph.list_header_id = qpht.list_header_id
                AND qpht.LANGUAGE = 'US'
                AND qph.list_header_id = qpl.list_header_id
                AND qpl.list_line_id = qpa.list_line_id
                AND qpa.product_attr_value = CAST((
                  msi1.inventory_item_id
                ) AS CHAR)
                AND qph.list_header_id = qq.list_header_id
                AND qq.qualifier_attr_value = prl.vendor_id
                AND aps1.vendor_name = suggested_vendor_name
                AND msi1.organization_id = 90
                AND qph.source_system_code = 'PO'
                AND qpht.name = suggested_vendor_name
                AND msi1.inventory_item_id = msi.inventory_item_id
                AND prl.need_by_date BETWEEN qpl.start_date_active AND COALESCE(qpl.end_date_active, need_by_date + 1)
            ),
            0
          )
          ELSE prl.unit_price
        END AS unit_price,
        primary_unit_of_measure,
        (
          COALESCE(prl.quantity, 0) - COALESCE(prl.quantity_cancelled, 0) - COALESCE(prl.quantity_received, 0)
        ) AS po_open_amount1,
        NULL AS po_line_type,
        suggested_vendor_name,
        cic.material_cost,
        cic.material_cost * (
          COALESCE(prl.quantity, 0) - COALESCE(prl.quantity_cancelled, 0) - COALESCE(prl.quantity_received, 0)
        ) AS ext_mtl_cost,
        CAST(NULL AS DECIMAL(15, 0)) AS source_organization_id,
        msi.planner_code,
        (
          SELECT
            agent_name
          FROM silver_bec_ods.po_agents_v
          WHERE
            agent_id = msi.buyer_id AND is_deleted_flg <> 'Y'
        ) AS buyer_name,
        unit_meas_lookup_code AS transactional_uom_code,
        CAST(NULL AS DECIMAL(15, 0)) AS release_num
      FROM (
        SELECT
          *
        FROM silver_bec_ods.po_requisition_lines_all
        WHERE
          is_deleted_flg <> 'Y'
      ) AS prl, (
        SELECT
          *
        FROM silver_bec_ods.po_requisition_headers_all
        WHERE
          is_deleted_flg <> 'Y'
      ) AS prh, (
        SELECT
          *
        FROM silver_bec_ods.cst_item_costs
        WHERE
          is_deleted_flg <> 'Y'
      ) AS cic, (
        SELECT
          *
        FROM silver_bec_ods.mtl_system_items_b
        WHERE
          is_deleted_flg <> 'Y'
      ) AS msi
      WHERE
        prh.requisition_header_id = prl.requisition_header_id
        AND prl.item_id = cic.inventory_item_id
        AND prl.destination_organization_id = msi.organization_id
        AND prl.item_id = msi.inventory_item_id
        AND prl.destination_organization_id = cic.organization_id
        AND cic.cost_type_id IN (1, 3)
        AND NOT EXISTS(
          SELECT
            1
          FROM (
            SELECT
              *
            FROM silver_bec_ods.po_distributions_all
            WHERE
              is_deleted_flg <> 'Y'
          ) AS pod, (
            SELECT
              *
            FROM silver_bec_ods.po_req_distributions_all
            WHERE
              is_deleted_flg <> 'Y'
          ) AS prd
          WHERE
            pod.req_distribution_id = prd.distribution_id
            AND prd.requisition_line_id = prl.requisition_line_id
        )
        AND COALESCE(prh.cancel_flag, 'N') = 'N'
        AND COALESCE(prl.cancel_flag, 'N') = 'N'
        AND NOT prl.item_id IS NULL
        AND prl.source_organization_id IS NULL
        AND destination_organization_id = 265 /* FS1 */
        AND prl.destination_type_code = 'INVENTORY'
        AND prh.interface_source_code = 'MSC'
        AND prh.authorization_status = 'APPROVED'
    )
  )
);
UPDATE bec_etl_ctrl.batch_dw_info SET load_type = 'I', last_refresh_date = CURRENT_TIMESTAMP()
WHERE
  dw_table_name = 'fact_rapa_stg9' AND batch_name = 'ascp';