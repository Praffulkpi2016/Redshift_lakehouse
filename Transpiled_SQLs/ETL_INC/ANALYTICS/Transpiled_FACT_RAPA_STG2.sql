TRUNCATE table gold_bec_dwh.FACT_RAPA_STG2;
WITH CTE_main_stg AS (
  SELECT
    CASE
      WHEN cic.cost_type_id = 1
      THEN 'Frozen'
      WHEN cic.cost_type_id = 3
      THEN 'Pending'
    END AS cost_type,
    v.compile_designator,
    'Receipt Forecast' AS data_type,
    'Planned Order' AS order_group,
    v.order_number,
    v.organization_id,
    v.inventory_item_id,
    v.new_order_date,
    0 AS aging_period,
    FLOOR(v.new_dock_date) AS new_dock_date,
    v.item_segments,
    v.description,
    v.planning_make_buy_code,
    v.category_name,
    v.quantity,
    CAST(NULL AS DECIMAL(38, 10)) AS primary_quantity,
    NULL AS primary_unit_of_measure,
    NULL AS po_line_type,
    v.source_vendor_name,
    cic.material_cost AS mtl_cost,
    cic.material_cost * quantity AS ext_mtl_cost,
    v.source_organization_id,
    v.planner_code,
    v.buyer_name,
    (
      SELECT
        pol.unit_meas_lookup_code
      FROM (
        SELECT
          *
        FROM silver_bec_ods.po_lines_all
        WHERE
          is_deleted_flg <> 'Y'
      ) AS pol, (
        SELECT
          *
        FROM silver_bec_ods.po_headers_all
        WHERE
          is_deleted_flg <> 'Y'
      ) AS poh
      WHERE
        poh.po_header_id = pol.po_header_id
        AND pol.line_num = v.po_line_id
        AND poh.segment1 = SUBSTRING(
          v.order_number,
          1,
          CASE
            WHEN REGEXP_INSTR(v.order_number, '[(]', 1, 1) = 0
            THEN 0
            ELSE REGEXP_INSTR(v.order_number, '[(]', 1, 1) - 1
          END
        )
    ) AS transactional_uom_code,
    CAST(NULL AS DECIMAL(15, 0)) AS release_num,
    CAST(msi.inventory_item_id AS STRING) AS inventory_item_id_msi
  FROM silver_bec_ods.MSC_ORDERS_V AS v, (
    SELECT
      *
    FROM silver_bec_ods.fnd_lookup_values
    WHERE
      is_deleted_flg <> 'Y'
  ) AS flv, (
    SELECT
      *
    FROM silver_bec_ods.mtl_system_items_b
    WHERE
      is_deleted_flg <> 'Y'
  ) AS msi, (
    SELECT
      *
    FROM silver_bec_ods.cst_item_costs
    WHERE
      is_deleted_flg <> 'Y'
  ) AS cic, CVMI AS cv
  WHERE
    1 = 1
    AND v.wip_supply_type = flv.lookup_code
    AND flv.lookup_type = 'WIP_SUPPLY'
    AND v.category_set_id = 9
    AND v.planning_make_buy_code = 2
    AND v.order_type_text = 'Planned order'
    AND msi.organization_id = v.organization_id
    AND msi.segment1 = v.item_segments
    AND cic.cost_type_id IN (1, 3)
    AND cic.organization_id = msi.organization_id
    AND cic.inventory_item_id = msi.inventory_item_id
    AND v.item_segments = cv.ITEM_SEGMENTS()
    AND v.ORGANIZATION_ID = cv.ORGANIZATION_ID()
    AND v.source_vendor_name = cv.VENDOR_NAME()
    AND (
      cvmi_flag = 'N' OR cvmi_flag IS NULL
    )
), CTE_main_stg AS (
  SELECT
    CASE
      WHEN cic.cost_type_id = 1
      THEN 'Frozen'
      WHEN cic.cost_type_id = 3
      THEN 'Pending'
    END AS cost_type,
    v.compile_designator,
    'Receipt Forecast' AS data_type,
    'CVMI Planned' AS order_group,
    v.order_number,
    v.organization_id,
    v.inventory_item_id,
    FLOOR(v.new_dock_date) AS new_dock_date1,
    (
      SELECT
        MAX(aging_period)
      FROM (
        SELECT
          *
        FROM silver_bec_ods.po_asl_attributes
        WHERE
          is_deleted_flg <> 'Y'
      ) AS pasl, (
        SELECT
          *
        FROM silver_bec_ods.ap_suppliers
        WHERE
          is_deleted_flg <> 'Y'
      ) AS aps
      WHERE
        NOT aging_period IS NULL
        AND pasl.vendor_id = aps.vendor_id
        AND pasl.using_organization_id = msi.organization_id
        AND pasl.item_id = msi.inventory_item_id
        AND pasl.using_organization_id = v.organization_id
        AND aps.vendor_name = v.source_vendor_name
    ) AS aging_period,
    v.new_dock_date,
    v.item_segments,
    v.description,
    v.planning_make_buy_code,
    v.category_name,
    v.quantity,
    CAST(NULL AS DECIMAL(38, 10)) AS primary_quantity,
    NULL AS primary_unit_of_measure,
    NULL AS po_line_type,
    v.source_vendor_name,
    cic.material_cost AS mtl_cost,
    cic.material_cost * quantity AS ext_mtl_cost,
    v.source_organization_id,
    v.planner_code,
    v.buyer_name,
    (
      SELECT
        pol.unit_meas_lookup_code
      FROM (
        SELECT
          *
        FROM silver_bec_ods.po_lines_all
        WHERE
          is_deleted_flg <> 'Y'
      ) AS pol, (
        SELECT
          *
        FROM silver_bec_ods.po_headers_all
        WHERE
          is_deleted_flg <> 'Y'
      ) AS poh
      WHERE
        poh.po_header_id = pol.po_header_id
        AND pol.line_num = v.po_line_id
        AND poh.segment1 = SUBSTRING(
          v.order_number,
          1,
          CASE
            WHEN REGEXP_INSTR(v.order_number, '[(]', 1, 1) = 0
            THEN 0
            ELSE REGEXP_INSTR(v.order_number, '[(]', 1, 1) - 1
          END
        )
    ) AS transactional_uom_code,
    CAST(NULL AS DECIMAL(15, 0)) AS release_num,
    CAST(msi.inventory_item_id AS STRING) AS inventory_item_id_msi
  FROM silver_bec_ods.MSC_ORDERS_V AS v, (
    SELECT
      *
    FROM silver_bec_ods.fnd_lookup_values
    WHERE
      is_deleted_flg <> 'Y'
  ) AS flv, (
    SELECT
      *
    FROM silver_bec_ods.mtl_system_items_b
    WHERE
      is_deleted_flg <> 'Y'
  ) AS msi, (
    SELECT
      *
    FROM silver_bec_ods.cst_item_costs
    WHERE
      is_deleted_flg <> 'Y'
  ) AS cic, CVMI AS cv
  WHERE
    1 = 1
    AND v.wip_supply_type = flv.lookup_code
    AND flv.lookup_type = 'WIP_SUPPLY'
    AND v.category_set_id = 9
    AND v.planning_make_buy_code = 2
    AND order_type_text = 'Planned order'
    AND msi.organization_id = v.organization_id
    AND msi.segment1 = v.item_segments
    AND cic.cost_type_id IN (1, 3)
    AND cic.organization_id = msi.organization_id
    AND cic.inventory_item_id = msi.inventory_item_id
    AND v.item_segments = cv.ITEM_SEGMENTS()
    AND v.ORGANIZATION_ID = cv.ORGANIZATION_ID()
    AND v.source_vendor_name = cv.VENDOR_NAME()
    AND (
      cvmi_flag = 'Y'
    )
), CVMI AS (
  SELECT
    msi.ORGANIZATION_ID,
    aps.vendor_name,
    msi.segment1 AS item_Segments,
    COALESCE(MAX(aging_period), 0) AS AGING_PERIOD,
    COALESCE(MAX(ASL.CONSIGNED_FROM_SUPPLIER_FLAG), 'N') AS CVMI_FLAG
  FROM (
    SELECT
      *
    FROM silver_bec_ods.PO_ASL_ATTRIBUTES
    WHERE
      is_deleted_flg <> 'Y'
  ) AS asl, (
    SELECT
      *
    FROM silver_bec_ods.mtl_system_items_b
    WHERE
      is_deleted_flg <> 'Y'
  ) AS msi, (
    SELECT
      *
    FROM silver_bec_ods.ap_suppliers
    WHERE
      is_deleted_flg <> 'Y'
  ) AS aps
  WHERE
    1 = 1
    AND ASL.ITEM_ID = MSI.INVENTORY_ITEM_ID
    AND ASL.USING_ORGANIZATION_ID = MSI.ORGANIZATION_ID
    AND asl.vendor_id = aps.vendor_id
  GROUP BY
    msi.ORGANIZATION_ID,
    aps.vendor_name,
    msi.segment1
), CVMI AS (
  SELECT
    msi.ORGANIZATION_ID,
    aps.vendor_name,
    msi.segment1 AS item_Segments,
    COALESCE(MAX(aging_period), 0) AS AGING_PERIOD,
    COALESCE(MAX(ASL.CONSIGNED_FROM_SUPPLIER_FLAG), 'N') AS CVMI_FLAG
  FROM (
    SELECT
      *
    FROM silver_bec_ods.PO_ASL_ATTRIBUTES
    WHERE
      is_deleted_flg <> 'Y'
  ) AS asl, (
    SELECT
      *
    FROM silver_bec_ods.mtl_system_items_b
    WHERE
      is_deleted_flg <> 'Y'
  ) AS msi, (
    SELECT
      *
    FROM silver_bec_ods.ap_suppliers
    WHERE
      is_deleted_flg <> 'Y'
  ) AS aps
  WHERE
    1 = 1
    AND ASL.ITEM_ID = MSI.INVENTORY_ITEM_ID
    AND ASL.USING_ORGANIZATION_ID = MSI.ORGANIZATION_ID
    AND asl.vendor_id = aps.vendor_id
  GROUP BY
    msi.ORGANIZATION_ID,
    aps.vendor_name,
    msi.segment1
)
INSERT INTO gold_bec_dwh.FACT_RAPA_STG2
(
  SELECT
    inventory_item_id,
    organization_id,
    cost_type,
    plan_name,
    data_type,
    order_group,
    (
      CASE
        WHEN order_group = 'CVMI Planned'
        THEN CASE
          WHEN order_type_meas IS NULL
          THEN 'CVMI Planned Order (Mat\'l Cost)'
          ELSE 'CVMI Planned Order'
        END
        WHEN order_group = 'Planned Order'
        THEN CASE
          WHEN order_type_meas IS NULL
          THEN 'Planned Order (Mat\'l Cost)'
          ELSE 'Planned Order'
        END
      END
    ) AS order_type_text,
    order_number,
    new_dock_date1,
    aging_period,
    new_dock_date,
    item_segments,
    description,
    CASE
      WHEN planning_make_buy_code = 1
      THEN 'Make'
      WHEN planning_make_buy_code = 2
      THEN 'Buy'
    END AS planning_make_buy_code,
    category_name,
    quantity,
    primary_quantity,
    order_type_meas AS calc_unit_cost,
    primary_unit_of_measure,
    (
      COALESCE(order_type_meas, mtl_cost) * quantity
    ) AS ext_unit_cost,
    po_line_type,
    source_vendor_name,
    mtl_cost,
    ext_mtl_cost,
    (
      (
        COALESCE(order_type_meas, mtl_cost) * quantity
      ) - ext_mtl_cost
    ) AS variance,
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
    (
      SELECT
        system_id
      FROM bec_etl_ctrl.etlsourceappid
      WHERE
        source_system = 'EBS'
    ) || '-' || order_number AS order_number_key,
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
    ) || '-' || COALESCE(order_number, 'NA') || '-' || COALESCE(cost_type, 'NA') AS dw_load_id,
    CURRENT_TIMESTAMP() AS dw_insert_date,
    CURRENT_TIMESTAMP() AS dw_update_date
  FROM (
    (
      SELECT
        cm.cost_type,
        cm.compile_designator AS plan_name,
        cm.data_type,
        cm.order_group,
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
            FROM silver_bec_ods.ap_suppliers
            WHERE
              is_deleted_flg <> 'Y'
          ) AS aps
          WHERE
            qph.list_header_id = qpht.list_header_id
            AND qpht.LANGUAGE = 'US'
            AND qph.list_header_id = qpl.list_header_id
            AND qpl.list_line_id = qpa.list_line_id
            AND qpa.product_attr_value = cm.inventory_item_id_msi
            AND qph.list_header_id = qq.list_header_id
            AND qq.qualifier_attr_value = aps.vendor_id
            AND aps.vendor_name = cm.source_vendor_name
            AND qph.source_system_code = 'PO'
            AND CASE WHEN qpht.name = 'NEWARK' THEN 'NEWARK..' ELSE qpht.name END = CASE
              WHEN cm.compile_designator = 'BE-GLOBAL'
              THEN aps.vendor_name
              ELSE CASE
                WHEN comt.name IS NULL
                THEN aps.vendor_name
                ELSE aps.vendor_name || '*' || cm.compile_designator
              END
            END
            AND cm.new_order_date BETWEEN qpl.start_date_active AND COALESCE(qpl.end_date_active, cm.new_order_date + 1)
        ) AS order_type_meas,
        cm.order_number,
        cm.organization_id,
        cm.inventory_item_id,
        FLOOR(cm.new_order_date) AS new_dock_date1,
        cm.aging_period,
        cm.new_dock_date,
        cm.item_segments,
        cm.description,
        cm.planning_make_buy_code,
        cm.category_name,
        cm.quantity,
        cm.primary_quantity,
        cm.primary_unit_of_measure,
        cm.po_line_type,
        cm.source_vendor_name,
        cm.mtl_cost,
        cm.ext_mtl_cost,
        cm.source_organization_id,
        cm.planner_code,
        cm.buyer_name,
        cm.transactional_uom_code,
        cm.release_num
      FROM CTE_main_stg AS cm
      LEFT OUTER JOIN (
        SELECT
          qpht.name,
          aps.vendor_name
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
          FROM silver_bec_ods.ap_suppliers
          WHERE
            is_deleted_flg <> 'Y'
        ) AS aps
        WHERE
          qph.list_header_id = qpht.list_header_id
          AND qpht.LANGUAGE = 'US'
          AND qph.list_header_id = qq.list_header_id
          AND qq.qualifier_attr_value = aps.vendor_id
      ) AS comt
        ON comt.vendor_name = cm.source_vendor_name
        AND comt.name = comt.vendor_name || '*' || cm.compile_designator
    )
    UNION ALL
    (
      SELECT
        cm.cost_type,
        cm.compile_designator AS plan_name,
        cm.data_type,
        cm.order_group,
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
            FROM silver_bec_ods.ap_suppliers
            WHERE
              is_deleted_flg <> 'Y'
          ) AS aps
          WHERE
            1 = 1
            AND qph.list_header_id = qpht.list_header_id
            AND qpht.LANGUAGE = 'US'
            AND qph.list_header_id = qpl.list_header_id
            AND qpl.list_line_id = qpa.list_line_id
            AND qpa.product_attr_value = cm.inventory_item_id_msi
            AND qph.list_header_id = qq.list_header_id
            AND qq.qualifier_attr_value = aps.vendor_id
            AND aps.vendor_name = cm.source_vendor_name
            AND qph.source_system_code = 'PO'
            AND CASE WHEN qpht.name = 'NEWARK' THEN 'NEWARK..' ELSE qpht.name END = CASE
              WHEN cm.compile_designator = 'BE-GLOBAL'
              THEN aps.vendor_name
              ELSE CASE
                WHEN comt.name IS NULL
                THEN aps.vendor_name
                ELSE aps.vendor_name || '*' || cm.compile_designator
              END
            END
            AND cm.new_dock_date BETWEEN qpl.start_date_active AND COALESCE(qpl.end_date_active, cm.new_dock_date + 1)
        ) AS order_type_meas,
        cm.order_number,
        cm.organization_id,
        cm.inventory_item_id,
        cm.new_dock_date1,
        cm.aging_period,
        FLOOR(cm.new_dock_date) AS new_dock_date,
        cm.item_segments,
        cm.description,
        cm.planning_make_buy_code,
        cm.category_name,
        cm.quantity,
        cm.primary_quantity,
        cm.primary_unit_of_measure,
        cm.po_line_type,
        cm.source_vendor_name,
        cm.mtl_cost,
        cm.ext_mtl_cost,
        cm.source_organization_id,
        cm.planner_code,
        cm.buyer_name,
        cm.transactional_uom_code,
        cm.release_num
      FROM CTE_main_stg AS cm
      LEFT OUTER JOIN (
        SELECT
          qpht.name,
          aps.vendor_name
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
          FROM silver_bec_ods.ap_suppliers
          WHERE
            is_deleted_flg <> 'Y'
        ) AS aps
        WHERE
          qph.list_header_id = qpht.list_header_id
          AND qpht.LANGUAGE = 'US'
          AND qph.list_header_id = qq.list_header_id
          AND qq.qualifier_attr_value = aps.vendor_id
      ) AS comt
        ON comt.vendor_name = cm.source_vendor_name
        AND comt.name = comt.vendor_name || '*' || cm.compile_designator
    )
  )
);
UPDATE bec_etl_ctrl.batch_dw_info SET load_type = 'I', last_refresh_date = CURRENT_TIMESTAMP()
WHERE
  dw_table_name = 'fact_rapa_stg2' AND batch_name = 'ascp';