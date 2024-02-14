TRUNCATE table gold_bec_dwh.FACT_RAPA_STG1;
INSERT INTO gold_bec_dwh.FACT_RAPA_STG1
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
    new_dock_date1,
    aging_period,
    dock_promised_date,
    part_number,
    description,
    planning_make_buy_code,
    NULL AS category_name,
    quantity,
    primary_quantity,
    unit_cost,
    primary_uom_code,
    extended_cost,
    po_line_type,
    VENDOR_NAME,
    mtl_cost,
    ext_mtl_cost,
    variance,
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
    ) || '-' || inventory_item_id AS inventory_item_id_KEY,
    (
      SELECT
        system_id
      FROM bec_etl_ctrl.etlsourceappid
      WHERE
        source_system = 'EBS'
    ) || '-' || organization_id AS organization_id_KEY,
    (
      SELECT
        system_id
      FROM bec_etl_ctrl.etlsourceappid
      WHERE
        source_system = 'EBS'
    ) || '-' || source_organization_id AS source_organization_id_KEY,
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
    ) || '-' || COALESCE(cost_type, 'NA') || '-' || COALESCE(plan_name, 'NA') || '-' || COALESCE(new_dock_date1, '1900-01-01 12:00:00') || '-' || COALESCE(quantity, 0) || '-' || COALESCE(unit_cost, 0) AS dw_load_id,
    CURRENT_TIMESTAMP() AS dw_insert_date,
    CURRENT_TIMESTAMP() AS dw_update_date
  FROM (
    SELECT
      inventory_item_id,
      organization_id,
      cost_type,
      plan_name,
      data_type,
      order_group,
      (
        CASE
          WHEN (
            NOT unit_price IS NULL AND osign >= 0
          )
          THEN 'CVMI Future Consumption'
          WHEN NOT operand IS NULL
          THEN 'CVMI Future Consumption'
          ELSE 'CVMI Future Consumption (Mat\'l Cost)'
        END
      ) AS order_type_text,
      order_number AS po_number,
      new_dock_date1,
      aging_period,
      dock_promised_date,
      part_number,
      description,
      planning_make_buy_code,
      CASE
        WHEN cvmi_count = 0
        THEN future_consumptions * allocation_percent
        ELSE future_consumptions
      END AS quantity,
      CAST(NULL AS DECIMAL(38, 10)) AS primary_quantity,
      COALESCE(
        CASE
          WHEN osign = -1 OR (
            osign IS NULL AND -1 IS NULL
          )
          THEN OPERAND
          WHEN osign = 0
          THEN UNIT_PRICE
          ELSE UNIT_PRICE
        END,
        mtl_cost
      ) AS unit_cost,
      primary_uom_code,
      (
        CAST((
          COALESCE(
            CASE
              WHEN osign = -1 OR (
                osign IS NULL AND -1 IS NULL
              )
              THEN OPERAND
              WHEN osign = 0
              THEN UNIT_PRICE
              ELSE UNIT_PRICE
            END,
            mtl_cost
          )
        ) AS DECIMAL(38, 10)) * CAST((
          CASE
            WHEN cvmi_count = 0
            THEN future_consumptions * allocation_percent
            ELSE future_consumptions
          END
        ) AS DECIMAL(38, 10))
      ) AS extended_cost,
      po_line_type,
      VENDOR_NAME,
      mtl_cost,
      (
        mtl_cost * CAST((
          CASE
            WHEN cvmi_count = 0
            THEN future_consumptions * allocation_percent
            ELSE future_consumptions
          END
        ) AS DECIMAL(38, 10))
      ) AS ext_mtl_cost,
      (
        COALESCE(extended_cost - ext_mtl_cost, 0)
      ) AS variance,
      source_organization_id,
      planner_code,
      buyer_name,
      transactional_uom_code,
      release_num
    FROM (
      SELECT
        cost_type,
        cvmi.plan_name,
        'Receipt Forecast' AS data_type,
        'CVMI Planned' AS order_group,
        POL.unit_price,
        SIGN(
          CAST(LAST_DAY(ADD_MONTHS(DATE_FLOOR('quarter', CURRENT_TIMESTAMP()), 2)) AS DATE) - CAST(as_of_date AS DATE)
        ) AS osign,
        (
          SELECT
            MAX(qpl.operand)
          FROM (
            SELECT
              B.list_header_id,
              B.source_system_code,
              T.name
            FROM (
              SELECT
                *
              FROM silver_bec_ods.QP_LIST_HEADERS_TL
              WHERE
                is_deleted_flg <> 'Y'
            ) AS T, (
              SELECT
                *
              FROM silver_bec_ods.QP_LIST_HEADERS_B
              WHERE
                is_deleted_flg <> 'Y'
            ) AS B
            WHERE
              B.LIST_HEADER_ID = T.LIST_HEADER_ID AND T.LANGUAGE = 'US'
          ) AS qph, (
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
            qph.list_header_id = qpl.list_header_id
            AND qpl.list_line_id = qpa.list_line_id
            AND qpa.product_attr_value = CAST(msi1.inventory_item_id AS STRING)
            AND qph.list_header_id = qq.list_header_id
            AND qq.qualifier_attr_value = aps.vendor_id
            AND aps1.vendor_name = aps.vendor_name
            AND msi1.organization_id = 90
            AND qph.source_system_code = 'PO'
            AND qph.name = CASE
              WHEN plan_name = 'BE-GLOBAL'
              THEN aps.vendor_name
              ELSE CASE
                WHEN (
                  SELECT
                    name
                  FROM (
                    SELECT
                      T.name
                    FROM (
                      SELECT
                        *
                      FROM silver_bec_ods.QP_LIST_HEADERS_TL
                      WHERE
                        is_deleted_flg <> 'Y'
                    ) AS T, (
                      SELECT
                        *
                      FROM silver_bec_ods.QP_LIST_HEADERS_B
                      WHERE
                        is_deleted_flg <> 'Y'
                    ) AS B
                    WHERE
                      B.LIST_HEADER_ID = T.LIST_HEADER_ID AND T.LANGUAGE = 'US'
                  )
                  WHERE
                    name = aps.vendor_name || '*' || plan_name
                ) IS NULL
                THEN aps.vendor_name
                ELSE aps.vendor_name || '*' || plan_name
              END
            END
            AND msi1.segment1 = cvmi.part_number
            AND as_of_date BETWEEN qpl.start_date_active AND COALESCE(qpl.end_date_active, as_of_date + 1)
        ) AS operand,
        NULL AS order_number,
        msi.organization_id,
        msi.inventory_item_id,
        as_of_date AS new_dock_date1,
        aging_period,
        as_of_date AS dock_promised_date,
        part_number,
        msi.description,
        CASE
          WHEN planning_make_buy_code = 1
          THEN 'Make'
          WHEN planning_make_buy_code = 2
          THEN 'Buy'
        END AS planning_make_buy_code,
        (
          SELECT
            COUNT(COALESCE(bqoh, 0))
          FROM (
            SELECT
              *
            FROM silver_bec_ods.xxbec_cvmi_cons_cal_final
            WHERE
              is_deleted_flg <> 'Y'
          ) AS x
          WHERE
            as_of_date >= cvmi.as_of_date
            AND COALESCE(bqoh, 0) > 0
            AND x.part_number = cvmi.part_number
            AND x.organization_id = cvmi.organization_id
        ) AS cvmi_count,
        future_consumptions,
        (
          SELECT
            MAX(sso.allocation_percent) / 100
          FROM (
            SELECT
              *
            FROM silver_bec_ods.mrp_sourcing_rules
            WHERE
              is_deleted_flg <> 'Y'
          ) AS sr, (
            SELECT
              *
            FROM silver_bec_ods.mrp_sr_assignments
            WHERE
              is_deleted_flg <> 'Y'
          ) AS sra, (
            SELECT
              *
            FROM silver_bec_ods.mrp_sr_receipt_org
            WHERE
              is_deleted_flg <> 'Y'
          ) AS sro, (
            SELECT
              *
            FROM silver_bec_ods.mrp_sr_source_org
            WHERE
              is_deleted_flg <> 'Y'
          ) AS sso, (
            SELECT
              *
            FROM silver_bec_ods.mtl_system_items_b
            WHERE
              is_deleted_flg <> 'Y'
          ) AS msi1
          WHERE
            sso.source_organization_id IS NULL
            AND sr.sourcing_rule_id = sro.SOURCING_RULE_ID()
            AND sr.sourcing_rule_id = sra.sourcing_rule_id
            AND sro.sr_receipt_id = sso.SR_RECEIPT_ID()
            AND sra.inventory_item_id = msi1.inventory_item_id
            AND msi1.organization_id = 90
            AND msi1.inventory_item_id IN (
              SELECT DISTINCT
                paa.item_id
              FROM (
                SELECT
                  *
                FROM silver_bec_ods.po_asl_attributes
                WHERE
                  is_deleted_flg <> 'Y'
              ) AS paa, (
                SELECT
                  *
                FROM silver_bec_ods.po_approved_supplier_list
                WHERE
                  is_deleted_flg <> 'Y'
              ) AS pasl
              WHERE
                consigned_from_supplier_flag = 'Y'
                AND pasl.asl_id = paa.asl_id
                AND COALESCE(pasl.disable_flag, 'N') = 'N'
            )
            AND msi1.inventory_item_id = msi.inventory_item_id
            AND cvmi.as_of_date BETWEEN sro.effective_date AND COALESCE(sro.disable_date, CURRENT_TIMESTAMP() + 1500)
            AND sra.organization_id = cvmi.organization_id
            AND sso.vendor_id = aps.vendor_id
        ) AS allocation_percent,
        NULL,
        msi.primary_unit_of_measure AS primary_uom_code,
        pol.line_type_id AS po_line_type_id,
        plt.line_type AS po_line_type,
        aps.vendor_name,
        cic.material_cost AS mtl_cost,
        CAST(NULL AS DECIMAL(15, 0)) AS source_organization_id,
        msi.planner_code,
        msi.buyer_id,
        papf.full_name AS buyer_name,
        NULL AS transactional_uom_code,
        CAST(NULL AS DECIMAL(15, 0)) AS release_num
      FROM (
        SELECT
          *
        FROM silver_bec_ods.xxbec_cvmi_cons_cal_final
        WHERE
          is_deleted_flg <> 'Y'
      ) AS cvmi, (
        SELECT
          *
        FROM silver_bec_ods.mtl_system_items_b
        WHERE
          is_deleted_flg <> 'Y'
      ) AS msi, (
        SELECT
          *
        FROM silver_bec_ods.po_asl_attributes
        WHERE
          is_deleted_flg <> 'Y'
      ) AS paa, (
        SELECT
          *
        FROM silver_bec_ods.po_approved_supplier_list
        WHERE
          is_deleted_flg <> 'Y'
      ) AS pasl, (
        SELECT
          *
        FROM silver_bec_ods.ap_suppliers
        WHERE
          is_deleted_flg <> 'Y'
      ) AS aps, (
        SELECT
          *
        FROM silver_bec_ods.cst_item_costs
        WHERE
          is_deleted_flg <> 'Y'
      ) AS cic, (
        SELECT
          *
        FROM silver_bec_ods.cst_cost_types
        WHERE
          is_deleted_flg <> 'Y'
      ) AS ct, (
        SELECT
          *
        FROM silver_bec_ods.po_asl_documents
        WHERE
          is_deleted_flg <> 'Y'
      ) AS pasld, (
        SELECT
          *
        FROM silver_bec_ods.po_lines_all
        WHERE
          is_deleted_flg <> 'Y'
      ) AS pol, (
        SELECT
          *
        FROM silver_bec_ods.po_line_types_vl
        WHERE
          is_deleted_flg <> 'Y'
      ) AS plt, (
        SELECT
          *
        FROM silver_bec_ods.per_all_people_f
        WHERE
          is_deleted_flg <> 'Y'
      ) AS papf
      WHERE
        cvmi.organization_id = msi.organization_id
        AND cvmi.part_number = msi.segment1
        AND pasl.asl_id = paa.asl_id
        AND paa.item_id = msi.inventory_item_id
        AND paa.using_organization_id = msi.organization_id
        AND consigned_from_supplier_flag = 'Y'
        AND pasl.asl_id = paa.asl_id
        AND COALESCE(pasl.disable_flag, 'N') = 'N'
        AND pasl.vendor_id = aps.vendor_id
        AND msi.organization_id = cic.organization_id
        AND msi.inventory_item_id = cic.inventory_item_id
        AND cic.cost_type_id = ct.COST_TYPE_ID()
        AND pasl.asl_id = pasld.ASL_ID()
        AND pasld.document_line_id = pol.PO_LINE_ID()
        AND pol.line_type_id = plt.LINE_TYPE_ID()
        AND aps.vendor_name <> 'PANWELL INTERNATIONAL CO. LTD'
        AND msi.buyer_id = papf.PERSON_ID()
    )
  )
);
UPDATE bec_etl_ctrl.batch_dw_info SET load_type = 'I', last_refresh_date = CURRENT_TIMESTAMP()
WHERE
  dw_table_name = 'fact_rapa_stg1' AND batch_name = 'ascp';