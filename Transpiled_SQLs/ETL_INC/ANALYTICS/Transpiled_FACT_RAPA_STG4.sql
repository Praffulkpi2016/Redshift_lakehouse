TRUNCATE table gold_bec_dwh.FACT_RAPA_STG4;
WITH CTE_Main_STG AS (
  SELECT
    *,
    CASE
      WHEN po_unit_of_measure = Decodest1
      OR (
        po_unit_of_measure IS NULL AND Decodest1 IS NULL
      )
      THEN rcv_quantity_received
      ELSE Decodest2
    END AS ext_mtl_cost_cacl
  FROM (
    SELECT
      CAST('Frozen' AS STRING) AS cost_type,
      CAST(NULL AS STRING) AS plan_name,
      CAST('Receipt Actual' AS STRING) AS data_type,
      CAST('PO Receipt' AS STRING) AS order_group,
      CAST('PO Receipt' AS STRING) AS order_type_text,
      po_number AS order_number,
      r.organization_id,
      organization_code,
      FLOOR(promised_date) AS promised_date,
      0 AS aging_period,
      receipt_date,
      item_name AS part_number,
      item_description,
      CAST('Buy' AS STRING) AS planning_make_buy_code,
      category,
      msi.inventory_item_id,
      rcv_quantity_received,
      rcv_quantity_received * (
        COALESCE(
          (
            SELECT
              conversion_rate
            FROM silver_bec_ods.mtl_uom_conversions AS muc
            WHERE
              muc.is_deleted_flg <> 'Y'
              AND muc.unit_of_measure = r.po_unit_of_measure
              AND muc.inventory_item_id = msi.inventory_item_id
          ),
          COALESCE(
            (
              SELECT
                conversion_rate
              FROM silver_bec_ods.mtl_uom_conversions
              WHERE
                is_deleted_flg <> 'Y' AND unit_of_measure = r.po_unit_of_measure AND inventory_item_id = 0
            ),
            1
          )
        ) / COALESCE(
          (
            SELECT
              conversion_rate
            FROM silver_bec_ods.mtl_uom_conversions AS muc
            WHERE
              muc.is_deleted_flg <> 'Y'
              AND muc.unit_of_measure = r.primary_unit_of_measure
              AND muc.inventory_item_id = msi.inventory_item_id
          ),
          COALESCE(
            (
              SELECT
                conversion_rate
              FROM silver_bec_ods.mtl_uom_conversions
              WHERE
                is_deleted_flg <> 'Y'
                AND unit_of_measure = r.primary_unit_of_measure
                AND inventory_item_id = 0
            ),
            1
          )
        )
      ) AS primary_quantity,
      (
        SELECT
          price_override
        FROM silver_bec_ods.po_line_locations_all AS pll
        WHERE
          pll.is_deleted_flg <> 'Y' AND pll.line_location_id = r.line_location_id
      ) AS po_unit_price,
      r.primary_unit_of_measure AS primary_unit_of_measure,
      rcv_quantity_received * (
        SELECT
          price_override
        FROM silver_bec_ods.po_line_locations_all AS pll
        WHERE
          pll.is_deleted_flg <> 'Y' AND pll.line_location_id = r.line_location_id
      ) AS extended_po_rcv_price,
      po_line_type,
      vendor_name,
      CAST(NULL AS DECIMAL(15, 0)) AS source_organization_id,
      msi.planner_code,
      r.buyer_name,
      r.po_unit_of_measure,
      po_release_number,
      (
        SELECT
          unit_of_measure
        FROM silver_bec_ods.mtl_units_of_measure_tl
        WHERE
          is_deleted_flg <> 'Y' AND uom_code = r.primary_uom_code
      ) AS Decodest1,
      (
        SELECT
          MAX(primary_quantity)
        FROM silver_bec_ods.mtl_material_transactions AS mmt, (
          SELECT
            *
          FROM silver_bec_ods.po_line_locations_all
          WHERE
            is_deleted_flg <> 'Y'
        ) AS poll
        WHERE
          mmt.transaction_source_id = poll.po_header_id
          AND poll.line_location_id = r.line_location_id
          AND mmt.transaction_date = r.receipt_date
          AND mmt.transaction_quantity = r.rcv_quantity_received
      ) AS Decodest2,
      rcv_quantity_received * (
        SELECT
          price_override
        FROM silver_bec_ods.po_line_locations_all AS pll
        WHERE
          pll.is_deleted_flg <> 'Y' AND pll.line_location_id = r.line_location_id
      ) AS variance_cacl,
      ROW_NUMBER() OVER (PARTITION BY po_number) AS ROWNUMBER
    FROM silver_bec_ods.bec_actual_po_recpt1 AS r, (
      SELECT
        *
      FROM silver_bec_ods.mtl_system_items_b
      WHERE
        is_deleted_flg <> 'Y'
    ) AS msi
    WHERE
      r.item_name = msi.segment1
      AND r.organization_id = msi.organization_id /*    and receipt_date between '01-JUL-2019' and '01-JAN-2020' */
      AND NOT item_name IS NULL /*  and po_number = '10148551' */
      AND r.cvmi_flag = 'N'
      AND r.organization_id IN (106, 245, 265, 285)
    UNION ALL
    SELECT
      CAST('Pending' AS STRING) AS cost_type,
      CAST(NULL AS STRING) AS plan_name,
      CAST('Receipt Actual' AS STRING) AS data_type,
      CAST('PO Receipt' AS STRING) AS order_group,
      CAST('PO Receipt' AS STRING) AS order_type_text,
      po_number AS order_number,
      r.organization_id,
      organization_code,
      FLOOR(promised_date) AS promised_date,
      0 AS aging_period,
      receipt_date,
      item_name AS part_number,
      item_description,
      CAST('Buy' AS STRING) AS planning_make_buy_code,
      category,
      msi.inventory_item_id,
      rcv_quantity_received,
      rcv_quantity_received * (
        COALESCE(
          (
            SELECT
              conversion_rate
            FROM silver_bec_ods.mtl_uom_conversions AS muc
            WHERE
              muc.is_deleted_flg <> 'Y'
              AND muc.unit_of_measure = r.po_unit_of_measure
              AND muc.inventory_item_id = msi.inventory_item_id
          ),
          COALESCE(
            (
              SELECT
                conversion_rate
              FROM silver_bec_ods.mtl_uom_conversions
              WHERE
                is_deleted_flg <> 'Y' AND unit_of_measure = r.po_unit_of_measure AND inventory_item_id = 0
            ),
            1
          )
        ) / COALESCE(
          (
            SELECT
              conversion_rate
            FROM silver_bec_ods.mtl_uom_conversions AS muc
            WHERE
              muc.is_deleted_flg <> 'Y'
              AND muc.unit_of_measure = r.primary_unit_of_measure
              AND muc.inventory_item_id = msi.inventory_item_id
          ),
          COALESCE(
            (
              SELECT
                conversion_rate
              FROM silver_bec_ods.mtl_uom_conversions
              WHERE
                is_deleted_flg <> 'Y'
                AND unit_of_measure = r.primary_unit_of_measure
                AND inventory_item_id = 0
            ),
            1
          )
        )
      ) AS primary_quantity,
      (
        SELECT
          price_override
        FROM silver_bec_ods.po_line_locations_all AS pll
        WHERE
          pll.is_deleted_flg <> 'Y' AND pll.line_location_id = r.line_location_id
      ) AS po_unit_price,
      r.primary_unit_of_measure AS primary_unit_of_measure,
      rcv_quantity_received * (
        SELECT
          price_override
        FROM silver_bec_ods.po_line_locations_all AS pll
        WHERE
          pll.is_deleted_flg <> 'Y' AND pll.line_location_id = r.line_location_id
      ) AS extended_po_rcv_price,
      po_line_type,
      vendor_name,
      CAST(NULL AS DECIMAL(15, 0)) AS source_organization_id,
      msi.planner_code,
      r.buyer_name,
      r.po_unit_of_measure,
      po_release_number,
      (
        SELECT
          unit_of_measure
        FROM silver_bec_ods.mtl_units_of_measure_tl
        WHERE
          is_deleted_flg <> 'Y' AND uom_code = r.primary_uom_code
      ) AS Decodest1,
      (
        SELECT
          MAX(primary_quantity)
        FROM (
          SELECT
            *
          FROM silver_bec_ods.mtl_material_transactions
          WHERE
            is_deleted_flg <> 'Y'
        ) AS mmt, (
          SELECT
            *
          FROM silver_bec_ods.po_line_locations_all
          WHERE
            is_deleted_flg <> 'Y'
        ) AS poll
        WHERE
          mmt.transaction_source_id = poll.po_header_id
          AND poll.line_location_id = r.line_location_id
          AND mmt.transaction_date = r.receipt_date
          AND mmt.transaction_quantity = r.rcv_quantity_received
      ) AS Decodest2,
      rcv_quantity_received * (
        SELECT
          price_override
        FROM silver_bec_ods.po_line_locations_all AS pll
        WHERE
          pll.is_deleted_flg <> 'Y' AND pll.line_location_id = r.line_location_id
      ) AS variance_cacl,
      ROW_NUMBER() OVER (PARTITION BY po_number) AS ROWNUMBER
    FROM silver_bec_ods.bec_actual_po_recpt1 AS r, (
      SELECT
        *
      FROM silver_bec_ods.mtl_system_items_b
      WHERE
        is_deleted_flg <> 'Y'
    ) AS msi
    WHERE
      r.item_name = msi.segment1
      AND r.organization_id = msi.organization_id /*    and receipt_date between '01-JUL-2019' and '01-JAN-2020' */
      AND NOT item_name IS NULL /*  and po_number = '10148551' */
      AND r.cvmi_flag = 'N'
      AND r.organization_id IN (106, 245, 265, 285)
  )
)
INSERT INTO gold_bec_dwh.FACT_RAPA_STG4
(
  SELECT
    inventory_item_id,
    organization_id,
    cost_type,
    plan_name,
    data_type,
    order_group,
    order_type_text,
    order_number,
    promised_date, /* organization_id, */ /* organization_code, */
    aging_period,
    receipt_date,
    part_number,
    item_description,
    planning_make_buy_code,
    category AS category_name,
    rcv_quantity_received,
    primary_quantity,
    po_unit_price,
    primary_unit_of_measure,
    extended_po_rcv_price,
    po_line_type,
    vendor_name,
    mtl_cost,
    ext_mtl_cost,
    variance,
    source_organization_id,
    planner_code,
    buyer_name,
    po_unit_of_measure, /* vendor_country_code, */
    po_release_number,
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
    ) || '-' || COALESCE(inventory_item_id, 0) || '-' || COALESCE(organization_id, 0) AS dw_load_id,
    CURRENT_TIMESTAMP() AS dw_insert_date,
    CURRENT_TIMESTAMP() AS dw_update_date
  FROM (
    SELECT
      *
    FROM (
      SELECT
        inventory_item_id,
        organization_id,
        cost_type,
        plan_name,
        data_type,
        order_group,
        order_type_text,
        order_number,
        promised_date, /* organization_id, */ /* organization_code, */
        aging_period,
        FLOOR(receipt_date) AS receipt_date,
        part_number,
        item_description,
        planning_make_buy_code,
        category,
        rcv_quantity_received,
        primary_quantity,
        po_unit_price,
        primary_unit_of_measure,
        extended_po_rcv_price,
        po_line_type,
        vendor_name,
        mtl_cost,
        (
          mtl_cost * ext_mtl_cost_cacl
        ) AS ext_mtl_cost,
        (
          variance_cacl - ext_mtl_cost
        ) AS variance,
        source_organization_id,
        planner_code,
        buyer_name,
        po_unit_of_measure, /* vendor_country_code, */
        po_release_number
      /* cost_update_id, */ /* ROWNUMBER */
      FROM (
        /* cms.inventory_item_id, cms.organization_id, */
        SELECT
          cms.cost_type,
          cms.plan_name,
          cms.data_type,
          cms.order_group,
          cms.order_type_text,
          cms.order_number,
          cms.organization_id,
          cms.promised_date,
          cms.aging_period,
          cms.receipt_date,
          cms.part_number,
          cms.item_description,
          cms.planning_make_buy_code,
          cms.category,
          cms.inventory_item_id,
          cms.rcv_quantity_received,
          cms.primary_quantity,
          cms.po_unit_price,
          cms.primary_unit_of_measure,
          cms.extended_po_rcv_price,
          cms.po_line_type,
          cms.vendor_name,
          cms.source_organization_id,
          cms.planner_code,
          cms.buyer_name,
          cms.po_unit_of_measure,
          cms.po_release_number,
          cms.ext_mtl_cost_cacl,
          cms.variance_cacl,
          MAX(maxc.cost_update_id) AS cost_update_id,
          (
            SELECT
              standard_cost - COALESCE(material_overhead, 0)
            FROM silver_bec_ods.cst_cost_history_v AS ch
            WHERE
              cost_update_id = MAX(maxc.cost_update_id)
              AND ch.inventory_item_id = cms.inventory_item_id
              AND ch.last_update_date <= cms.receipt_date
              AND organization_id = cms.organization_id
          ) AS mtl_cost,
          cms.ROWNUMBER
        FROM CTE_Main_STG AS cms
        LEFT JOIN silver_bec_ods.cst_cost_history_v AS maxc
          ON maxc.inventory_item_id = cms.inventory_item_id
          AND maxc.organization_id = cms.organization_id
          AND maxc.last_update_date <= cms.receipt_date
        GROUP BY
          cms.cost_type,
          cms.plan_name,
          cms.data_type,
          cms.order_group,
          cms.order_type_text,
          cms.order_number,
          cms.organization_id,
          cms.organization_code,
          cms.promised_date,
          cms.aging_period,
          cms.receipt_date,
          cms.part_number,
          cms.item_description,
          cms.planning_make_buy_code,
          cms.category,
          cms.inventory_item_id,
          cms.rcv_quantity_received,
          cms.primary_quantity,
          cms.po_unit_price,
          cms.primary_unit_of_measure,
          cms.extended_po_rcv_price,
          cms.po_line_type,
          cms.vendor_name,
          cms.source_organization_id,
          cms.planner_code,
          cms.buyer_name,
          cms.po_unit_of_measure,
          cms.po_release_number,
          cms.ext_mtl_cost_cacl,
          cms.variance_cacl,
          cms.ROWNUMBER
      )
    )
  )
);
UPDATE bec_etl_ctrl.batch_dw_info SET load_type = 'I', last_refresh_date = CURRENT_TIMESTAMP()
WHERE
  dw_table_name = 'fact_rapa_stg4' AND batch_name = 'ascp';