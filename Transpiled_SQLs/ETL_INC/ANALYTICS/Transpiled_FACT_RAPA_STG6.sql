TRUNCATE table gold_bec_dwh.FACT_RAPA_STG6;
INSERT INTO gold_bec_dwh.FACT_RAPA_STG6
(
  SELECT
    INVENTORY_ITEM_ID,
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
    PART_NUMBER,
    description,
    planning_make_buy_code,
    NULL AS category_name,
    po_open_quantity,
    primary_quantity,
    price_override,
    primary_unit_of_measure,
    extended_cost,
    po_line_type,
    vendor_name,
    mtl_cost,
    ext_mtl_cost,
    variance,
    source_organization_id,
    planner_code,
    buyer_name,
    unit_meas_lookup_code,
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
    ) || '-' || COALESCE(cost_type, 'NA') || '-' || COALESCE(plan_name, 'NA') || '-' || COALESCE(promised_date1, '1900-01-01 12:00:00') || '-' || COALESCE(po_open_quantity, 0) || '-' || COALESCE(price_override, 0) AS dw_load_id,
    CURRENT_TIMESTAMP() AS dw_insert_date,
    CURRENT_TIMESTAMP() AS dw_update_date
  FROM (
    SELECT
      INVENTORY_ITEM_ID,
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
      PART_NUMBER,
      description,
      planning_make_buy_code,
      po_open_quantity,
      (
        COALESCE(NV1, COALESCE(NV2, 1)) / COALESCE(NV3, COALESCE(NV4, 1))
      ) AS conversion_percent,
      quantity * conversion_percent - quantity_received * conversion_percent - quantity_cancelled * conversion_percent AS primary_quantity,
      price_override,
      primary_unit_of_measure,
      extended_cost,
      po_line_type,
      vendor_name,
      mtl_cost,
      mtl_cost * (
        quantity * conversion_percent - quantity_received * conversion_percent - quantity_cancelled * conversion_percent
      ) AS ext_mtl_cost,
      extended_cost - (
        mtl_cost * (
          quantity * conversion_percent - quantity_received * conversion_percent - quantity_cancelled * conversion_percent
        )
      ) AS variance,
      source_organization_id,
      planner_code,
      buyer_name,
      unit_meas_lookup_code,
      release_num
    FROM (
      SELECT
        msi.inventory_item_id,
        poll.ship_to_organization_id AS organization_id,
        ct.cost_type,
        NULL AS plan_name,
        'Receipt Forecast' AS data_type,
        'CVMI Planned' AS order_group,
        'CVMI Shipment PO' AS order_type_text,
        poh.segment1 AS po_number,
        FLOOR(promised_date) AS promised_date1,
        pasl.aging_period,
        CASE
          WHEN SIGN(
            DATEDIFF(COALESCE(poll.promised_date, CURRENT_TIMESTAMP() - 1), CURRENT_TIMESTAMP())
          ) = -1
          OR (
            SIGN(
              DATEDIFF(COALESCE(poll.promised_date, CURRENT_TIMESTAMP() - 1), CURRENT_TIMESTAMP())
            ) IS NULL
            AND -1 IS NULL
          )
          THEN CURRENT_TIMESTAMP()
          ELSE FLOOR(COALESCE(poll.promised_date, CURRENT_TIMESTAMP() - 1))
        END AS promised_date,
        msi.segment1 AS PART_NUMBER,
        msi.description,
        CASE
          WHEN msi.planning_make_buy_code = 1
          THEN 'Make'
          WHEN msi.planning_make_buy_code = 2
          THEN 'Buy'
          ELSE NULL
        END AS planning_make_buy_code,
        (
          poll.quantity - poll.quantity_received - quantity_cancelled
        ) AS po_open_quantity,
        poll.quantity,
        poll.quantity_received,
        poll.QUANTITY_CANCELLED,
        (
          SELECT
            conversion_rate
          FROM (
            SELECT
              *
            FROM silver_bec_ods.mtl_uom_conversions
            WHERE
              is_deleted_flg <> 'Y'
          ) AS muc
          WHERE
            muc.unit_of_measure = pol.unit_meas_lookup_code
            AND muc.inventory_item_id = msi.inventory_item_id
        ) AS NV1,
        (
          SELECT
            conversion_rate
          FROM (
            SELECT
              *
            FROM silver_bec_ods.mtl_uom_conversions
            WHERE
              is_deleted_flg <> 'Y'
          ) AS muc
          WHERE
            muc.unit_of_measure = pol.unit_meas_lookup_code AND inventory_item_id = 0
        ) AS NV2,
        (
          SELECT
            conversion_rate
          FROM (
            SELECT
              *
            FROM silver_bec_ods.mtl_uom_conversions
            WHERE
              is_deleted_flg <> 'Y'
          ) AS muc
          WHERE
            muc.unit_of_measure = msi.primary_unit_of_measure
            AND muc.inventory_item_id = msi.inventory_item_id
        ) AS NV3,
        (
          SELECT
            conversion_rate
          FROM (
            SELECT
              *
            FROM silver_bec_ods.mtl_uom_conversions
            WHERE
              is_deleted_flg <> 'Y'
          ) AS muc
          WHERE
            muc.unit_of_measure = msi.primary_unit_of_measure AND inventory_item_id = 0
        ) AS NV4,
        price_override,
        msi.primary_unit_of_measure,
        price_override * (
          poll.quantity - poll.quantity_received - poll.QUANTITY_CANCELLED
        ) AS extended_cost,
        NULL AS po_line_type,
        aps.vendor_name,
        cic.material_cost AS mtl_cost,
        CAST(NULL AS DECIMAL(15, 0)) AS source_organization_id,
        msi.planner_code,
        papf.full_name AS buyer_name,
        pol.unit_meas_lookup_code,
        por.release_num
      FROM (
        SELECT
          *
        FROM silver_bec_ods.po_line_locations_all
        WHERE
          is_deleted_flg <> 'Y'
      ) AS poll, (
        SELECT
          *
        FROM silver_bec_ods.po_headers_all
        WHERE
          is_deleted_flg <> 'Y'
      ) AS poh, (
        SELECT
          *
        FROM silver_bec_ods.po_lines_all
        WHERE
          is_deleted_flg <> 'Y'
      ) AS pol, (
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
      ) AS aps, (
        SELECT
          *
        FROM silver_bec_ods.po_asl_attributes
        WHERE
          is_deleted_flg <> 'Y'
      ) AS pasl, (
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
        FROM silver_bec_ods.po_releases_all
        WHERE
          is_deleted_flg <> 'Y'
      ) AS por, (
        SELECT
          *
        FROM silver_bec_ods.per_all_people_f
        WHERE
          is_deleted_flg <> 'Y'
      ) AS papf
      WHERE
        poll.po_header_id = poh.po_header_id
        AND poll.po_line_id = pol.po_line_id
        AND pol.item_id = msi.inventory_item_id
        AND poh.vendor_id = aps.vendor_id
        AND poll.ship_to_organization_id = msi.organization_id
        AND poll.consigned_flag = 'Y'
        AND COALESCE(poll.closed_code, 'OPEN') <> 'CLOSED'
        AND (
          poll.quantity - poll.quantity_received - poll.QUANTITY_CANCELLED
        ) > 0
        AND NOT pasl.aging_period IS NULL
        AND pasl.using_organization_id = poll.ship_to_organization_id
        AND pasl.vendor_id = poh.vendor_id
        AND pasl.vendor_site_id = poh.vendor_site_id
        AND pasl.item_id = pol.item_id
        AND poll.po_release_id = por.PO_RELEASE_ID()
        AND cic.organization_id = poll.ship_to_organization_id
        AND cic.inventory_item_id = pol.item_id
        AND cic.cost_type_id = ct.cost_type_id
        AND ct.cost_type_id IN (1, 3)
        AND msi.buyer_id = papf.PERSON_ID()
    )
  )
);
UPDATE bec_etl_ctrl.batch_dw_info SET load_type = 'I', last_refresh_date = CURRENT_TIMESTAMP()
WHERE
  dw_table_name = 'fact_rapa_stg6' AND batch_name = 'ascp';