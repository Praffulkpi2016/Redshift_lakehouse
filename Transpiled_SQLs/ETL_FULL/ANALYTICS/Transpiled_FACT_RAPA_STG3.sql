DROP table IF EXISTS gold_bec_dwh.FACT_RAPA_STG3;
CREATE TABLE gold_bec_dwh.FACT_RAPA_STG3 AS
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
    NULL AS category_name,
    po_open_qty,
    primary_quantity,
    unit_price,
    primary_unit_of_measure,
    po_open_amount,
    po_line_type,
    vendor_name,
    mtl_cost,
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
    ) || '-' || COALESCE(inventory_item_id, 0) || '-' || COALESCE(organization_id, 0) AS dw_load_id,
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
      po_number, /* ,ship_to_organization_id */
      promised_date1,
      aging_period, /* ,pd_cnt */ /* ,DATEDIFF(days,DATEADD(day, adj::integer  ,promised_date1),SYSDATE) */
      (
        CASE
          WHEN PD_CNT = 1
          THEN DATE_ADD(promised_date1, CAST(adj AS INT))
          WHEN PD_CNT = 2
          AND DATEDIFF(CURRENT_TIMESTAMP(), DATE_ADD(promised_date1, CAST(adj AS INT))) >= 0
          THEN CURRENT_TIMESTAMP()
          WHEN PD_CNT = 2
          AND DATEDIFF(CURRENT_TIMESTAMP(), DATE_ADD(promised_date1, CAST(adj AS INT))) < 0
          THEN DATE_ADD(promised_date1, CAST(adj AS INT))
          ELSE FLOOR(need_by_date)
        END
      ) AS promised_date, /* ,mod(postprocessing_lead_time,5) */
      purchase_item,
      item_description,
      planning_make_buy_code, /* ,category */
      po_open_qty,
      CAST((
        CAST((
          COALESCE(ordered_quantity * conversion_rate, 0)
        ) AS DECIMAL(38, 10)) - (
          COALESCE(quantity_received * conversion_rate, 0)
        )
      ) AS DECIMAL(38, 10)) AS primary_quantity,
      unit_price,
      primary_unit_of_measure,
      po_open_amount,
      po_line_type,
      vendor_name,
      mtl_cost,
      mtl_cost * (
        CAST((
          COALESCE(ordered_quantity * conversion_rate, 0)
        ) AS DECIMAL(38, 10)) - CAST((
          COALESCE(quantity_received * conversion_rate, 0)
        ) AS DECIMAL(38, 10))
      ) AS ext_mtl_cost,
      po_open_amount - (
        mtl_cost * (
          CAST((
            COALESCE(ordered_quantity * conversion_rate, 0)
          ) AS DECIMAL(38, 10)) - CAST((
            COALESCE(quantity_received * conversion_rate, 0)
          ) AS DECIMAL(38, 10))
        )
      ) AS VARIANCE,
      source_organization_id,
      planner_code,
      buyer_name, /* ,vendor_country_code */
      transactional_uom_code,
      release_num
    FROM (
      SELECT
        msib.inventory_item_id,
        'Frozen' AS cost_type,
        NULL AS plan_name,
        'Receipt Forecast' AS data_type,
        'Open PO' AS order_group,
        (
          CASE
            WHEN promised_date >= FLOOR(CURRENT_TIMESTAMP())
            THEN 'Open PO'
            WHEN promised_date < FLOOR(CURRENT_TIMESTAMP())
            THEN 'Past Due POs'
            WHEN promised_date IS NULL
            THEN 'Blank Promised Date POs'
          END
        ) AS order_type_text,
        po_number, /* 'Open PO'           order_type_text, */
        ship_to_organization_id,
        FLOOR(promised_date) AS promised_date1, /*  Null              org_code, */
        need_by_date,
        0 AS aging_period,
        postprocessing_lead_time,
        FLOOR(postprocessing_lead_time / 5) AS weeks,
        COALESCE(
          (
            CASE
              WHEN (
                EXTRACT(dayofweek FROM promised_date) + MOD(postprocessing_lead_time, 5) >= 7
              )
              THEN CAST((
                (
                  FLOOR(postprocessing_lead_time / 5) * 7
                ) + 2 + MOD(postprocessing_lead_time, 5)
              ) AS DECIMAL)
              ELSE CAST((
                (
                  FLOOR(postprocessing_lead_time / 5) * 7
                ) + MOD(postprocessing_lead_time, 5)
              ) AS DECIMAL)
            END
          ),
          0
        ) AS ADJ,
        (
          CASE
            WHEN (
              promised_date >= CURRENT_TIMESTAMP()
            )
            THEN 1
            WHEN (
              promised_date < CURRENT_TIMESTAMP()
            )
            THEN 2
            ELSE 3
          END
        ) AS pd_cnt,
        purchase_item, /*	5 days1, */
        pov.item_description,
        NULL AS planning_make_buy_code,
        NULL AS category,
        (
          COALESCE(ordered_quantity, 0) - COALESCE(quantity_received, 0)
        ) AS po_open_qty,
        ordered_quantity,
        quantity_received,
        (
          COALESCE(
            (
              SELECT
                conversion_rate
              FROM silver_bec_ods.mtl_uom_conversions AS muc
              WHERE
                is_deleted_flg <> 'Y'
                AND unit_of_measure = pov.po_uom
                AND muc.inventory_item_id = msib.inventory_item_id
            ),
            COALESCE(
              (
                SELECT
                  conversion_rate
                FROM silver_bec_ods.mtl_uom_conversions
                WHERE
                  is_deleted_flg <> 'Y' AND unit_of_measure = pov.po_uom AND inventory_item_id = 0
              ),
              1
            )
          ) / COALESCE(
            (
              SELECT
                conversion_rate
              FROM silver_bec_ods.mtl_uom_conversions AS muc
              WHERE
                is_deleted_flg <> 'Y'
                AND unit_of_measure = pov.std_uom
                AND muc.inventory_item_id = msib.inventory_item_id
            ),
            COALESCE(
              (
                SELECT
                  conversion_rate
                FROM silver_bec_ods.mtl_uom_conversions AS muc
                WHERE
                  is_deleted_flg <> 'Y' AND unit_of_measure = pov.std_uom AND muc.inventory_item_id = 0
              ),
              1
            )
          )
        ) AS conversion_rate,
        pov.unit_price,
        pov.std_uom AS primary_unit_of_measure,
        (
          COALESCE(ordered_quantity, 0) - COALESCE(quantity_received, 0)
        ) * unit_price AS po_open_amount,
        po_line_type,
        NULL AS vendor_name,
        (
          SELECT
            CIC.material_cost
          FROM silver_bec_ods.cst_item_costs AS cic
          WHERE
            cost_type_id = 1
            AND cic.organization_id = msib.organization_id
            AND cic.inventory_item_id = msib.inventory_item_id
        ) AS mtl_cost,
        CAST(NULL AS DECIMAL(15, 0)) AS source_organization_id,
        msib.planner_code AS planner_code,
        im_buyer AS buyer_name,
        pov.po_uom AS transactional_uom_code,
        release_num
      FROM gold_bec_dwh.FACT_PO_SHIPMENT_DETAILS AS pov, (
        SELECT
          *
        FROM silver_bec_ods.mtl_system_items_b
        WHERE
          is_deleted_flg <> 'Y'
      ) AS msib
      /* (select * from  silver_bec_ods.cst_item_costs     where is_deleted_flg <> 'Y')      cic, */ /* (select * from  silver_bec_ods.cst_cost_types     where is_deleted_flg <> 'Y')      ct */
      WHERE
        authorization_status IN ('APPROVED', 'PRE-APPROVED', 'REQUIRES REAPPROVAL') /*    AND ship_to_organization_id IN ( 106, 245, 265, 285 ) */
        AND cvmi_flag = 'N'
        AND shipment_status IN ('OPEN', 'CLOSED FOR INVOICE') /* AND promised_date >= floor(sysdate) */
        AND NOT purchase_item IS NULL
        AND pov.purchase_item = msib.segment1
        AND msib.organization_id = pov.ship_to_organization_id /* AND cic.organization_id(+) = msib.organization_id
      				AND cic.inventory_item_id(+) = msib.inventory_item_id
      				AND cic.cost_type_id         = ct.cost_type_id(+)
      				AND (ct.cost_type_id(+) = 1 AND ct.cost_type_id(+) = 3) */ /* AND PO_NUMBER in ( '10197714', -->=sysdate
      					'10000092', --promisedate is NULL
      					'004387')  --pd is < sysdate	
      				*/
      UNION ALL
      SELECT
        msib.inventory_item_id,
        'Pending' AS cost_type,
        NULL AS plan_name,
        'Receipt Forecast' AS data_type,
        'Open PO' AS order_group,
        (
          CASE
            WHEN promised_date >= FLOOR(CURRENT_TIMESTAMP())
            THEN 'Open PO'
            WHEN promised_date < FLOOR(CURRENT_TIMESTAMP())
            THEN 'Past Due POs'
            WHEN promised_date IS NULL
            THEN 'Blank Promised Date POs'
          END
        ) AS order_type_text,
        po_number, /* 'Open PO'           order_type_text, */
        ship_to_organization_id,
        FLOOR(promised_date) AS promised_date1, /*  Null              org_code, */
        need_by_date,
        0 AS aging_period,
        postprocessing_lead_time,
        FLOOR(postprocessing_lead_time / 5) AS weeks,
        COALESCE(
          (
            CASE
              WHEN (
                EXTRACT(dayofweek FROM promised_date) + MOD(postprocessing_lead_time, 5) >= 7
              )
              THEN CAST((
                (
                  FLOOR(postprocessing_lead_time / 5) * 7
                ) + 2 + MOD(postprocessing_lead_time, 5)
              ) AS DECIMAL)
              ELSE CAST((
                (
                  FLOOR(postprocessing_lead_time / 5) * 7
                ) + MOD(postprocessing_lead_time, 5)
              ) AS DECIMAL)
            END
          ),
          0
        ) AS ADJ,
        (
          CASE
            WHEN (
              promised_date >= CURRENT_TIMESTAMP()
            )
            THEN 1
            WHEN (
              promised_date < CURRENT_TIMESTAMP()
            )
            THEN 2
            ELSE 3
          END
        ) AS pd_cnt,
        purchase_item, /*	5 days1, */
        pov.item_description,
        NULL AS planning_make_buy_code,
        NULL AS category,
        (
          COALESCE(ordered_quantity, 0) - COALESCE(quantity_received, 0)
        ) AS po_open_qty,
        ordered_quantity,
        quantity_received,
        (
          COALESCE(
            (
              SELECT
                conversion_rate
              FROM silver_bec_ods.mtl_uom_conversions AS muc
              WHERE
                is_deleted_flg <> 'Y'
                AND unit_of_measure = pov.po_uom
                AND muc.inventory_item_id = msib.inventory_item_id
            ),
            COALESCE(
              (
                SELECT
                  conversion_rate
                FROM silver_bec_ods.mtl_uom_conversions
                WHERE
                  is_deleted_flg <> 'Y' AND unit_of_measure = pov.po_uom AND inventory_item_id = 0
              ),
              1
            )
          ) / COALESCE(
            (
              SELECT
                conversion_rate
              FROM silver_bec_ods.mtl_uom_conversions AS muc
              WHERE
                is_deleted_flg <> 'Y'
                AND unit_of_measure = pov.std_uom
                AND muc.inventory_item_id = msib.inventory_item_id
            ),
            COALESCE(
              (
                SELECT
                  conversion_rate
                FROM silver_bec_ods.mtl_uom_conversions AS muc
                WHERE
                  is_deleted_flg <> 'Y' AND unit_of_measure = pov.std_uom AND muc.inventory_item_id = 0
              ),
              1
            )
          )
        ) AS conversion_rate,
        pov.unit_price,
        pov.std_uom AS primary_unit_of_measure,
        (
          COALESCE(ordered_quantity, 0) - COALESCE(quantity_received, 0)
        ) * unit_price AS po_open_amount,
        po_line_type,
        NULL AS vendor_name,
        (
          SELECT
            CIC.material_cost
          FROM silver_bec_ods.cst_item_costs AS cic
          WHERE
            cost_type_id = 1
            AND cic.organization_id = msib.organization_id
            AND cic.inventory_item_id = msib.inventory_item_id
        ) AS mtl_cost,
        CAST(NULL AS DECIMAL(15, 0)) AS source_organization_id,
        msib.planner_code AS planner_code,
        im_buyer AS buyer_name,
        pov.po_uom AS transactional_uom_code,
        release_num
      FROM gold_bec_dwh.FACT_PO_SHIPMENT_DETAILS AS pov, (
        SELECT
          *
        FROM silver_bec_ods.mtl_system_items_b
        WHERE
          is_deleted_flg <> 'Y'
      ) AS msib
      /* (select * from  silver_bec_ods.cst_item_costs     where is_deleted_flg <> 'Y')      cic, */ /* (select * from  silver_bec_ods.cst_cost_types     where is_deleted_flg <> 'Y')      ct */
      WHERE
        authorization_status IN ('APPROVED', 'PRE-APPROVED', 'REQUIRES REAPPROVAL') /*    AND ship_to_organization_id IN ( 106, 245, 265, 285 ) */
        AND cvmi_flag = 'N'
        AND shipment_status IN ('OPEN', 'CLOSED FOR INVOICE') /* AND promised_date >= floor(sysdate) */
        AND NOT purchase_item IS NULL
        AND pov.purchase_item = msib.segment1
        AND msib.organization_id = pov.ship_to_organization_id
    )
  )
);
UPDATE bec_etl_ctrl.batch_dw_info SET load_type = 'I', last_refresh_date = CURRENT_TIMESTAMP()
WHERE
  dw_table_name = 'fact_rapa_stg3' AND batch_name = 'ascp';