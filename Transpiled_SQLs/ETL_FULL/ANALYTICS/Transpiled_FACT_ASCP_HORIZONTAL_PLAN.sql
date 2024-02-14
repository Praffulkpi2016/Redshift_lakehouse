DROP table IF EXISTS gold_bec_dwh.FACT_ASCP_HORIZONTAL_PLAN;
CREATE TABLE gold_bec_dwh.fact_ascp_horizontal_plan AS
(
  SELECT
    A.inventory_item_id,
    A.organization_id,
    A.plan_id,
    A.category_set_id,
    A.sr_instance_id,
    A.fill_kill_flag,
    A.so_line_split,
    A.quantity_rate,
    A.new_due_date,
    A.order_type_entity,
    (
      SELECT
        system_id
      FROM bec_etl_ctrl.etlsourceappid
      WHERE
        source_system = 'EBS'
    ) || '-' || A.plan_id AS plan_id_KEY,
    (
      SELECT
        system_id
      FROM bec_etl_ctrl.etlsourceappid
      WHERE
        source_system = 'EBS'
    ) || '-' || A.ORGANIZATION_ID AS ORGANIZATION_ID_KEY,
    (
      SELECT
        system_id
      FROM bec_etl_ctrl.etlsourceappid
      WHERE
        source_system = 'EBS'
    ) || '-' || A.inventory_item_id AS inventory_item_id_KEY,
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
    ) || '-' || COALESCE(A.plan_id, 0) || '-' || COALESCE(A.ORGANIZATION_ID, 0) || '-' || COALESCE(A.inventory_item_id, 0) || '-' || COALESCE(A.category_set_id, 0) || '-' || COALESCE(A.new_due_date, CAST('01-01-1900' AS TIMESTAMP)) || '-' || COALESCE(A.order_type_entity, 'NA') AS dw_load_id,
    CURRENT_TIMESTAMP() AS dw_insert_date,
    CURRENT_TIMESTAMP() AS dw_update_date
  FROM (
    (
      WITH order_types AS (
        SELECT
          'Gross requirements' AS order_type_entity
        UNION ALL
        SELECT
          'Total supply'
        UNION ALL
        SELECT
          'Current scheduled receipts'
        UNION ALL
        SELECT
          'Projected available balance'
        UNION ALL
        SELECT
          'Projected on hand'
        UNION ALL
        SELECT
          'Sales orders'
        UNION ALL
        SELECT
          'Forecast'
        UNION ALL
        SELECT
          'Dependent demand'
        UNION ALL
        SELECT
          'Expected scrap'
        UNION ALL
        SELECT
          'Work orders'
        UNION ALL
        SELECT
          'Purchase orders'
        UNION ALL
        SELECT
          'Requisitions/ CVMI Consumtion Plan'
        UNION ALL
        SELECT
          'In Transit'
        UNION ALL
        SELECT
          'In Receiving'
        UNION ALL
        SELECT
          'Planned orders'
        UNION ALL
        SELECT
          'Beginning on hand'
        UNION ALL
        SELECT
          'Safety stock'
      ), Gross_Requirements AS (
        SELECT
          mo.inventory_item_id,
          mo.organization_id,
          mo.plan_id,
          mo.category_set_id,
          mo.sr_instance_id,
          mo.fill_kill_flag,
          mo.so_line_split,
          ABS(SUM(quantity_rate)) AS quantity_rate,
          FLOOR(mo.new_due_date) AS new_due_date,
          'Gross requirements' AS order_type_entity
        FROM BEC_ODS.msc_orders_v AS mo, gold_bec_dwh.DIM_ASCP_PLANS AS PLANS
        WHERE
          (
            (
              NOT mo.order_type IN (18, 6, 7, 30, 31)
            )
            OR (
              mo.order_type IN (18, 6, 7, 30) AND mo.quantity_rate <> 0
            )
            OR (
              mo.order_type = 30 AND (
                mo.fill_kill_flag = 1 OR mo.so_line_split = 1
              )
            )
            OR (
              mo.order_type = 31 AND mo.quantity <> 0
            )
          )
          AND mo.category_set_id = 9
          AND order_type <> 60
          AND action <> 'Cancel'
          AND SOURCE_TABLE = 'MSC_DEMANDS'
          AND mo.plan_id = plans.plan_id
          AND mo.sr_instance_id = plans.sr_instance_id
          AND plans.LOAD_FLG = 'Y'
        GROUP BY
          mo.inventory_item_id,
          mo.organization_id,
          mo.plan_id,
          mo.category_set_id,
          mo.sr_instance_id,
          FLOOR(mo.new_due_date),
          mo.fill_kill_flag,
          mo.so_line_split
      ), Total_Supply AS (
        SELECT
          mo.inventory_item_id,
          mo.organization_id,
          mo.plan_id,
          mo.category_set_id,
          mo.sr_instance_id,
          mo.fill_kill_flag,
          mo.so_line_split,
          SUM(quantity_rate) AS quantity_rate,
          FLOOR(mo.new_due_date) AS new_due_date,
          'Total supply' AS order_type_entity
        FROM BEC_ODS.msc_orders_v AS mo, gold_bec_dwh.DIM_ASCP_PLANS AS plans
        WHERE
          (
            (
              NOT mo.order_type IN (18, 6, 7, 30, 31)
            )
            OR (
              mo.order_type IN (18, 6, 7, 30) AND mo.quantity_rate <> 0
            )
            OR (
              mo.order_type = 30 AND (
                mo.fill_kill_flag = 1 OR mo.so_line_split = 1
              )
            )
            OR (
              mo.order_type = 31 AND mo.quantity <> 0
            )
          )
          AND mo.category_set_id = 9
          AND order_type <> 60
          AND action <> 'Cancel'
          AND SOURCE_TABLE = 'MSC_SUPPLIES'
          AND mo.plan_id = plans.plan_id
          AND mo.sr_instance_id = plans.sr_instance_id
          AND plans.LOAD_FLG = 'Y'
        GROUP BY
          mo.inventory_item_id,
          mo.organization_id,
          mo.plan_id,
          mo.category_set_id,
          mo.sr_instance_id,
          FLOOR(mo.new_due_date),
          mo.fill_kill_flag,
          mo.so_line_split
      ), Current_Scheduled_Receipts AS (
        SELECT
          mo.inventory_item_id,
          mo.organization_id,
          mo.plan_id,
          mo.category_set_id,
          mo.sr_instance_id,
          mo.fill_kill_flag,
          mo.so_line_split,
          SUM(quantity_rate) AS quantity_rate,
          CASE
            WHEN mo.OLD_DUE_DATE < plans.CURR_START_DATE
            THEN FLOOR(plans.CURR_START_DATE - 1)
            ELSE FLOOR(mo.OLD_DUE_DATE)
          END AS new_due_date,
          'Current scheduled receipts' AS order_type_entity
        FROM BEC_ODS.msc_orders_v AS mo, gold_bec_dwh.DIM_ASCP_PLANS AS plans
        WHERE
          (
            (
              NOT mo.order_type IN (18, 6, 7, 30, 31)
            )
            OR (
              mo.order_type IN (18, 6, 7, 30) AND mo.quantity_rate <> 0
            )
            OR (
              mo.order_type = 30 AND (
                mo.fill_kill_flag = 1 OR mo.so_line_split = 1
              )
            )
            OR (
              mo.order_type = 31 AND mo.quantity <> 0
            )
          )
          AND mo.category_set_id = 9
          AND order_type <> 60
          AND (
            (
              SOURCE_TABLE = 'MSC_SUPPLIES' AND action = 'Cancel'
            )
            OR (
              SOURCE_TABLE = 'MSC_SUPPLIES' AND NOT OLD_DUE_DATE IS NULL
            )
          )
          AND ORDER_TYPE_TEXT <> 'On Hand'
          AND mo.plan_id = plans.plan_id
          AND mo.sr_instance_id = plans.sr_instance_id
          AND plans.LOAD_FLG = 'Y'
        GROUP BY
          mo.inventory_item_id,
          mo.organization_id,
          mo.plan_id,
          mo.category_set_id,
          mo.sr_instance_id,
          mo.fill_kill_flag,
          mo.so_line_split,
          CASE
            WHEN mo.OLD_DUE_DATE < plans.CURR_START_DATE
            THEN FLOOR(plans.CURR_START_DATE - 1)
            ELSE FLOOR(mo.OLD_DUE_DATE)
          END
      ), prj_onhand_supply AS (
        SELECT
          mo.inventory_item_id,
          mo.organization_id,
          mo.plan_id,
          mo.category_set_id,
          mo.sr_instance_id,
          mo.fill_kill_flag,
          mo.so_line_split,
          SUM(quantity_rate) AS quantity_rate,
          FLOOR(mo.new_due_date) AS new_due_date,
          'Total supply' AS order_type_entity
        FROM BEC_ODS.msc_orders_v AS mo, gold_bec_dwh.DIM_ASCP_PLANS AS plans
        WHERE
          (
            (
              NOT mo.order_type IN (18, 6, 7, 30, 31)
            )
            OR (
              mo.order_type IN (18, 6, 7, 30) AND mo.quantity_rate <> 0
            )
            OR (
              mo.order_type = 30 AND (
                mo.fill_kill_flag = 1 OR mo.so_line_split = 1
              )
            )
            OR (
              mo.order_type = 31 AND mo.quantity <> 0
            )
          )
          AND mo.category_set_id = 9
          AND order_type <> 60
          AND action <> 'Cancel'
          AND SOURCE_TABLE = 'MSC_SUPPLIES'
          AND order_type_text IN ('Non-standard job', 'Nonstandard job by-product', 'Work order co-product/by-product', 'Work order', 'On Hand')
          AND mo.plan_id = plans.plan_id
          AND mo.sr_instance_id = plans.sr_instance_id
          AND plans.LOAD_FLG = 'Y'
        GROUP BY
          mo.inventory_item_id,
          mo.organization_id,
          mo.plan_id,
          mo.category_set_id,
          mo.sr_instance_id,
          FLOOR(mo.new_due_date),
          mo.fill_kill_flag,
          mo.so_line_split
      ), prj_available_bal AS (
        SELECT
          COALESCE(gr.inventory_item_id, ts.inventory_item_id) AS inventory_item_id,
          COALESCE(gr.organization_id, ts.organization_id) AS organization_id,
          COALESCE(gr.plan_id, ts.plan_id) AS plan_id,
          COALESCE(gr.category_set_id, ts.category_set_id) AS category_set_id,
          COALESCE(gr.sr_instance_id, ts.sr_instance_id) AS sr_instance_id,
          gr.fill_kill_flag,
          gr.so_line_split,
          COALESCE(ts.quantity_rate, 0) - COALESCE(gr.quantity_rate, 0) AS quantity_rate,
          COALESCE(gr.new_due_date, ts.new_due_date) AS new_due_date, /* sum((NVL(ts.quantity_rate,0) - NVL(gr.quantity_rate,0))) OVER (ORDER BY COALESCE(gr.new_due_date,ts.new_due_date))  qty, */
          'Projected available balance' AS order_type_entity
        FROM Gross_Requirements AS gr
        FULL OUTER JOIN Total_Supply AS ts
          ON gr.inventory_item_id = ts.inventory_item_id
          AND gr.organization_id = ts.organization_id
          AND gr.plan_id = ts.plan_id
          AND gr.category_set_id = ts.category_set_id
          AND gr.sr_instance_id = ts.sr_instance_id
          AND gr.new_due_date = ts.new_due_date
      ), prj_onhand_bal AS (
        SELECT
          COALESCE(gr.inventory_item_id, ts.inventory_item_id, csr.inventory_item_id) AS inventory_item_id,
          COALESCE(gr.organization_id, ts.organization_id, csr.organization_id) AS organization_id,
          COALESCE(gr.plan_id, ts.plan_id, csr.plan_id) AS plan_id,
          COALESCE(gr.category_set_id, ts.category_set_id, csr.category_set_id) AS category_set_id,
          COALESCE(gr.sr_instance_id, ts.sr_instance_id, csr.sr_instance_id) AS sr_instance_id,
          gr.fill_kill_flag,
          gr.so_line_split,
          (
            COALESCE(csr.quantity_rate, 0) + COALESCE(ts.quantity_rate, 0) - COALESCE(gr.quantity_rate, 0)
          ) AS quantity_rate,
          COALESCE(gr.new_due_date, ts.new_due_date, csr.new_due_date) AS new_due_date,
          'Projected on hand' AS order_type_entity
        FROM Gross_Requirements AS gr
        FULL OUTER JOIN prj_onhand_supply AS ts
          ON gr.inventory_item_id = ts.inventory_item_id
          AND gr.organization_id = ts.organization_id
          AND gr.plan_id = ts.plan_id
          AND gr.category_set_id = ts.category_set_id
          AND gr.sr_instance_id = ts.sr_instance_id
          AND gr.new_due_date = ts.new_due_date
        FULL OUTER JOIN Current_Scheduled_Receipts AS CSR
          ON COALESCE(gr.inventory_item_id, ts.inventory_item_id) = CSR.inventory_item_id
          AND COALESCE(gr.organization_id, ts.organization_id) = CSR.organization_id
          AND COALESCE(gr.plan_id, ts.plan_id) = CSR.plan_id
          AND COALESCE(gr.category_set_id, ts.category_set_id) = CSR.category_set_id
          AND COALESCE(gr.sr_instance_id, ts.sr_instance_id) = CSR.sr_instance_id
          AND COALESCE(gr.new_due_date, ts.new_due_date) = CSR.new_due_date
      )
      SELECT
        mo.inventory_item_id,
        mo.organization_id,
        mo.plan_id,
        mo.category_set_id,
        mo.sr_instance_id,
        mo.fill_kill_flag,
        mo.so_line_split,
        ABS(SUM(quantity_rate)) AS quantity_rate,
        FLOOR(mo.new_due_date) AS new_due_date,
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
        END AS order_type_entity
      FROM BEC_ODS.msc_orders_v AS mo, gold_bec_dwh.DIM_ASCP_PLANS AS plans
      WHERE
        (
          (
            NOT mo.order_type IN (18, 6, 7, 30, 31)
          )
          OR (
            mo.order_type IN (18, 6, 7, 30) AND mo.quantity_rate <> 0
          )
          OR (
            mo.order_type = 30 AND (
              mo.fill_kill_flag = 1 OR mo.so_line_split = 1
            )
          )
          OR (
            mo.order_type = 31 AND mo.quantity <> 0
          )
        )
        AND mo.category_set_id = 9
        AND order_type <> 60
        AND action <> 'Cancel'
        AND mo.plan_id = plans.plan_id
        AND mo.sr_instance_id = plans.sr_instance_id
        AND plans.LOAD_FLG = 'Y'
      GROUP BY
        mo.inventory_item_id,
        mo.organization_id,
        mo.plan_id,
        mo.category_set_id,
        mo.sr_instance_id,
        FLOOR(mo.new_due_date),
        mo.fill_kill_flag,
        mo.so_line_split,
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
        END
      UNION
      SELECT
        mss.INVENTORY_ITEM_ID,
        mss.ORGANIZATION_ID,
        mss.PLAN_ID,
        mic.category_set_id,
        mss.SR_INSTANCE_ID,
        NULL AS fill_kill_flag,
        NULL AS so_line_split,
        mss.SAFETY_STOCK_QUANTITY AS quantity_rate,
        FLOOR(mss.PERIOD_START_DATE) AS new_due_date,
        'Safety stock' AS order_type_entity
      FROM (
        SELECT
          *
        FROM BEC_ODS.MSC_SAFETY_STOCKS
        WHERE
          IS_DELETED_FLG <> 'Y'
      ) AS mss, (
        SELECT
          *
        FROM BEC_ODS.msc_item_categories
        WHERE
          IS_DELETED_FLG <> 'Y'
      ) AS mic, gold_bec_dwh.DIM_ASCP_PLANS AS plans
      WHERE
        mss.sr_instance_id = mic.sr_instance_id
        AND mss.inventory_item_id = mic.inventory_item_id
        AND mss.organization_id = mic.organization_id
        AND category_set_id = 9
        AND mss.plan_id = plans.plan_id
        AND mss.sr_instance_id = plans.sr_instance_id
        AND plans.LOAD_FLG = 'Y' /* AND mss.plan_id = 40029 */ /* AND mss.INVENTORY_ITEM_ID = 1338018 */ /* AND mss.ORGANIZATION_ID = 265 */
      UNION
      SELECT
        mo.inventory_item_id,
        mo.organization_id,
        mo.plan_id,
        mo.category_set_id,
        mo.sr_instance_id,
        mo.fill_kill_flag,
        mo.so_line_split,
        quantity_rate,
        mo.new_due_date,
        order_type_entity
      FROM Current_Scheduled_Receipts AS mo
      UNION
      SELECT
        mo.inventory_item_id,
        mo.organization_id,
        mo.plan_id,
        mo.category_set_id,
        mo.sr_instance_id,
        mo.fill_kill_flag,
        mo.so_line_split,
        quantity_rate,
        mo.new_due_date,
        order_type_entity
      FROM Gross_Requirements AS mo
      UNION
      SELECT
        mo.inventory_item_id,
        mo.organization_id,
        mo.plan_id,
        mo.category_set_id,
        mo.sr_instance_id,
        mo.fill_kill_flag,
        mo.so_line_split,
        quantity_rate,
        mo.new_due_date,
        'Total supply' AS order_type_entity
      FROM Total_Supply AS mo
      UNION
      SELECT
        pab.inventory_item_id,
        pab.organization_id,
        pab.plan_id,
        pab.category_set_id,
        pab.sr_instance_id,
        pab.fill_kill_flag,
        pab.so_line_split,
        (
          SUM(quantity_rate) OVER (PARTITION BY pab.plan_id, pab.sr_instance_id, pab.category_set_id, pab.organization_id, pab.inventory_item_id ORDER BY pab.new_due_date NULLS LAST ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW)
        ) AS quantity_rate,
        pab.new_due_date,
        pab.order_type_entity
      FROM prj_available_bal AS pab
      UNION
      SELECT
        pab.inventory_item_id,
        pab.organization_id,
        pab.plan_id,
        pab.category_set_id,
        pab.sr_instance_id,
        pab.fill_kill_flag,
        pab.so_line_split,
        (
          SUM(quantity_rate) OVER (PARTITION BY pab.plan_id, pab.sr_instance_id, pab.category_set_id, pab.organization_id, pab.inventory_item_id ORDER BY pab.new_due_date NULLS LAST ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW)
        ) AS quantity_rate,
        pab.new_due_date,
        pab.order_type_entity
      FROM prj_onhand_bal AS pab
      UNION
      SELECT
        mv.inventory_item_id,
        mv.organization_id,
        mp.plan_id,
        9 AS category_set_id,
        mp.sr_instance_id,
        NULL AS fill_kill_flag, /* mo.order_type, */
        NULL AS so_line_split,
        0 AS quantity_rate,
        FLOOR(mp.curr_start_date) AS new_due_date,
        ot.order_type_entity
      FROM order_types AS ot, silver_bec_ods.msc_system_items AS mv, silver_bec_ods.msc_orders_v AS mov, gold_bec_dwh.DIM_ASCP_PLANS AS mp
      WHERE
        mv.plan_id = mp.plan_id
        AND mv.sr_instance_id = mp.sr_instance_id
        AND mov.plan_id = mv.plan_id
        AND mov.sr_instance_id = mv.sr_instance_id
        AND mov.inventory_item_id = mv.inventory_item_id
        AND mov.organization_id = mv.organization_id
        AND mov.category_set_id = 9
        AND mp.LOAD_FLG = 'Y'
      GROUP BY
        mv.inventory_item_id,
        mv.organization_id,
        mp.plan_id,
        mp.sr_instance_id,
        FLOOR(mp.curr_start_date),
        ot.order_type_entity
    )
  ) AS A
);
DROP table IF EXISTS gold_bec_dwh.fact_ascp_hp_safetystock_temp;
CREATE TABLE gold_bec_dwh.fact_ascp_hp_safetystock_temp AS
SELECT
  a.inventory_item_id,
  a.organization_id,
  a.plan_id,
  b.category_set_id,
  b.sr_instance_id,
  b.fill_kill_flag,
  b.so_line_split,
  b.quantity_rate,
  a.new_due_date,
  b.new_due_date AS new_due_date_new
FROM (
  SELECT DISTINCT
    plan_id,
    inventory_item_id,
    organization_id,
    new_due_date
  FROM gold_bec_dwh.FACT_ASCP_HORIZONTAL_PLAN
) AS a, (
  SELECT DISTINCT
    inventory_item_id,
    organization_id,
    plan_id,
    category_set_id,
    sr_instance_id,
    fill_kill_flag,
    so_line_split,
    quantity_rate,
    new_due_date
  FROM gold_bec_dwh.FACT_ASCP_HORIZONTAL_PLAN
  WHERE
    1 = 1 AND order_type_entity LIKE 'Safe%' AND quantity_rate <> 0
) AS b
WHERE
  1 = 1
  AND a.plan_id = b.PLAN_ID()
  AND a.inventory_item_id = b.INVENTORY_ITEM_ID()
  AND a.organization_id = b.ORGANIZATION_ID()
  AND a.new_due_date = b.NEW_DUE_DATE()
ORDER BY
  new_due_date NULLS LAST;
INSERT INTO gold_bec_dwh.FACT_ASCP_HORIZONTAL_PLAN (
  inventory_item_id,
  organization_id,
  plan_id,
  category_set_id,
  sr_instance_id,
  fill_kill_flag,
  so_line_split,
  quantity_rate,
  new_due_date,
  order_type_entity,
  plan_id_KEY,
  ORGANIZATION_ID_KEY,
  inventory_item_id_KEY,
  is_deleted_flg,
  source_app_id,
  dw_load_id,
  dw_insert_date,
  dw_update_date
)
(
  SELECT
    inventory_item_id,
    organization_id,
    plan_id,
    category_set_id_new,
    sr_instance_id_new,
    fill_kill_flag_new,
    so_line_split_new,
    quantity_rate_new,
    new_due_date,
    'Safety stock' AS order_type_entity,
    (
      SELECT
        system_id
      FROM bec_etl_ctrl.etlsourceappid
      WHERE
        source_system = 'EBS'
    ) || '-' || plan_id AS plan_id_KEY,
    (
      SELECT
        system_id
      FROM bec_etl_ctrl.etlsourceappid
      WHERE
        source_system = 'EBS'
    ) || '-' || ORGANIZATION_ID AS ORGANIZATION_ID_KEY,
    (
      SELECT
        system_id
      FROM bec_etl_ctrl.etlsourceappid
      WHERE
        source_system = 'EBS'
    ) || '-' || inventory_item_id AS inventory_item_id_KEY,
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
    ) || '-' || COALESCE(plan_id, 0) || '-' || COALESCE(ORGANIZATION_ID, 0) || '-' || COALESCE(inventory_item_id, 0) || '-' || COALESCE(category_set_id_new, 0) || '-' || COALESCE(new_due_date, CAST('01-01-1900' AS TIMESTAMP)) || '-' || COALESCE(order_type_entity, 'NA') AS dw_load_id,
    CURRENT_TIMESTAMP() AS dw_insert_date,
    CURRENT_TIMESTAMP() AS dw_update_date
  FROM (
    SELECT
      *,
      CASE
        WHEN quantity_rate IS NULL
        THEN (
          SELECT
            inner_table.quantity_rate
          FROM gold_bec_dwh.fact_ascp_hp_safetystock_temp AS inner_table
          WHERE
            1 = 1
            AND inner_table.plan_id = outertab.plan_id
            AND inner_table.inventory_item_id = outertab.inventory_item_id
            AND inner_table.organization_id = outertab.organization_id
            AND inner_table.new_due_date < outertab.new_due_date
            AND NOT inner_table.quantity_rate IS NULL
          ORDER BY
            inner_table.new_due_date DESC NULLS FIRST
          LIMIT 1
        )
        ELSE quantity_rate
      END AS quantity_rate_new,
      CASE
        WHEN category_set_id IS NULL
        THEN (
          SELECT
            inner_table.category_set_id
          FROM gold_bec_dwh.fact_ascp_hp_safetystock_temp AS inner_table
          WHERE
            1 = 1
            AND inner_table.plan_id = outertab.plan_id
            AND inner_table.inventory_item_id = outertab.inventory_item_id
            AND inner_table.organization_id = outertab.organization_id
            AND inner_table.new_due_date < outertab.new_due_date
            AND NOT inner_table.category_set_id IS NULL
          ORDER BY
            inner_table.new_due_date DESC NULLS FIRST
          LIMIT 1
        )
        ELSE category_set_id
      END AS category_set_id_new,
      CASE
        WHEN sr_instance_id IS NULL
        THEN (
          SELECT
            inner_table.sr_instance_id
          FROM gold_bec_dwh.fact_ascp_hp_safetystock_temp AS inner_table
          WHERE
            1 = 1
            AND inner_table.plan_id = outertab.plan_id
            AND inner_table.inventory_item_id = outertab.inventory_item_id
            AND inner_table.organization_id = outertab.organization_id
            AND inner_table.new_due_date < outertab.new_due_date
            AND NOT inner_table.sr_instance_id IS NULL
          ORDER BY
            inner_table.new_due_date DESC NULLS FIRST
          LIMIT 1
        )
        ELSE sr_instance_id
      END AS sr_instance_id_new,
      CASE
        WHEN fill_kill_flag IS NULL
        THEN (
          SELECT
            inner_table.fill_kill_flag
          FROM gold_bec_dwh.fact_ascp_hp_safetystock_temp AS inner_table
          WHERE
            1 = 1
            AND inner_table.plan_id = outertab.plan_id
            AND inner_table.inventory_item_id = outertab.inventory_item_id
            AND inner_table.organization_id = outertab.organization_id
            AND inner_table.new_due_date < outertab.new_due_date
            AND NOT inner_table.fill_kill_flag IS NULL
          ORDER BY
            inner_table.new_due_date DESC NULLS FIRST
          LIMIT 1
        )
        ELSE fill_kill_flag
      END AS fill_kill_flag_new,
      CASE
        WHEN so_line_split IS NULL
        THEN (
          SELECT
            inner_table.so_line_split
          FROM gold_bec_dwh.fact_ascp_hp_safetystock_temp AS inner_table
          WHERE
            1 = 1
            AND inner_table.plan_id = outertab.plan_id
            AND inner_table.inventory_item_id = outertab.inventory_item_id
            AND inner_table.organization_id = outertab.organization_id
            AND inner_table.new_due_date < outertab.new_due_date
            AND NOT inner_table.so_line_split IS NULL
          ORDER BY
            inner_table.new_due_date DESC NULLS FIRST
          LIMIT 1
        )
        ELSE so_line_split
      END AS so_line_split_new
    FROM gold_bec_dwh.fact_ascp_hp_safetystock_temp AS outertab
  )
  WHERE
    new_due_date_new IS NULL
  ORDER BY
    new_due_date ASC NULLS LAST
);
DROP table IF EXISTS gold_bec_dwh.fact_ascp_hp_projonhand_temp;
CREATE TABLE gold_bec_dwh.fact_ascp_hp_projonhand_temp AS
SELECT
  a.inventory_item_id,
  a.organization_id,
  a.plan_id,
  b.category_set_id,
  b.sr_instance_id,
  b.fill_kill_flag,
  b.so_line_split,
  b.quantity_rate,
  a.new_due_date,
  b.new_due_date AS new_due_date_new
FROM (
  SELECT DISTINCT
    plan_id,
    inventory_item_id,
    organization_id,
    new_due_date
  FROM gold_bec_dwh.FACT_ASCP_HORIZONTAL_PLAN
  WHERE
    order_type_entity = 'Safety stock'
) AS a, (
  SELECT DISTINCT
    inventory_item_id,
    organization_id,
    plan_id,
    category_set_id,
    sr_instance_id,
    fill_kill_flag,
    so_line_split,
    quantity_rate,
    new_due_date
  FROM gold_bec_dwh.FACT_ASCP_HORIZONTAL_PLAN
  WHERE
    1 = 1 AND order_type_entity = 'Projected on hand'
) AS b
WHERE
  1 = 1
  AND a.plan_id = b.PLAN_ID()
  AND a.inventory_item_id = b.INVENTORY_ITEM_ID()
  AND a.organization_id = b.ORGANIZATION_ID()
  AND a.new_due_date = b.NEW_DUE_DATE()
ORDER BY
  new_due_date NULLS LAST;
INSERT INTO gold_bec_dwh.FACT_ASCP_HORIZONTAL_PLAN (
  inventory_item_id,
  organization_id,
  plan_id,
  category_set_id,
  sr_instance_id,
  fill_kill_flag,
  so_line_split,
  quantity_rate,
  new_due_date,
  order_type_entity,
  plan_id_KEY,
  ORGANIZATION_ID_KEY,
  inventory_item_id_KEY,
  is_deleted_flg,
  source_app_id,
  dw_load_id,
  dw_insert_date,
  dw_update_date
)
(
  SELECT
    inventory_item_id,
    organization_id,
    plan_id,
    category_set_id_new,
    sr_instance_id_new,
    fill_kill_flag_new,
    so_line_split_new,
    quantity_rate_new,
    new_due_date,
    'Projected on hand' AS order_type_entity,
    (
      SELECT
        system_id
      FROM bec_etl_ctrl.etlsourceappid
      WHERE
        source_system = 'EBS'
    ) || '-' || plan_id AS plan_id_KEY,
    (
      SELECT
        system_id
      FROM bec_etl_ctrl.etlsourceappid
      WHERE
        source_system = 'EBS'
    ) || '-' || ORGANIZATION_ID AS ORGANIZATION_ID_KEY,
    (
      SELECT
        system_id
      FROM bec_etl_ctrl.etlsourceappid
      WHERE
        source_system = 'EBS'
    ) || '-' || inventory_item_id AS inventory_item_id_KEY,
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
    ) || '-' || COALESCE(plan_id, 0) || '-' || COALESCE(ORGANIZATION_ID, 0) || '-' || COALESCE(inventory_item_id, 0) || '-' || COALESCE(category_set_id_new, 0) || '-' || COALESCE(new_due_date, CAST('01-01-1900' AS TIMESTAMP)) || '-' || COALESCE(order_type_entity, 'NA') AS dw_load_id,
    CURRENT_TIMESTAMP() AS dw_insert_date,
    CURRENT_TIMESTAMP() AS dw_update_date
  FROM (
    SELECT
      *,
      CASE
        WHEN quantity_rate IS NULL
        THEN (
          SELECT
            inner_table.quantity_rate
          FROM gold_bec_dwh.fact_ascp_hp_projonhand_temp AS inner_table
          WHERE
            1 = 1
            AND inner_table.plan_id = outertab.plan_id
            AND inner_table.inventory_item_id = outertab.inventory_item_id
            AND inner_table.organization_id = outertab.organization_id
            AND inner_table.new_due_date < outertab.new_due_date
            AND NOT inner_table.quantity_rate IS NULL
          ORDER BY
            inner_table.new_due_date DESC NULLS FIRST
          LIMIT 1
        )
        ELSE quantity_rate
      END AS quantity_rate_new,
      CASE
        WHEN category_set_id IS NULL
        THEN (
          SELECT
            inner_table.category_set_id
          FROM gold_bec_dwh.fact_ascp_hp_projonhand_temp AS inner_table
          WHERE
            1 = 1
            AND inner_table.plan_id = outertab.plan_id
            AND inner_table.inventory_item_id = outertab.inventory_item_id
            AND inner_table.organization_id = outertab.organization_id
            AND inner_table.new_due_date < outertab.new_due_date
            AND NOT inner_table.category_set_id IS NULL
          ORDER BY
            inner_table.new_due_date DESC NULLS FIRST
          LIMIT 1
        )
        ELSE category_set_id
      END AS category_set_id_new,
      CASE
        WHEN sr_instance_id IS NULL
        THEN (
          SELECT
            inner_table.sr_instance_id
          FROM gold_bec_dwh.fact_ascp_hp_projonhand_temp AS inner_table
          WHERE
            1 = 1
            AND inner_table.plan_id = outertab.plan_id
            AND inner_table.inventory_item_id = outertab.inventory_item_id
            AND inner_table.organization_id = outertab.organization_id
            AND inner_table.new_due_date < outertab.new_due_date
            AND NOT inner_table.sr_instance_id IS NULL
          ORDER BY
            inner_table.new_due_date DESC NULLS FIRST
          LIMIT 1
        )
        ELSE sr_instance_id
      END AS sr_instance_id_new,
      CASE
        WHEN fill_kill_flag IS NULL
        THEN (
          SELECT
            inner_table.fill_kill_flag
          FROM gold_bec_dwh.fact_ascp_hp_projonhand_temp AS inner_table
          WHERE
            1 = 1
            AND inner_table.plan_id = outertab.plan_id
            AND inner_table.inventory_item_id = outertab.inventory_item_id
            AND inner_table.organization_id = outertab.organization_id
            AND inner_table.new_due_date < outertab.new_due_date
            AND NOT inner_table.fill_kill_flag IS NULL
          ORDER BY
            inner_table.new_due_date DESC NULLS FIRST
          LIMIT 1
        )
        ELSE fill_kill_flag
      END AS fill_kill_flag_new,
      CASE
        WHEN so_line_split IS NULL
        THEN (
          SELECT
            inner_table.so_line_split
          FROM gold_bec_dwh.fact_ascp_hp_projonhand_temp AS inner_table
          WHERE
            1 = 1
            AND inner_table.plan_id = outertab.plan_id
            AND inner_table.inventory_item_id = outertab.inventory_item_id
            AND inner_table.organization_id = outertab.organization_id
            AND inner_table.new_due_date < outertab.new_due_date
            AND NOT inner_table.so_line_split IS NULL
          ORDER BY
            inner_table.new_due_date DESC NULLS FIRST
          LIMIT 1
        )
        ELSE so_line_split
      END AS so_line_split_new
    FROM gold_bec_dwh.fact_ascp_hp_projonhand_temp AS outertab
  )
  WHERE
    new_due_date_new IS NULL
  ORDER BY
    new_due_date ASC NULLS LAST
);
DROP table IF EXISTS gold_bec_dwh.fact_ascp_hp_projavailbal_temp;
CREATE TABLE gold_bec_dwh.fact_ascp_hp_projavailbal_temp AS
SELECT
  a.inventory_item_id,
  a.organization_id,
  a.plan_id,
  b.category_set_id,
  b.sr_instance_id,
  b.fill_kill_flag,
  b.so_line_split,
  b.quantity_rate,
  a.new_due_date,
  b.new_due_date AS new_due_date_new
FROM (
  SELECT DISTINCT
    plan_id,
    inventory_item_id,
    organization_id,
    new_due_date
  FROM gold_bec_dwh.FACT_ASCP_HORIZONTAL_PLAN
  WHERE
    order_type_entity = 'Projected on hand'
) AS a, (
  SELECT DISTINCT
    inventory_item_id,
    organization_id,
    plan_id,
    category_set_id,
    sr_instance_id,
    fill_kill_flag,
    so_line_split,
    quantity_rate,
    new_due_date
  FROM gold_bec_dwh.FACT_ASCP_HORIZONTAL_PLAN
  WHERE
    1 = 1 AND order_type_entity = 'Projected available balance'
) AS b
WHERE
  1 = 1
  AND a.plan_id = b.PLAN_ID()
  AND a.inventory_item_id = b.INVENTORY_ITEM_ID()
  AND a.organization_id = b.ORGANIZATION_ID()
  AND a.new_due_date = b.NEW_DUE_DATE()
ORDER BY
  new_due_date NULLS LAST;
INSERT INTO gold_bec_dwh.FACT_ASCP_HORIZONTAL_PLAN (
  inventory_item_id,
  organization_id,
  plan_id,
  category_set_id,
  sr_instance_id,
  fill_kill_flag,
  so_line_split,
  quantity_rate,
  new_due_date,
  order_type_entity,
  plan_id_KEY,
  ORGANIZATION_ID_KEY,
  inventory_item_id_KEY,
  is_deleted_flg,
  source_app_id,
  dw_load_id,
  dw_insert_date,
  dw_update_date
)
(
  SELECT
    inventory_item_id,
    organization_id,
    plan_id,
    category_set_id_new,
    sr_instance_id_new,
    fill_kill_flag_new,
    so_line_split_new,
    quantity_rate_new,
    new_due_date,
    'Projected available balance' AS order_type_entity,
    (
      SELECT
        system_id
      FROM bec_etl_ctrl.etlsourceappid
      WHERE
        source_system = 'EBS'
    ) || '-' || plan_id AS plan_id_KEY,
    (
      SELECT
        system_id
      FROM bec_etl_ctrl.etlsourceappid
      WHERE
        source_system = 'EBS'
    ) || '-' || ORGANIZATION_ID AS ORGANIZATION_ID_KEY,
    (
      SELECT
        system_id
      FROM bec_etl_ctrl.etlsourceappid
      WHERE
        source_system = 'EBS'
    ) || '-' || inventory_item_id AS inventory_item_id_KEY,
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
    ) || '-' || COALESCE(plan_id, 0) || '-' || COALESCE(ORGANIZATION_ID, 0) || '-' || COALESCE(inventory_item_id, 0) || '-' || COALESCE(category_set_id_new, 0) || '-' || COALESCE(new_due_date, CAST('01-01-1900' AS TIMESTAMP)) || '-' || COALESCE(order_type_entity, 'NA') AS dw_load_id,
    CURRENT_TIMESTAMP() AS dw_insert_date,
    CURRENT_TIMESTAMP() AS dw_update_date
  FROM (
    SELECT
      *,
      CASE
        WHEN quantity_rate IS NULL
        THEN (
          SELECT
            inner_table.quantity_rate
          FROM gold_bec_dwh.fact_ascp_hp_projavailbal_temp AS inner_table
          WHERE
            1 = 1
            AND inner_table.plan_id = outertab.plan_id
            AND inner_table.inventory_item_id = outertab.inventory_item_id
            AND inner_table.organization_id = outertab.organization_id
            AND inner_table.new_due_date < outertab.new_due_date
            AND NOT inner_table.quantity_rate IS NULL
          ORDER BY
            inner_table.new_due_date DESC NULLS FIRST
          LIMIT 1
        )
        ELSE quantity_rate
      END AS quantity_rate_new,
      CASE
        WHEN category_set_id IS NULL
        THEN (
          SELECT
            inner_table.category_set_id
          FROM gold_bec_dwh.fact_ascp_hp_projavailbal_temp AS inner_table
          WHERE
            1 = 1
            AND inner_table.plan_id = outertab.plan_id
            AND inner_table.inventory_item_id = outertab.inventory_item_id
            AND inner_table.organization_id = outertab.organization_id
            AND inner_table.new_due_date < outertab.new_due_date
            AND NOT inner_table.category_set_id IS NULL
          ORDER BY
            inner_table.new_due_date DESC NULLS FIRST
          LIMIT 1
        )
        ELSE category_set_id
      END AS category_set_id_new,
      CASE
        WHEN sr_instance_id IS NULL
        THEN (
          SELECT
            inner_table.sr_instance_id
          FROM gold_bec_dwh.fact_ascp_hp_projavailbal_temp AS inner_table
          WHERE
            1 = 1
            AND inner_table.plan_id = outertab.plan_id
            AND inner_table.inventory_item_id = outertab.inventory_item_id
            AND inner_table.organization_id = outertab.organization_id
            AND inner_table.new_due_date < outertab.new_due_date
            AND NOT inner_table.sr_instance_id IS NULL
          ORDER BY
            inner_table.new_due_date DESC NULLS FIRST
          LIMIT 1
        )
        ELSE sr_instance_id
      END AS sr_instance_id_new,
      CASE
        WHEN fill_kill_flag IS NULL
        THEN (
          SELECT
            inner_table.fill_kill_flag
          FROM gold_bec_dwh.fact_ascp_hp_projavailbal_temp AS inner_table
          WHERE
            1 = 1
            AND inner_table.plan_id = outertab.plan_id
            AND inner_table.inventory_item_id = outertab.inventory_item_id
            AND inner_table.organization_id = outertab.organization_id
            AND inner_table.new_due_date < outertab.new_due_date
            AND NOT inner_table.fill_kill_flag IS NULL
          ORDER BY
            inner_table.new_due_date DESC NULLS FIRST
          LIMIT 1
        )
        ELSE fill_kill_flag
      END AS fill_kill_flag_new,
      CASE
        WHEN so_line_split IS NULL
        THEN (
          SELECT
            inner_table.so_line_split
          FROM gold_bec_dwh.fact_ascp_hp_projavailbal_temp AS inner_table
          WHERE
            1 = 1
            AND inner_table.plan_id = outertab.plan_id
            AND inner_table.inventory_item_id = outertab.inventory_item_id
            AND inner_table.organization_id = outertab.organization_id
            AND inner_table.new_due_date < outertab.new_due_date
            AND NOT inner_table.so_line_split IS NULL
          ORDER BY
            inner_table.new_due_date DESC NULLS FIRST
          LIMIT 1
        )
        ELSE so_line_split
      END AS so_line_split_new
    FROM gold_bec_dwh.fact_ascp_hp_projavailbal_temp AS outertab
  )
  WHERE
    new_due_date_new IS NULL
  ORDER BY
    new_due_date ASC NULLS LAST
);
UPDATE bec_etl_ctrl.batch_dw_info SET load_type = 'I', last_refresh_date = CURRENT_TIMESTAMP()
WHERE
  dw_table_name = 'fact_ascp_horizontal_plan' AND batch_name = 'ascp';