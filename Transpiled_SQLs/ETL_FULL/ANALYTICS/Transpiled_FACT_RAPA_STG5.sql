DROP table IF EXISTS gold_bec_dwh.FACT_RAPA_STG5;
CREATE TABLE gold_bec_dwh.FACT_RAPA_STG5 AS
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
    consumption_date1, /* c.organization_id, */ /* organization_code, */
    aging_period,
    consumption_date,
    item,
    description,
    planning_make_buy_code,
    NULL AS category_name,
    consumption_quantity,
    CAST(NULL AS DECIMAL(38, 10)) AS primary_quantity,
    unit_price,
    uom_code,
    consumption_value,
    po_line_type,
    vendor_name,
    material_cost,
    extended_cost,
    consigned_ppv,
    source_organization_id,
    planner_code,
    buyer_name,
    primary_unit_of_measure,
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
      msi.inventory_item_id,
      c.organization_id,
      'Pending' AS cost_type,
      NULL AS plan_name,
      'Receipt Actual' AS data_type,
      'CVMI Consumption' AS order_group,
      'CVMI Consumption' AS order_type_text,
      po_number,
      FLOOR(consumption_date) AS consumption_date1, /* c.organization_id, */ /* organization_code, */
      0 AS aging_period,
      FLOOR(consumption_date) AS consumption_date,
      c.item,
      c.description,
      NULL AS planning_make_buy_code,
      consumption_quantity, /* NULL                    category_name, */
      NULL,
      unit_price,
      primary_unit_of_measure AS uom_code,
      consumption_value,
      NULL AS po_line_type,
      vendor_name,
      material_cost,
      extended_cost,
      consigned_ppv,
      CAST(NULL AS DECIMAL(15, 0)) AS source_organization_id,
      planner_code,
      agent_name AS buyer_name,
      primary_unit_of_measure,
      c.release_num
    FROM (
      SELECT
        *
      FROM silver_bec_ods.bec_cvmi_consumption_view
      WHERE
        is_deleted_flg <> 'Y'
    ) AS c, (
      SELECT
        *
      FROM silver_bec_ods.mtl_system_items_b
      WHERE
        is_deleted_flg <> 'Y'
    ) AS msi, (
      SELECT
        *
      FROM silver_bec_ods.po_headers_all
      WHERE
        is_deleted_flg <> 'Y'
    ) AS poh, (
      SELECT
        *
      FROM silver_bec_ods.po_agents_v
      WHERE
        is_deleted_flg <> 'Y'
    ) AS poa
    WHERE
      c.item = msi.segment1
      AND c.organization_id = msi.organization_id
      AND c.po_number = poh.SEGMENT1()
      AND poh.agent_id = poa.AGENT_ID()
    UNION ALL
    SELECT
      msi.inventory_item_id,
      c.organization_id,
      'Frozen' AS cost_type,
      NULL AS plan_name,
      'Receipt Actual' AS data_type,
      'CVMI Consumption' AS order_group,
      'CVMI Consumption' AS order_type_text,
      po_number,
      FLOOR(consumption_date) AS consumption_date1, /* c.organization_id, */ /* organization_code, */
      0 AS aging_period,
      FLOOR(consumption_date) AS consumption_date,
      c.item,
      c.description,
      NULL AS planning_make_buy_code,
      consumption_quantity, /* NULL                    category_name, */
      NULL,
      unit_price,
      primary_unit_of_measure AS uom_code,
      consumption_value,
      NULL AS po_line_type,
      vendor_name,
      material_cost,
      extended_cost,
      consigned_ppv,
      CAST(NULL AS DECIMAL(15, 0)) AS source_organization_id,
      planner_code,
      agent_name AS buyer_name,
      msi.primary_unit_of_measure,
      c.release_num
    FROM (
      SELECT
        *
      FROM silver_bec_ods.bec_cvmi_consumption_view
      WHERE
        is_deleted_flg <> 'Y'
    ) AS c, (
      SELECT
        *
      FROM silver_bec_ods.mtl_system_items_b
      WHERE
        is_deleted_flg <> 'Y'
    ) AS msi, (
      SELECT
        *
      FROM silver_bec_ods.po_headers_all
      WHERE
        is_deleted_flg <> 'Y'
    ) AS poh, (
      SELECT
        *
      FROM silver_bec_ods.po_agents_v
      WHERE
        is_deleted_flg <> 'Y'
    ) AS poa
    WHERE
      c.item = msi.segment1
      AND c.organization_id = msi.organization_id
      AND c.po_number = poh.SEGMENT1()
      AND poh.agent_id = poa.AGENT_ID()
  )
);
UPDATE bec_etl_ctrl.batch_dw_info SET load_type = 'I', last_refresh_date = CURRENT_TIMESTAMP()
WHERE
  dw_table_name = 'fact_rapa_stg5' AND batch_name = 'ascp';