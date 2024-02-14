DROP table IF EXISTS gold_bec_dwh.FACT_ASCP_FORECAST;
CREATE TABLE gold_bec_dwh.fact_ascp_forecast AS
(
  SELECT
    h.organization_id,
    h.inventory_item_id,
    l.transaction_id,
    h.concatenated_segments AS part_number,
    h.item_description,
    h.primary_uom_code,
    h.bom_item_type_desc,
    h.ato_forecast_control_desc,
    h.mrp_planning_code_desc,
    h.pick_components_flag,
    h.forecast_designator,
    l.bucket_type,
    l.forecast_date,
    l.current_forecast_quantity,
    l.original_forecast_quantity,
    l.comments,
    h.attribute1 AS HEADER_ATTRIBUTE1,
    l.attribute1 AS LINE_ATTRIBUTE1,
    msi.planner_code,
    msi.buyer_id,
    pa.agent_name AS buyer_name,
    msi.PLANNING_MAKE_BUY_CODE,
    CURRENT_TIMESTAMP() AS Datestamp,
    'N' AS IS_DELETED_FLG,
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
    ) || '-' || COALESCE(h.inventory_item_id, 0) || '-' || COALESCE(h.organization_id, 0) || '-' || COALESCE(h.forecast_designator, 'NA') || '-' || COALESCE(l.transaction_id, 0) AS dw_load_id,
    CURRENT_TIMESTAMP() AS dw_insert_date,
    CURRENT_TIMESTAMP() AS dw_update_date
  FROM silver_bec_ods.mrp_forecast_items_v AS h, silver_bec_ods.mrp_forecast_dates_v AS l, silver_bec_ods.mtl_system_items_b AS msi, silver_bec_ods.po_agents_v AS pa
  WHERE
    h.organization_id = l.organization_id
    AND h.inventory_item_id = l.inventory_item_id
    AND h.forecast_designator = l.forecast_designator
    AND l.inventory_item_id = msi.INVENTORY_ITEM_ID()
    AND l.organization_id = msi.ORGANIZATION_ID()
    AND msi.buyer_id = pa.AGENT_ID()
);
UPDATE bec_etl_ctrl.batch_dw_info SET load_type = 'I', last_refresh_date = CURRENT_TIMESTAMP()
WHERE
  dw_table_name = 'fact_ascp_forecast' AND batch_name = 'ascp';