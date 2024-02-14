DROP table IF EXISTS gold_bec_dwh.DIM_ITEM_COST_DETAILS;
CREATE TABLE gold_bec_dwh.DIM_ITEM_COST_DETAILS AS
(
  SELECT
    inventory_item_id,
    organization_id,
    cost_element_id,
    cost_type_id,
    resource_id,
    resource_code,
    item_cost,
    'N' AS is_deleted_flg,
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
    ) || '-' || COALESCE(inventory_item_id, 0) || '-' || COALESCE(organization_id, 0) || '-' || COALESCE(cost_element_id, 0) || '-' || COALESCE(cost_type_id, 0) || '-' || COALESCE(resource_id, 0) AS dw_load_id,
    CURRENT_TIMESTAMP() AS dw_insert_date,
    CURRENT_TIMESTAMP() AS dw_update_date
  FROM (
    SELECT
      ccd.inventory_item_id,
      ccd.organization_id,
      ccd.cost_element_id,
      ccd.cost_type_id,
      ccd.resource_id,
      br.resource_code,
      SUM(item_cost) AS item_cost
    FROM silver_bec_ods.cst_item_cost_details AS ccd, silver_bec_ods.bom_resources AS br
    WHERE
      1 = 1 AND ccd.resource_id = br.resource_id AND ccd.ORGANIZATION_ID = br.organization_id
    GROUP BY
      ccd.inventory_item_id,
      ccd.organization_id,
      ccd.cost_element_id,
      ccd.cost_type_id,
      ccd.resource_id,
      br.resource_code
  )
);
UPDATE bec_etl_ctrl.batch_dw_info SET load_type = 'I', last_refresh_date = CURRENT_TIMESTAMP()
WHERE
  dw_table_name = 'dim_item_cost_details' AND batch_name = 'inv';