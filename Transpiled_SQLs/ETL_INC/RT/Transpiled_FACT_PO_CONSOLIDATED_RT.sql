TRUNCATE table gold_bec_dwh_rpt.fact_po_consolidated_rt;
INSERT INTO gold_bec_dwh_rpt.fact_po_consolidated_rt
(
  SELECT
    fact_po_consolidated.*,
    COALESCE(fact_po_consolidated.amount_billed, 0) AS amount_billed1,
    COALESCE(fact_po_consolidated.amount_received, 0) AS amount_received1,
    dim_purchasing_category.item_category_segment1 AS `inv_category_segment1`,
    dim_inv_category.item_category_segment1 AS `po_category_segment1`,
    dim_purchasing_category.item_category_segment2 AS `po_category_segment2`,
    dim_inv_category.item_category_segment2 AS `inv_category_segment2`
  FROM `bec_dwh`.`fact_po_consolidated` AS `fact_po_consolidated`
  LEFT JOIN `bec_dwh`.`dim_inv_item_category_set` AS `dim_inv_category`
    ON (
      (
        `fact_po_consolidated`.`item_id` = `dim_inv_category`.`inventory_item_id`
      )
      AND (
        `fact_po_consolidated`.`ship_to_organization_id` = `dim_inv_category`.`organization_id`
      )
      AND (
        1 = `dim_inv_category`.`category_set_id`
      )
    )
  LEFT JOIN `bec_dwh`.`dim_inv_item_category_set` AS `dim_purchasing_category`
    ON (
      (
        `fact_po_consolidated`.`item_id` = `dim_purchasing_category`.`inventory_item_id`
      )
      AND (
        `fact_po_consolidated`.`ship_to_organization_id` = `dim_purchasing_category`.`organization_id`
      )
      AND (
        1100000043 = `dim_purchasing_category`.`category_set_id`
      )
    )
);
UPDATE bec_etl_ctrl.batch_dw_info SET load_type = 'I', last_refresh_date = CURRENT_TIMESTAMP()
WHERE
  dw_table_name = 'fact_po_consolidated_rt' AND batch_name = 'po';