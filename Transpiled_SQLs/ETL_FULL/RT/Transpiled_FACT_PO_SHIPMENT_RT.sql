DROP table IF EXISTS gold_bec_dwh_rpt.FACT_PO_SHIPMENT_RT;
CREATE TABLE gold_bec_dwh_rpt.fact_po_shipment_rt AS
(
  SELECT
    fact_po_shipment.*,
    `dim_inv_category`.`item_category_segment1` AS `inv_category_segment1 `,
    `dim_inv_category`.`item_category_segment2` AS `inv_category_segment2 `
  FROM `bec_dwh`.`fact_po_shipment` AS `fact_po_shipment`
  LEFT JOIN `bec_dwh`.`dim_inv_item_category_set` AS `dim_inv_category`
    ON (
      (
        `fact_po_shipment`.`item_id` = `dim_inv_category`.`inventory_item_id`
      )
      AND (
        `fact_po_shipment`.`to_organization_id` = `dim_inv_category`.`organization_id`
      )
      AND (
        1 = `dim_inv_category`.`category_set_id`
      )
    )
);
UPDATE bec_etl_ctrl.batch_dw_info SET load_type = 'I', last_refresh_date = CURRENT_TIMESTAMP()
WHERE
  dw_table_name = 'fact_po_shipment_rt' AND batch_name = 'po';