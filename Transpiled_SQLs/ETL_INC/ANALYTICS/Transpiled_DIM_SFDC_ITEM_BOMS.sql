TRUNCATE table gold_bec_dwh.DIM_SFDC_ITEM_BOMS;
INSERT INTO gold_bec_dwh.DIM_SFDC_ITEM_BOMS
(
  SELECT
    iasy.organization_id,
    iasy.inventory_item_id,
    SUBSTRING(iasy.segment1, REGEXP_INSTR(iasy.segment1, '-') + 1) AS `SiteId`,
    ood.organization_code AS `Org Code`,
    iasy.segment1 AS `Assembly Item`,
    iasy.description AS `Assembly Description`,
    comp.component_item_id,
    icmp.segment1 AS `Component`,
    icmp.description AS ` Component Description`,
    comp.operation_seq_num AS `Operation Sequence`,
    comp.item_num AS `Component Item Num`,
    comp.component_quantity AS `Quantity`,
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
    ) || '-' || COALESCE(iasy.inventory_item_id, 0) || '-' || COALESCE(iasy.organization_id, 0) || '-' || COALESCE(comp.component_item_id, 0) AS dw_load_id,
    CURRENT_TIMESTAMP() AS dw_insert_date,
    CURRENT_TIMESTAMP() AS dw_update_date
  FROM silver_bec_ods.mtl_system_items_B AS iasy, silver_bec_ods.bom_bill_of_materials AS bom, silver_bec_ods.bom_inventory_components AS comp, silver_bec_ods.mtl_system_items_B AS icmp, silver_bec_ods.org_organization_definitions AS ood
  WHERE
    1 = 1
    AND ood.organization_id = icmp.organization_id
    AND iasy.organization_id = icmp.organization_id
    AND iasy.inventory_item_id = bom.assembly_item_id
    AND iasy.organization_id = bom.organization_id
    AND bom.bill_sequence_id = comp.bill_sequence_id
    AND comp.component_item_id = icmp.inventory_item_id
    AND comp.EFFECTIVITY_DATE = (
      SELECT
        MAX(EFFECTIVITY_DATE)
      FROM silver_bec_ods.bom_inventory_components
      WHERE
        COMPONENT_ITEM_ID = comp.component_item_id
        AND bill_sequence_id = comp.bill_sequence_id
      GROUP BY
        COMPONENT_ITEM_ID
    )
    AND (
      iasy.segment1 LIKE 'SI-%' OR iasy.segment1 LIKE 'SD-%' OR iasy.segment1 LIKE 'SP-%'
    )
    AND COALESCE(comp.DISABLE_DATE, CURRENT_TIMESTAMP() + 1) > FLOOR(CURRENT_TIMESTAMP())
);
UPDATE bec_etl_ctrl.batch_dw_info SET load_type = 'I', last_refresh_date = CURRENT_TIMESTAMP()
WHERE
  dw_table_name = 'dim_sfdc_item_boms' AND batch_name = 'salesforce';