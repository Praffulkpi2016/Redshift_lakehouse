DROP table IF EXISTS gold_bec_dwh.FACT_ASCP_HP_BOM_ITEMS;
CREATE TABLE gold_bec_dwh.fact_ascp_hp_bom_items AS
(
  WITH items(plan_id, organization_id, inventory_item_id, using_assembly_id, level, root_assembly, bill_sequence_id) AS (
    SELECT DISTINCT
      bom1.plan_id,
      bom1.organization_id,
      bom1.assembly_item_id AS inventory_item_id,
      bom1.assembly_item_id AS using_assembly_id,
      0 AS level,
      bom1.assembly_item_id AS root_assembly,
      bill_sequence_id
    FROM silver_bec_ods.msc_boms AS bom1, silver_bec_ods.msc_system_items AS msi, gold_bec_dwh.fact_ascp_hp_items_stg AS fahp2
    WHERE
      alternate_bom_designator#1 IS NULL
      AND msi.inventory_item_id = bom1.assembly_item_id
      AND msi.planning_make_buy_code = 1
      AND msi.plan_id = bom1.plan_id
      AND msi.organization_id = bom1.organization_id
      AND fahp2.plan_id = bom1.plan_id
      AND fahp2.organization_id = bom1.organization_id
      AND fahp2.sr_instance_id = bom1.sr_instance_id
    UNION ALL
    SELECT
      mbc.plan_id,
      mbc.organization_id,
      mbc.inventory_item_id,
      mbc.using_assembly_id,
      level + 1 AS level,
      items.root_assembly,
      COALESCE(bom2.bill_sequence_id, 1)
    FROM silver_bec_ods.msc_bom_components AS mbc, silver_bec_ods.msc_boms AS bom2, gold_bec_dwh.fact_ascp_hp_items_stg AS fahp3, items
    WHERE
      CURRENT_TIMESTAMP() BETWEEN mbc.effectivity_date AND COALESCE(mbc.disable_date, CURRENT_TIMESTAMP())
      AND mbc.effectivity_date <= CURRENT_TIMESTAMP()
      AND mbc.plan_id = bom2.PLAN_ID()
      AND alternate_bom_designator#1 IS NULL
      AND mbc.organization_id = bom2.ORGANIZATION_ID()
      AND mbc.inventory_item_id = bom2.ASSEMBLY_ITEM_ID()
      AND fahp3.plan_id = mbc.plan_id
      AND fahp3.organization_id = mbc.organization_id
      AND fahp3.sr_instance_id = mbc.sr_instance_id
      AND fahp3.inventory_item_id = mbc.inventory_item_id
      AND mbc.plan_id = items.plan_id
      AND mbc.organization_id = items.organization_id
      AND items.inventory_item_id = mbc.USING_ASSEMBLY_ID
      AND mbc.bill_sequence_id = COALESCE(items.bill_sequence_id, 1)
  )
  SELECT
    *,
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
    ) || '-' || using_assembly_id AS using_assembly_id_key,
    (
      SELECT
        system_id
      FROM bec_etl_ctrl.etlsourceappid
      WHERE
        source_system = 'EBS'
    ) || '-' || plan_id AS plan_id_key,
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
    ) || '-' || COALESCE(inventory_item_id, 0) || '-' || COALESCE(using_assembly_id, 0) || '-' || COALESCE(plan_id, 0) || '-' || COALESCE(organization_id, 0) AS dw_load_id,
    CURRENT_TIMESTAMP() AS dw_insert_date,
    CURRENT_TIMESTAMP() AS dw_update_date
  FROM items
);
UPDATE bec_etl_ctrl.batch_dw_info SET load_type = 'I', last_refresh_date = CURRENT_TIMESTAMP()
WHERE
  dw_table_name = 'fact_ascp_hp_bom_items' AND batch_name = 'ascp';