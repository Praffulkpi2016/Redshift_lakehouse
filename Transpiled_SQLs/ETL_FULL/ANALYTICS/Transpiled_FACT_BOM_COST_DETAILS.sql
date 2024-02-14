DROP table IF EXISTS gold_bec_dwh.FACT_BOM_COST_DETAILS;
CREATE TABLE gold_bec_dwh.FACT_BOM_COST_DETAILS AS
SELECT
  top_item_id,
  assembly_item_id,
  component_item_id,
  organization_id,
  sort_order,
  top_assembly,
  assembly,
  component,
  bom_level,
  description,
  item_type,
  parent_is_make_buy,
  make_buy,
  status,
  item_num,
  operation_seq_num,
  primary_uom_code,
  component_quantity,
  extended_quantity,
  costed_flag,
  wip_supply_type,
  bom_type,
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
  ) || '-' || COALESCE(component_item_id, 0) || '-' || COALESCE(organization_id, 0) || '-' || COALESCE(assembly_item_id, 0) || '-' || COALESCE(sort_order, 'NA') AS dw_load_id,
  CURRENT_TIMESTAMP() AS dw_insert_date,
  CURRENT_TIMESTAMP() AS dw_update_date
FROM (
  SELECT
    xib.top_item_id,
    bom.assembly_item_id,
    xib.component_item_id,
    xib.organization_id,
    TRIM('.' FROM CASE
      WHEN SUBSTRING(sort_order, 1, 7) IS NULL
      THEN ''
      ELSE TRIM(0 FROM SUBSTRING(sort_order, 1, 7))
    END || CASE
      WHEN SUBSTRING(sort_order, 8, 7) IS NULL
      THEN ''
      ELSE '.' || TRIM(0 FROM SUBSTRING(sort_order, 8, 7))
    END || CASE
      WHEN SUBSTRING(sort_order, 15, 7) IS NULL
      THEN ''
      ELSE '.' || TRIM(0 FROM SUBSTRING(sort_order, 15, 7))
    END || CASE
      WHEN SUBSTRING(sort_order, 22, 7) IS NULL
      THEN ''
      ELSE '.' || TRIM(0 FROM SUBSTRING(sort_order, 22, 7))
    END || CASE
      WHEN SUBSTRING(sort_order, 29, 7) IS NULL
      THEN ''
      ELSE '.' || TRIM(0 FROM SUBSTRING(sort_order, 29, 7))
    END || CASE
      WHEN SUBSTRING(sort_order, 36, 7) IS NULL
      THEN ''
      ELSE '.' || TRIM(0 FROM SUBSTRING(sort_order, 36, 7))
    END || CASE
      WHEN SUBSTRING(sort_order, 43, 7) IS NULL
      THEN ''
      ELSE '.' || TRIM(0 FROM SUBSTRING(sort_order, 43, 7))
    END || CASE
      WHEN SUBSTRING(sort_order, 50, 7) IS NULL
      THEN ''
      ELSE '.' || TRIM(0 FROM SUBSTRING(sort_order, 50, 7))
    END || CASE
      WHEN SUBSTRING(sort_order, 57, 7) IS NULL
      THEN ''
      ELSE '.' || TRIM(0 FROM SUBSTRING(sort_order, 57, 7))
    END || CASE
      WHEN SUBSTRING(sort_order, 64, 7) IS NULL
      THEN ''
      ELSE '.' || TRIM(0 FROM SUBSTRING(sort_order, 64, 7))
    END || CASE
      WHEN SUBSTRING(sort_order, 71, 7) IS NULL
      THEN ''
      ELSE '.' || TRIM(0 FROM SUBSTRING(sort_order, 71, 7))
    END || CASE
      WHEN SUBSTRING(sort_order, 78, 7) IS NULL
      THEN ''
      ELSE '.' || TRIM(0 FROM SUBSTRING(sort_order, 78, 7))
    END || CASE
      WHEN SUBSTRING(sort_order, 85, 7) IS NULL
      THEN ''
      ELSE '.' || TRIM(0 FROM SUBSTRING(sort_order, 85, 7))
    END || CASE
      WHEN SUBSTRING(sort_order, 92, 7) IS NULL
      THEN ''
      ELSE '.' || TRIM(0 FROM SUBSTRING(sort_order, 92, 7))
    END || CASE
      WHEN SUBSTRING(sort_order, 99, 7) IS NULL
      THEN ''
      ELSE '.' || TRIM(0 FROM SUBSTRING(sort_order, 99, 7))
    END || CASE
      WHEN SUBSTRING(sort_order, 106, 7) IS NULL
      THEN ''
      ELSE '.' || TRIM(0 FROM SUBSTRING(sort_order, 106, 7))
    END || CASE
      WHEN SUBSTRING(sort_order, 113, 7) IS NULL
      THEN ''
      ELSE '.' || TRIM(0 FROM SUBSTRING(sort_order, 113, 7))
    END || CASE
      WHEN SUBSTRING(sort_order, 120, 7) IS NULL
      THEN ''
      ELSE '.' || TRIM(0 FROM SUBSTRING(sort_order, 120, 7))
    END || CASE
      WHEN SUBSTRING(sort_order, 127, 7) IS NULL
      THEN ''
      ELSE '.' || TRIM(0 FROM SUBSTRING(sort_order, 127, 7))
    END) AS sort_order,
    msi2.segment1 AS top_assembly,
    msi3.segment1 AS assembly,
    msi.segment1 AS component,
    xib.plan_level AS bom_level,
    REPLACE(
      REPLACE(REPLACE(REPLACE(msi.description, '&', ' AND '), '<', ' '), '>', ' '),
      'Â°',
      'DEG '
    ) AS DESCRIPTION,
    CASE
      WHEN msi.planning_make_buy_code = 1
      THEN 'Make'
      WHEN msi.planning_make_buy_code = 2
      THEN 'Buy'
      ELSE NULL
    END AS item_type,
    CASE
      WHEN msi3.planning_make_buy_code = 1
      THEN 'Parent Is Make'
      WHEN msi3.planning_make_buy_code = 2
      THEN 'Parent Is Buy'
      ELSE NULL
    END AS parent_is_make_buy,
    COALESCE(
      xib.attribute12,
      CASE
        WHEN msi.planning_make_buy_code = 1
        THEN 'Make'
        WHEN msi.planning_make_buy_code = 2
        THEN 'Buy'
        ELSE NULL
      END
    ) AS Make_buy,
    msi.inventory_item_status_code AS status,
    xib.item_num,
    xib.operation_seq_num,
    msi.primary_uom_code,
    xib.component_quantity,
    xib.extended_quantity,
    CASE
      WHEN xib.extend_cost_flag = 1
      THEN 'Yes'
      WHEN xib.extend_cost_flag = 2
      THEN 'No'
    END AS Costed_Flag,
    lu1.meaning AS wip_supply_type,
    lu2.meaning AS bom_type
  FROM (
    SELECT
      *
    FROM silver_bec_ods.xxbec_ibom_rc_temp
    WHERE
      is_deleted_flg <> 'Y'
  ) AS xib, (
    SELECT
      *
    FROM silver_bec_ods.mtl_system_items_b
    WHERE
      is_deleted_flg <> 'Y'
  ) AS msi, (
    SELECT
      *
    FROM silver_bec_ods.mtl_system_items_b
    WHERE
      is_deleted_flg <> 'Y'
  ) AS msi2, (
    SELECT
      *
    FROM silver_bec_ods.bom_structures_b
    WHERE
      is_deleted_flg <> 'Y'
  ) AS bom, (
    SELECT
      *
    FROM silver_bec_ods.mtl_system_items_b
    WHERE
      is_deleted_flg <> 'Y'
  ) AS msi3, (
    SELECT
      *
    FROM silver_bec_ods.fnd_lookup_values
    WHERE
      is_deleted_flg <> 'Y'
  ) AS lu1, (
    SELECT
      *
    FROM silver_bec_ods.fnd_lookup_values
    WHERE
      is_deleted_flg <> 'Y'
  ) AS lu2
  WHERE
    1 = 1
    AND xib.bill_sequence_id = bom.bill_sequence_id
    AND xib.organization_id = bom.organization_id
    AND bom.assembly_item_id = msi3.inventory_item_id
    AND bom.organization_id = msi3.organization_id
    AND xib.component_item_id = msi.INVENTORY_ITEM_ID()
    AND xib.organization_id = msi.ORGANIZATION_ID()
    AND xib.top_item_id = msi2.INVENTORY_ITEM_ID()
    AND xib.organization_id = msi2.ORGANIZATION_ID()
    AND msi.wip_supply_type = lu1.LOOKUP_CODE
    AND lu1.LOOKUP_TYPE = 'WIP_SUPPLY'
    AND msi.BOM_ITEM_TYPE = lu2.LOOKUP_CODE
    AND lu2.LOOKUP_TYPE = 'BOM_ITEM_TYPE'
);
UPDATE bec_etl_ctrl.batch_dw_info SET load_type = 'I', last_refresh_date = CURRENT_TIMESTAMP()
WHERE
  dw_table_name = 'fact_bom_cost_details' AND batch_name = 'costing';