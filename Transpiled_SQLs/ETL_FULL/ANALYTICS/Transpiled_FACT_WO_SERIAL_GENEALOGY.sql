DROP table IF EXISTS gold_bec_dwh.FACT_WO_SERIAL_GENEALOGY;
CREATE TABLE gold_bec_dwh.FACT_WO_SERIAL_GENEALOGY AS
(
  SELECT
    part_number,
    inventory_item_id,
    organization_id,
    (
      SELECT
        system_id
      FROM bec_etl_ctrl.etlsourceappid
      WHERE
        source_system = 'EBS'
    ) || '-' || organization_id AS organization_id_key,
    part_description,
    serial_number,
    transaction_id,
    transaction_date,
    transaction_type_id,
    transaction_source_id,
    transaction_type,
    transaction_source_name,
    wip_entity_id,
    work_order,
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
    ) || '-' || COALESCE(inventory_item_id, 0) || '-' || COALESCE(organization_id, 0) || '-' || COALESCE(serial_number, 'NA') || '-' || COALESCE(transaction_id, 0) || '-' || COALESCE(wip_entity_id, 0) AS dw_load_id,
    CURRENT_TIMESTAMP() AS dw_insert_date,
    CURRENT_TIMESTAMP() AS dw_update_date
  FROM (
    SELECT
      msi.segment1 AS PART_NUMBER,
      msi.inventory_item_id,
      msi.organization_id,
      msi.description AS Part_Description,
      mut.serial_number,
      mmt.transaction_id,
      mmt.transaction_Date,
      mmt.transaction_type_id,
      mmt.transaction_source_id,
      mtt.transaction_type_name AS Transaction_Type,
      mmt.TRANSACTION_SOURCE_NAME,
      we.WIP_ENTITY_ID,
      we.WIP_ENTITY_NAME AS WORK_ORDER
    FROM (
      SELECT
        *
      FROM silver_bec_ods.mtl_material_transactions
      WHERE
        IS_DELETED_FLG <> 'Y'
    ) AS mmt, (
      SELECT
        *
      FROM silver_bec_ods.mtl_unit_transactions
      WHERE
        IS_DELETED_FLG <> 'Y'
    ) AS mut, (
      SELECT
        *
      FROM silver_bec_ods.mtl_system_items_b
      WHERE
        IS_DELETED_FLG <> 'Y'
    ) AS msi, (
      SELECT
        *
      FROM silver_bec_ods.mtl_transaction_types
      WHERE
        IS_DELETED_FLG <> 'Y'
    ) AS mtt, (
      SELECT
        *
      FROM silver_bec_ods.wip_entities
      WHERE
        IS_DELETED_FLG <> 'Y'
    ) AS we
    WHERE
      mmt.transaction_id = mut.transaction_id
      AND mmt.inventory_item_id = msi.INVENTORY_ITEM_ID()
      AND mmt.organization_Id = msi.ORGANIZATION_ID()
      AND mmt.transaction_type_id = mtt.TRANSACTION_TYPE_ID()
      AND mmt.transaction_source_id = we.WIP_ENTITY_ID()
      AND mmt.organization_id = we.ORGANIZATION_ID()
      AND mmt.transaction_type_id IN (44)
    UNION
    SELECT
      msi.segment1 AS PART_NUMBER,
      msi.inventory_item_id,
      msi.organization_id,
      msi.description AS Part_Description,
      mut.serial_number,
      mmt.transaction_id,
      mmt.transaction_Date,
      mmt.transaction_type_id,
      mmt.transaction_source_id,
      mtt.transaction_type_name AS Transaction_Type,
      mmt.TRANSACTION_SOURCE_NAME,
      we.WIP_ENTITY_ID,
      we.WIP_ENTITY_NAME AS WORK_ORDER
    FROM (
      SELECT
        *
      FROM silver_bec_ods.mtl_material_transactions
      WHERE
        IS_DELETED_FLG <> 'Y'
    ) AS mmt, (
      SELECT
        *
      FROM silver_bec_ods.mtl_unit_transactions
      WHERE
        IS_DELETED_FLG <> 'Y'
    ) AS mut, (
      SELECT
        *
      FROM silver_bec_ods.mtl_system_items_b
      WHERE
        IS_DELETED_FLG <> 'Y'
    ) AS msi, (
      SELECT
        *
      FROM silver_bec_ods.mtl_transaction_types
      WHERE
        IS_DELETED_FLG <> 'Y'
    ) AS mtt, (
      SELECT
        *
      FROM silver_bec_ods.wip_entities
      WHERE
        IS_DELETED_FLG <> 'Y'
    ) AS we
    WHERE
      mmt.transaction_id = mut.transaction_id
      AND mmt.inventory_item_id = msi.INVENTORY_ITEM_ID()
      AND mmt.organization_Id = msi.ORGANIZATION_ID()
      AND mmt.transaction_type_id = mtt.TRANSACTION_TYPE_ID()
      AND mmt.transaction_source_id = we.WIP_ENTITY_ID()
      AND mmt.organization_id = we.ORGANIZATION_ID()
      AND mmt.transaction_type_id IN (35, 17, 43, 44)
      AND mmt.INVENTORY_ITEM_ID IN (1279864, 1229865, 1280864, 2203875)
      AND mmt.transaction_Source_id IN (
        SELECT
          mmt.transaction_source_id
        FROM silver_bec_ods.mtl_material_transactions AS mmt, silver_bec_ods.mtl_unit_transactions AS mut
        WHERE
          mmt.transaction_id = mut.transaction_id
          AND mmt.transaction_type_id IN (35, 17, 43, 44)
      )
    UNION
    SELECT
      msi.segment1 AS PART_NUMBER,
      msi.inventory_item_id,
      msi.organization_id,
      msi.description AS Part_Description,
      mut.serial_number,
      mmt.transaction_id,
      mmt.transaction_Date,
      mmt.transaction_type_id,
      mmt.transaction_source_id,
      mtt.transaction_type_name AS Transaction_Type,
      mmt.TRANSACTION_SOURCE_NAME,
      we.WIP_ENTITY_ID,
      we.WIP_ENTITY_NAME AS WORK_ORDER
    FROM (
      SELECT
        *
      FROM silver_bec_ods.mtl_material_transactions
      WHERE
        IS_DELETED_FLG <> 'Y'
    ) AS mmt, (
      SELECT
        *
      FROM silver_bec_ods.mtl_unit_transactions
      WHERE
        IS_DELETED_FLG <> 'Y'
    ) AS mut, (
      SELECT
        *
      FROM silver_bec_ods.mtl_system_items_b
      WHERE
        IS_DELETED_FLG <> 'Y'
    ) AS msi, (
      SELECT
        *
      FROM silver_bec_ods.mtl_transaction_types
      WHERE
        IS_DELETED_FLG <> 'Y'
    ) AS mtt, (
      SELECT
        *
      FROM silver_bec_ods.wip_entities
      WHERE
        IS_DELETED_FLG <> 'Y'
    ) AS we
    WHERE
      mmt.transaction_id = mut.transaction_id
      AND mmt.inventory_item_id = msi.INVENTORY_ITEM_ID()
      AND mmt.organization_Id = msi.ORGANIZATION_ID()
      AND mmt.transaction_type_id = mtt.TRANSACTION_TYPE_ID()
      AND mmt.transaction_source_id = we.WIP_ENTITY_ID()
      AND mmt.organization_id = we.ORGANIZATION_ID()
      AND mmt.transaction_type_id IN (35, 17, 43, 44)
      AND mut.serial_number IN (
        SELECT
          mut.serial_number
        FROM silver_bec_ods.mtl_material_transactions AS mmt, silver_bec_ods.mtl_unit_transactions AS mut
        WHERE
          mmt.transaction_id = mut.transaction_id
          AND mmt.transaction_type_id IN (35, 17, 43, 44)
      )
      AND mmt.transaction_Source_id IN (
        SELECT
          mmt.transaction_source_id
        FROM silver_bec_ods.mtl_material_transactions AS mmt, silver_bec_ods.mtl_unit_transactions AS mut
        WHERE
          mmt.transaction_id = mut.transaction_id
          AND mmt.transaction_type_id IN (35, 17, 43, 44)
      )
    UNION
    SELECT
      msi.segment1 AS PART_NUMBER,
      msi.inventory_item_id,
      msi.organization_id,
      msi.description AS Part_Description,
      mut.serial_number,
      mmt.transaction_id,
      mmt.transaction_Date,
      mmt.transaction_type_id,
      mmt.transaction_source_id,
      mtt.transaction_type_name AS Transaction_Type,
      mmt.TRANSACTION_SOURCE_NAME,
      we.WIP_ENTITY_ID,
      we.WIP_ENTITY_NAME AS WORK_ORDER
    FROM (
      SELECT
        *
      FROM silver_bec_ods.mtl_material_transactions
      WHERE
        IS_DELETED_FLG <> 'Y'
    ) AS mmt, (
      SELECT
        *
      FROM silver_bec_ods.mtl_unit_transactions
      WHERE
        IS_DELETED_FLG <> 'Y'
    ) AS mut, (
      SELECT
        *
      FROM silver_bec_ods.mtl_system_items_b
      WHERE
        IS_DELETED_FLG <> 'Y'
    ) AS msi, (
      SELECT
        *
      FROM silver_bec_ods.mtl_transaction_types
      WHERE
        IS_DELETED_FLG <> 'Y'
    ) AS mtt, (
      SELECT
        *
      FROM silver_bec_ods.wip_entities
      WHERE
        IS_DELETED_FLG <> 'Y'
    ) AS we
    WHERE
      mmt.transaction_id = mut.transaction_id
      AND mmt.inventory_item_id = msi.INVENTORY_ITEM_ID()
      AND mmt.organization_Id = msi.ORGANIZATION_ID()
      AND mmt.transaction_type_id = mtt.TRANSACTION_TYPE_ID()
      AND mmt.transaction_source_id = we.WIP_ENTITY_ID()
      AND mmt.organization_id = we.ORGANIZATION_ID()
      AND mmt.transaction_type_id IN (35, 17, 43, 44)
      AND mmt.TRANSACTION_SOURCE_ID IN (
        SELECT
          mmt.transaction_Source_id
        FROM silver_bec_ods.mtl_material_transactions AS mmt, silver_bec_ods.mtl_unit_transactions AS mut, silver_bec_ods.mtl_system_items_b AS msi, silver_bec_ods.mtl_transaction_types AS mtt, silver_bec_ods.wip_entities AS we
        WHERE
          mmt.transaction_id = mut.transaction_id
          AND mmt.inventory_item_id = msi.INVENTORY_ITEM_ID()
          AND mmt.organization_Id = msi.ORGANIZATION_ID()
          AND mmt.transaction_type_id = mtt.TRANSACTION_TYPE_ID()
          AND mmt.transaction_source_id = we.WIP_ENTITY_ID()
          AND mmt.organization_id = we.ORGANIZATION_ID()
          AND mmt.transaction_type_id IN (35, 17, 43, 44)
          AND we.WIP_ENTITY_NAME LIKE 'PWM%'
          AND mut.serial_number IN (
            SELECT
              mut.serial_number
            FROM silver_bec_ods.mtl_material_transactions AS mmt, silver_bec_ods.mtl_unit_transactions AS mut
            WHERE
              mmt.transaction_id = mut.transaction_id
              AND mmt.transaction_type_id IN (35, 17, 43, 44)
              AND mmt.transaction_Source_id IN (
                SELECT
                  mmt.transaction_source_id
                FROM silver_bec_ods.mtl_material_transactions AS mmt, silver_bec_ods.mtl_unit_transactions AS mut
                WHERE
                  mmt.transaction_id = mut.transaction_id
                  AND mmt.transaction_type_id IN (35, 17, 43, 44)
              )
          )
      )
  )
);
UPDATE bec_etl_ctrl.batch_dw_info SET load_type = 'I', last_refresh_date = CURRENT_TIMESTAMP()
WHERE
  dw_table_name = 'fact_wo_serial_genealogy' AND batch_name = 'wip';