/* Delete Records */
DELETE FROM gold_bec_dwh.FACT_INVENTORY_AGING
WHERE
  (COALESCE(organization_id, 0), COALESCE(inventory_item_id, 0), COALESCE(subinventory_code, 'NA'), COALESCE(locator_id, 0), COALESCE(onhand_quantities_id, 0), COALESCE(orig_date_received, '1900-01-01 12:00:00')) IN (
    SELECT
      COALESCE(ods.organization_id, 0) AS organization_id,
      COALESCE(ods.inventory_item_id, 0) AS inventory_item_id,
      COALESCE(ods.subinventory_code, 'NA') AS subinventory_code,
      COALESCE(ods.locator_id, 0) AS locator_id,
      COALESCE(ods.onhand_quantities_id, 0) AS onhand_quantities_id,
      COALESCE(ods.orig_date_received, '1900-01-01 12:00:00') AS orig_date_received
    FROM gold_bec_dwh.FACT_INVENTORY_AGING AS dw, (
      SELECT
        moqd.organization_id,
        msib.inventory_item_id,
        moqd.subinventory_code,
        mil.inventory_location_id AS locator_id,
        moqd.onhand_quantities_id,
        moqd.orig_date_received
      FROM silver_bec_ods.mtl_onhand_quantities_detail AS moqd, silver_bec_ods.mtl_system_items_b AS msib, silver_bec_ods.mtl_secondary_inventories AS msiv, silver_bec_ods.mtl_item_categories AS mic, silver_bec_ods.mtl_categories_b AS mc, silver_bec_ods.mtl_item_locations AS mil, silver_bec_ods.cst_cost_groups AS ccg, silver_bec_ods.cst_quantity_layers AS cql, silver_bec_ods.cst_item_costs AS cic, silver_bec_ods.mtl_parameters AS mp
      WHERE
        moqd.organization_id = msib.organization_id
        AND mp.organization_id = moqd.organization_id
        AND cic.inventory_item_id = moqd.inventory_item_id
        AND cql.INVENTORY_ITEM_ID() = cic.inventory_item_id
        AND cql.ORGANIZATION_ID() = cic.organization_id
        AND mp.cost_organization_id = cic.organization_id
        AND mp.primary_cost_method = cic.cost_type_id
        AND CASE
          WHEN cql.inventory_item_id IS NULL
          THEN CASE
            WHEN mp.primary_cost_method = 1
            THEN 1
            ELSE COALESCE(mp.default_cost_group_id, 1)
          END
          ELSE cql.cost_group_id
        END = CASE
          WHEN mp.primary_cost_method = 1
          THEN 1
          ELSE COALESCE(moqd.cost_group_id, mp.default_cost_group_id)
        END
        AND moqd.inventory_item_id = msib.inventory_item_id
        AND msiv.organization_id = moqd.organization_id
        AND msiv.secondary_inventory_name = moqd.subinventory_code
        AND moqd.cost_group_id = ccg.cost_group_id
        AND msib.organization_id = mic.organization_id
        AND msib.inventory_item_id = mic.inventory_item_id
        AND mc.category_id = mic.category_id
        AND moqd.organization_id = mil.ORGANIZATION_ID()
        AND moqd.locator_id = mil.INVENTORY_LOCATION_ID()
        AND mic.category_set_id = 1
        AND (
          msib.kca_seq_date >= (
            SELECT
              (
                executebegints - prune_days
              )
            FROM bec_etl_ctrl.batch_dw_info
            WHERE
              dw_table_name = 'fact_inventory_aging' AND batch_name = 'costing'
          )
          OR mic.kca_seq_date >= (
            SELECT
              (
                executebegints - prune_days
              )
            FROM bec_etl_ctrl.batch_dw_info
            WHERE
              dw_table_name = 'fact_inventory_aging' AND batch_name = 'costing'
          )
          OR cic.kca_seq_date >= (
            SELECT
              (
                executebegints - prune_days
              )
            FROM bec_etl_ctrl.batch_dw_info
            WHERE
              dw_table_name = 'fact_inventory_aging' AND batch_name = 'costing'
          )
          OR moqd.kca_seq_date >= (
            SELECT
              (
                executebegints - prune_days
              )
            FROM bec_etl_ctrl.batch_dw_info
            WHERE
              dw_table_name = 'fact_inventory_aging' AND batch_name = 'costing'
          )
          OR mil.kca_seq_date >= (
            SELECT
              (
                executebegints - prune_days
              )
            FROM bec_etl_ctrl.batch_dw_info
            WHERE
              dw_table_name = 'fact_inventory_aging' AND batch_name = 'costing'
          )
          OR mc.kca_seq_date >= (
            SELECT
              (
                executebegints - prune_days
              )
            FROM bec_etl_ctrl.batch_dw_info
            WHERE
              dw_table_name = 'fact_inventory_aging' AND batch_name = 'costing'
          )
          OR msiv.kca_seq_date >= (
            SELECT
              (
                executebegints - prune_days
              )
            FROM bec_etl_ctrl.batch_dw_info
            WHERE
              dw_table_name = 'fact_inventory_aging' AND batch_name = 'costing'
          )
          OR ccg.kca_seq_date >= (
            SELECT
              (
                executebegints - prune_days
              )
            FROM bec_etl_ctrl.batch_dw_info
            WHERE
              dw_table_name = 'fact_inventory_aging' AND batch_name = 'costing'
          )
          OR msib.is_deleted_flg = 'Y'
          OR mic.is_deleted_flg = 'Y'
          OR cic.is_deleted_flg = 'Y'
          OR moqd.is_deleted_flg = 'Y'
          OR mil.is_deleted_flg = 'Y'
          OR mc.is_deleted_flg = 'Y'
          OR msiv.is_deleted_flg = 'Y'
          OR ccg.is_deleted_flg = 'Y'
          OR cql.is_deleted_flg = 'Y'
          OR mp.is_deleted_flg = 'Y'
        )
    ) AS ods
    WHERE
      1 = 1
      AND dw.dw_load_id = (
        SELECT
          system_id
        FROM bec_etl_ctrl.etlsourceappid
        WHERE
          source_system = 'EBS'
      ) || '-' || COALESCE(ods.organization_id, 0) || '-' || COALESCE(ods.inventory_item_id, 0) || '-' || COALESCE(ods.subinventory_code, 'NA') || '-' || COALESCE(ods.locator_id, 0) || '-' || COALESCE(ods.onhand_quantities_id, 0) || '-' || COALESCE(ods.orig_date_received, '1900-01-01 12:00:00')
  );
/* Insert Records */
INSERT INTO gold_bec_dwh.FACT_INVENTORY_AGING (
  ORGANIZATION_ID,
  INVENTORY_ITEM_ID,
  SUBINVENTORY_CODE,
  LOCATOR_CODE,
  LOCATOR_ID,
  ITEM_CODE,
  ITEM_CATEGORY,
  ITEM_DESC,
  UOM_CODE,
  OWING_PARTY,
  INVENTORY_ASSET_FLAG,
  ASSET_INVENTORY,
  AGING_CAL,
  ONHAND_QUANTITIES_ID,
  ORIG_DATE_RECEIVED,
  include_expense_items,
  include_expense_sub_inventory,
  ORG_LAST_TRX_DATE,
  SUBINV_LAST_TRX_DATE,
  LOC_LAST_TRX_DATE,
  ORG_FIRST_TRX_DATE,
  SUBINV_FIRST_TRX_DATE,
  LOC_FIRST_TRX_DATE,
  ON_HAND,
  ORG_SUM_ON_HAND,
  SUBINV_SUM_ON_HAND,
  LOC_SUM_ON_HAND,
  NET_VALUE,
  ORGANIZATION_ID_KEY,
  INVENTORY_ITEM_ID_KEY,
  SUBINVENTORY_CODE_KEY,
  LOCATOR_ID_KEY,
  is_deleted_flg,
  SOURCE_APP_ID,
  DW_LOAD_ID,
  DW_INSERT_DATE,
  DW_UPDATE_DATE
)
(
  SELECT
    organization_id,
    inventory_item_id,
    subinventory_code,
    locator_code,
    locator_id,
    item_code,
    item_category,
    item_desc,
    uom_code,
    owing_party,
    inventory_asset_flag,
    asset_inventory,
    aging_cal,
    onhand_quantities_id,
    orig_date_received,
    include_expense_items,
    include_expense_sub_inventory,
    FLOOR(MAX(org_last_trx_date)) AS org_last_trx_date,
    FLOOR(MAX(subinv_last_trx_date)) AS subinv_last_trx_date,
    FLOOR(MAX(loc_last_trx_date)) AS loc_last_trx_date,
    FLOOR(MIN(org_first_trx_date)) AS org_first_trx_date,
    FLOOR(MIN(subinv_first_trx_date)) AS subinv_first_trx_date,
    FLOOR(MIN(loc_first_trx_date)) AS loc_first_trx_date,
    SUM(on_hand) AS on_hand,
    MAX(org_sum_on_hand) AS org_sum_on_hand,
    MAX(subinv_sum_on_hand) AS subinv_sum_on_hand,
    MAX(loc_sum_on_hand) AS loc_sum_on_hand,
    SUM(net_value) AS net_value,
    (
      SELECT
        system_id
      FROM bec_etl_ctrl.etlsourceappid
      WHERE
        source_system = 'EBS'
    ) || '-' || organization_id AS organization_id_KEY,
    (
      SELECT
        system_id
      FROM bec_etl_ctrl.etlsourceappid
      WHERE
        source_system = 'EBS'
    ) || '-' || inventory_item_id AS inventory_item_id_KEY,
    (
      SELECT
        system_id
      FROM bec_etl_ctrl.etlsourceappid
      WHERE
        source_system = 'EBS'
    ) || '-' || subinventory_code AS subinventory_code_KEY,
    (
      SELECT
        system_id
      FROM bec_etl_ctrl.etlsourceappid
      WHERE
        source_system = 'EBS'
    ) || '-' || locator_id AS locator_id_KEY,
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
    ) || '-' || COALESCE(organization_id, 0) || '-' || COALESCE(inventory_item_id, 0) || '-' || COALESCE(subinventory_code, 'NA') || '-' || COALESCE(locator_id, 0) || '-' || COALESCE(onhand_quantities_id, 0) || '-' || COALESCE(orig_date_received, '1900-01-01 12:00:00') AS dw_load_id,
    CURRENT_TIMESTAMP() AS dw_insert_date,
    CURRENT_TIMESTAMP() AS dw_update_date
  FROM (
    SELECT
      msib.segment1 AS item_code,
      msib.inventory_item_id,
      moqd.organization_id,
      moqd.subinventory_code,
      ccg.cost_group,
      msib.inventory_asset_flag,
      msiv.asset_inventory,
      mil.segment1 || '.' || mil.segment2 || '.' || mil.segment3 AS locator_code,
      mil.inventory_location_id AS locator_id,
      mc.segment3 || '.' || mc.segment4 || '.' || mc.segment1 || '.' || mc.segment2 || '.' || mc.segment5 || '.' || mc.segment6 AS item_category,
      msib.description AS item_desc,
      CASE
        WHEN is_consigned = 1
        THEN CASE
          WHEN moqd.owning_tp_type = 1
          THEN (
            SELECT
              vendor_name || '-' || pvsa.vendor_site_code
            FROM (
              SELECT
                *
              FROM silver_bec_ods.po_vendors
              WHERE
                is_deleted_flg <> 'Y'
            ) AS pv, (
              SELECT
                *
              FROM silver_bec_ods.ap_supplier_sites_all
              WHERE
                is_deleted_flg <> 'Y'
            ) AS pvsa
            WHERE
              pvsa.vendor_id = pv.vendor_id AND pvsa.vendor_site_id = moqd.owning_organization_id
          )
          WHEN moqd.owning_tp_type = 2
          THEN (
            SELECT
              (
                SUBSTRING(hout.name, 1, 60) || '-' || SUBSTRING(mp.organization_code, 1, 3)
              ) AS party
            FROM (
              SELECT
                *
              FROM silver_bec_ods.hr_organization_information
              WHERE
                is_deleted_flg <> 'Y'
            ) AS hoi, (
              SELECT
                *
              FROM silver_bec_ods.hr_all_organization_units_tl
              WHERE
                is_deleted_flg <> 'Y'
            ) AS hout, (
              SELECT
                *
              FROM silver_bec_ods.mtl_parameters
              WHERE
                is_deleted_flg <> 'Y'
            ) AS mp
            WHERE
              hoi.organization_id = hout.organization_id
              AND hout.organization_id = mp.organization_id
              AND hout.language = 'US'
              AND hoi.org_information1 = 'OPERATING_UNIT'
              AND hoi.org_information2 = 'Y'
              AND hoi.org_information_context = 'CLASS'
              AND hoi.organization_id = moqd.owning_organization_id
              AND moqd.organization_id <> moqd.owning_organization_id
          )
        END
        ELSE NULL
      END AS owing_party,
      MAX(moqd.last_update_date) OVER (PARTITION BY moqd.organization_id, moqd.inventory_item_id, moqd.is_consigned, moqd.owning_tp_type, CASE WHEN moqd.is_consigned = 1 THEN moqd.owning_organization_id ELSE NULL END) AS org_last_trx_date,
      MAX(moqd.last_update_date) OVER (PARTITION BY moqd.organization_id, moqd.inventory_item_id, moqd.subinventory_code, moqd.is_consigned, moqd.owning_tp_type, CASE WHEN moqd.is_consigned = 1 THEN moqd.owning_organization_id ELSE NULL END) AS subinv_last_trx_date,
      MAX(moqd.last_update_date) OVER (PARTITION BY moqd.organization_id, moqd.inventory_item_id, moqd.subinventory_code, moqd.locator_id, moqd.is_consigned, moqd.owning_tp_type, CASE WHEN moqd.is_consigned = 1 THEN moqd.owning_organization_id ELSE NULL END) AS loc_last_trx_date,
      MIN(moqd.last_update_date) OVER (PARTITION BY moqd.organization_id, moqd.inventory_item_id, moqd.is_consigned, moqd.owning_tp_type, CASE WHEN moqd.is_consigned = 1 THEN moqd.owning_organization_id ELSE NULL END) AS org_first_trx_date,
      MIN(moqd.last_update_date) OVER (PARTITION BY moqd.organization_id, moqd.inventory_item_id, moqd.subinventory_code, moqd.is_consigned, moqd.owning_tp_type, CASE WHEN moqd.is_consigned = 1 THEN moqd.owning_organization_id ELSE NULL END) AS subinv_first_trx_date,
      MIN(moqd.last_update_date) OVER (PARTITION BY moqd.organization_id, moqd.inventory_item_id, moqd.subinventory_code, moqd.locator_id, moqd.is_consigned, moqd.owning_tp_type, CASE WHEN moqd.is_consigned = 1 THEN moqd.owning_organization_id ELSE NULL END) AS loc_first_trx_date,
      moqd.primary_transaction_quantity AS on_hand,
      SUM(moqd.primary_transaction_quantity) OVER (PARTITION BY moqd.organization_id, moqd.inventory_item_id, moqd.is_consigned, moqd.owning_tp_type, CASE WHEN moqd.is_consigned = 1 THEN moqd.owning_organization_id ELSE NULL END) AS org_sum_on_hand,
      SUM(moqd.primary_transaction_quantity) OVER (PARTITION BY moqd.organization_id, moqd.inventory_item_id, moqd.subinventory_code, moqd.is_consigned, moqd.owning_tp_type, CASE WHEN moqd.is_consigned = 1 THEN moqd.owning_organization_id ELSE NULL END) AS subinv_sum_on_hand,
      SUM(moqd.primary_transaction_quantity) OVER (PARTITION BY moqd.organization_id, moqd.inventory_item_id, moqd.subinventory_code, moqd.locator_id, moqd.is_consigned, moqd.owning_tp_type, CASE WHEN moqd.is_consigned = 1 THEN moqd.owning_organization_id ELSE NULL END) AS loc_sum_on_hand,
      moqd.primary_transaction_quantity * COALESCE(
        CASE WHEN cql.inventory_item_id IS NULL THEN cic.item_cost ELSE cql.item_cost END,
        0
      ) AS net_value,
      msib.primary_uom_code AS uom_code,
      FLOOR(COALESCE(moqd.orig_date_received, moqd.creation_date)) AS aging_cal,
      moqd.onhand_quantities_id,
      moqd.orig_date_received,
      msib.INVENTORY_ASSET_FLAG AS INCLUDE_EXPENSE_ITEMS,
      msiv.asset_inventory AS include_expense_sub_inventory
    FROM (
      SELECT
        *
      FROM silver_bec_ods.mtl_onhand_quantities_detail
      WHERE
        is_deleted_flg <> 'Y'
    ) AS moqd, (
      SELECT
        *
      FROM silver_bec_ods.mtl_system_items_b
      WHERE
        is_deleted_flg <> 'Y'
    ) AS msib, (
      SELECT
        *
      FROM silver_bec_ods.mtl_secondary_inventories
      WHERE
        is_deleted_flg <> 'Y'
    ) AS msiv, (
      SELECT
        *
      FROM silver_bec_ods.mtl_item_categories
      WHERE
        is_deleted_flg <> 'Y'
    ) AS mic, (
      SELECT
        *
      FROM silver_bec_ods.mtl_categories_b
      WHERE
        is_deleted_flg <> 'Y'
    ) AS mc, (
      SELECT
        *
      FROM silver_bec_ods.mtl_item_locations
      WHERE
        is_deleted_flg <> 'Y'
    ) AS mil, (
      SELECT
        *
      FROM silver_bec_ods.cst_cost_groups
      WHERE
        is_deleted_flg <> 'Y'
    ) AS ccg, (
      SELECT
        *
      FROM silver_bec_ods.cst_quantity_layers
      WHERE
        is_deleted_flg <> 'Y'
    ) AS cql, (
      SELECT
        *
      FROM silver_bec_ods.cst_item_costs
      WHERE
        is_deleted_flg <> 'Y'
    ) AS cic, (
      SELECT
        *
      FROM silver_bec_ods.mtl_parameters
      WHERE
        is_deleted_flg <> 'Y'
    ) AS mp
    WHERE
      moqd.organization_id = msib.organization_id
      AND mp.organization_id = moqd.organization_id
      AND cic.inventory_item_id = moqd.inventory_item_id
      AND cql.INVENTORY_ITEM_ID() = cic.inventory_item_id
      AND cql.ORGANIZATION_ID() = cic.organization_id
      AND mp.cost_organization_id = cic.organization_id
      AND mp.primary_cost_method = cic.cost_type_id
      AND CASE
        WHEN cql.inventory_item_id IS NULL
        THEN CASE
          WHEN mp.primary_cost_method = 1
          THEN 1
          ELSE COALESCE(mp.default_cost_group_id, 1)
        END
        ELSE cql.cost_group_id
      END = CASE
        WHEN mp.primary_cost_method = 1
        THEN 1
        ELSE COALESCE(moqd.cost_group_id, mp.default_cost_group_id)
      END
      AND moqd.inventory_item_id = msib.inventory_item_id
      AND msiv.organization_id = moqd.organization_id
      AND msiv.secondary_inventory_name = moqd.subinventory_code
      AND moqd.cost_group_id = ccg.cost_group_id
      AND msib.organization_id = mic.organization_id
      AND msib.inventory_item_id = mic.inventory_item_id
      AND mc.category_id = mic.category_id
      AND moqd.organization_id = mil.ORGANIZATION_ID()
      AND moqd.locator_id = mil.INVENTORY_LOCATION_ID()
      AND mic.category_set_id = 1
      AND (
        msib.kca_seq_date >= (
          SELECT
            (
              executebegints - prune_days
            )
          FROM bec_etl_ctrl.batch_dw_info
          WHERE
            dw_table_name = 'fact_inventory_aging' AND batch_name = 'costing'
        )
        OR mic.kca_seq_date >= (
          SELECT
            (
              executebegints - prune_days
            )
          FROM bec_etl_ctrl.batch_dw_info
          WHERE
            dw_table_name = 'fact_inventory_aging' AND batch_name = 'costing'
        )
        OR cic.kca_seq_date >= (
          SELECT
            (
              executebegints - prune_days
            )
          FROM bec_etl_ctrl.batch_dw_info
          WHERE
            dw_table_name = 'fact_inventory_aging' AND batch_name = 'costing'
        )
        OR moqd.kca_seq_date >= (
          SELECT
            (
              executebegints - prune_days
            )
          FROM bec_etl_ctrl.batch_dw_info
          WHERE
            dw_table_name = 'fact_inventory_aging' AND batch_name = 'costing'
        )
        OR mil.kca_seq_date >= (
          SELECT
            (
              executebegints - prune_days
            )
          FROM bec_etl_ctrl.batch_dw_info
          WHERE
            dw_table_name = 'fact_inventory_aging' AND batch_name = 'costing'
        )
        OR mc.kca_seq_date >= (
          SELECT
            (
              executebegints - prune_days
            )
          FROM bec_etl_ctrl.batch_dw_info
          WHERE
            dw_table_name = 'fact_inventory_aging' AND batch_name = 'costing'
        )
        OR msiv.kca_seq_date >= (
          SELECT
            (
              executebegints - prune_days
            )
          FROM bec_etl_ctrl.batch_dw_info
          WHERE
            dw_table_name = 'fact_inventory_aging' AND batch_name = 'costing'
        )
        OR ccg.kca_seq_date >= (
          SELECT
            (
              executebegints - prune_days
            )
          FROM bec_etl_ctrl.batch_dw_info
          WHERE
            dw_table_name = 'fact_inventory_aging' AND batch_name = 'costing'
        )
      )
  )
  GROUP BY
    organization_id,
    inventory_item_id,
    subinventory_code,
    locator_code,
    locator_id,
    item_code,
    item_category,
    item_desc,
    uom_code,
    owing_party,
    inventory_asset_flag,
    asset_inventory,
    aging_cal,
    onhand_quantities_id,
    orig_date_received,
    include_expense_items,
    include_expense_sub_inventory
);
UPDATE bec_etl_ctrl.batch_dw_info SET load_type = 'I', last_refresh_date = CURRENT_TIMESTAMP()
WHERE
  dw_table_name = 'fact_inventory_aging' AND batch_name = 'costing';