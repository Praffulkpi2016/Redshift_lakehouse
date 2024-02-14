DROP table IF EXISTS gold_bec_dwh.FACT_INVENTORY_AGING;
CREATE TABLE gold_bec_dwh.FACT_INVENTORY_AGING AS
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
  INCLUDE_EXPENSE_ITEMS,
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
  include_expense_sub_inventory;
UPDATE bec_etl_ctrl.batch_dw_info SET load_type = 'I', last_refresh_date = CURRENT_TIMESTAMP()
WHERE
  dw_table_name = 'fact_inventory_aging' AND batch_name = 'costing';