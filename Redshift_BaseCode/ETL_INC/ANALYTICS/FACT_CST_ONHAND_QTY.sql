/*
# Copyright(c) 2022 KPI Partners, Inc. All Rights Reserved.
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
# WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
#
# author: KPI Partners, Inc.
# version: 2022.06
# description: This script represents incremental load approach for Facts.
# File Version: KPI v1.0
*/
begin;

Truncate table bec_dwh.FACT_CST_ONHAND_QTY;

Insert into bec_dwh.FACT_CST_ONHAND_QTY (
  select 
    *, 
    row_number() OVER (PARTITION BY dw_load_id) as ROWNUMBER 
  from 
    (
      SELECT 
        part_number, 
        cat_seg1, 
        cat_seg2, 
        category, 
        organization_name, 
        operating_unit, 
        description, 
        unit_of_measure, 
        inventory_item_status_code, 
        planning_make_buy_code, 
        mrp_planning_code, 
        quantity, 
        material_cost, 
        material_overhead_cost, 
        resource_cost, 
        outside_processing_cost, 
        overhead_cost, 
        item_cost, 
        nvl(material_cost, 0) * nvl(quantity, 0) as Ext_Material_Cost, 
        nvl(material_overhead_cost, 0) * nvl(quantity, 0) as Ext_material_overhead_cost, 
        nvl(resource_cost, 0) * nvl(quantity, 0) as Ext_resource_cost, 
        nvl(outside_processing_cost, 0) * nvl(quantity, 0) as Ext_outside_processing_cost, 
        nvl(overhead_cost, 0) * nvl(quantity, 0) as Ext_overhead_cost, 
        nvl(item_cost, 0) * nvl(quantity, 0) as Extended_cost, 
        subinventory, 
        locator, 
        serial_number, 
        lot_number, 
        date_received, 
        transaction_type_id, 
        transaction_source_type_id, 
        organization_id, 
        source_date_aging, 
        material_account, 
        subinventory_type, 
        subinventory_category, 
        subinventory_subcategory, 
        uom_code, 
        vmi_flag, 
        (
          select 
            system_id 
          from 
            bec_etl_ctrl.etlsourceappid 
          where 
            source_system = 'EBS'
        )|| '-' || organization_id as organization_id_KEY, 
        (
          select 
            system_id 
          from 
            bec_etl_ctrl.etlsourceappid 
          where 
            source_system = 'EBS'
        )|| '-' || transaction_type_id as transaction_type_id_KEY, 
        (
          select 
            system_id 
          from 
            bec_etl_ctrl.etlsourceappid 
          where 
            source_system = 'EBS'
        )|| '-' || transaction_source_type_id as transaction_source_type_id_KEY, 
        -- audit columns
        'N' as is_deleted_flg, 
        (
          select 
            system_id 
          from 
            bec_etl_ctrl.etlsourceappid 
          where 
            source_system = 'EBS'
        ) as source_app_id, 
        (
          select 
            system_id 
          from 
            bec_etl_ctrl.etlsourceappid 
          where 
            source_system = 'EBS'
        )|| '-' || nvl(part_number, 'NA')|| '-' || nvl(organization_id, 0) || '-' || nvl(locator, 'NA')|| '-' || nvl(serial_number, 'NA')|| '-' || nvl(lot_number, 'NA')|| '-' || nvl(
          date_received, '1900-01-01 12:00:00'
        )|| '-' || nvl(subinventory, 'NA')|| '-' || nvl(quantity, 0) as dw_load_id, 
        getdate() as dw_insert_date, 
        getdate() as dw_update_date 
      FROM 
        (
          (
            With CTE_CST_STANDARD_COSTS_U1 as (
              SELECT 
                standard_cost, 
                inventory_item_id, 
                organization_id 
              FROM 
                (select * from bec_ods.cst_standard_costs where is_deleted_flg <> 'Y') c 
              WHERE 
                standard_cost_revision_date <= GETDATE() 
                AND cost_update_id = (
                  SELECT 
                    MAX(cost_update_id) 
                  FROM 
                    (select * from bec_ods.cst_standard_costs where is_deleted_flg <> 'Y') c1 
                  WHERE 
                    inventory_item_id = c.inventory_item_id 
                    AND organization_id = c.organization_id 
                    AND standard_cost_revision_date <= GETDATE()
                )
            ), 
            CTE_CST_ELEMENTAL_COSTS_U1 as (
              SELECT 
                SUM(e.standard_cost) as standard_cost, 
                e.inventory_item_id, 
                e.organization_id 
              FROM 
                (select * from bec_ods.cst_elemental_costs where is_deleted_flg <> 'Y') e, 
                (
                  select 
                    distinct inventory_item_id, 
                    organization_id 
                  from 
                    bec_dwh.FACT_CST_ONHAND_QTY_UNION1_STG
                ) mmt 
              WHERE 
                1 = 1 
                AND e.inventory_item_id = mmt.inventory_item_id 
                AND e.organization_id = mmt.organization_id 
                AND e.creation_date = (
                  SELECT 
                    MAX(creation_date) 
                  FROM 
                    (select * from bec_ods.cst_elemental_costs where is_deleted_flg <> 'Y') e1 
                  WHERE 
                    e1.inventory_item_id = mmt.inventory_item_id 
                    AND e1.organization_id = mmt.organization_id 
                    AND e1.creation_date <= GETDATE()
                ) 
              Group by 
                e.inventory_item_id, 
                e.organization_id
            ), 
            CTE_CST_MATERIAL_COSTS_U1 as (
              SELECT 
                SUM(e.standard_cost) as standard_cost, 
                e.inventory_item_id, 
                e.organization_id 
              FROM 
                (select * from bec_ods.cst_elemental_costs where is_deleted_flg <> 'Y') e, 
                (
                  select 
                    distinct inventory_item_id, 
                    organization_id 
                  from 
                    bec_dwh.FACT_CST_ONHAND_QTY_UNION1_STG
                ) mmt 
              WHERE 
                1 = 1 
                AND e.cost_element_id = 1 
                AND e.inventory_item_id = mmt.inventory_item_id 
                AND e.organization_id = mmt.organization_id 
                AND e.creation_date = (
                  SELECT 
                    MAX(creation_date) 
                  FROM 
                    (select * from bec_ods.cst_elemental_costs where is_deleted_flg <> 'Y') e1 
                  WHERE 
                    e1.inventory_item_id = mmt.inventory_item_id 
                    AND e1.organization_id = mmt.organization_id 
                    AND e1.creation_date <= GETDATE()
                ) 
              Group by 
                e.inventory_item_id, 
                e.organization_id
            ), 
            CTE_CST_MATERIAL_OVERHEAD_COSTS_U1 as (
              SELECT 
                SUM(e.standard_cost) as standard_cost, 
                e.inventory_item_id, 
                e.organization_id 
              FROM 
                (select * from bec_ods.cst_elemental_costs where is_deleted_flg <> 'Y') e, 
                (
                  select 
                    distinct inventory_item_id, 
                    organization_id 
                  from 
                    bec_dwh.FACT_CST_ONHAND_QTY_UNION1_STG
                ) mmt 
              WHERE 
                1 = 1 
                AND e.cost_element_id = 2 
                AND e.inventory_item_id = mmt.inventory_item_id 
                AND e.organization_id = mmt.organization_id 
                AND e.creation_date = (
                  SELECT 
                    MAX(creation_date) 
                  FROM 
                    (select * from bec_ods.cst_elemental_costs where is_deleted_flg <> 'Y') e1 
                  WHERE 
                    e1.inventory_item_id = mmt.inventory_item_id 
                    AND e1.organization_id = mmt.organization_id 
                    AND e1.creation_date <= GETDATE()
                ) 
              Group by 
                e.inventory_item_id, 
                e.organization_id
            ), 
            CTE_CST_RESOURCE_COSTS_U1 as (
              SELECT 
                SUM(e.standard_cost) as standard_cost, 
                e.inventory_item_id, 
                e.organization_id 
              FROM 
                (select * from bec_ods.cst_elemental_costs where is_deleted_flg <> 'Y') e, 
                (
                  select 
                    distinct inventory_item_id, 
                    organization_id 
                  from 
                    bec_dwh.FACT_CST_ONHAND_QTY_UNION1_STG
                ) mmt 
              WHERE 
                1 = 1 
                AND e.cost_element_id = 3 
                AND e.inventory_item_id = mmt.inventory_item_id 
                AND e.organization_id = mmt.organization_id 
                AND e.creation_date = (
                  SELECT 
                    MAX(creation_date) 
                  FROM 
                    (select * from bec_ods.cst_elemental_costs where is_deleted_flg <> 'Y') e1 
                  WHERE 
                    e1.inventory_item_id = mmt.inventory_item_id 
                    AND e1.organization_id = mmt.organization_id 
                    AND e1.creation_date <= GETDATE()
                ) 
              Group by 
                e.inventory_item_id, 
                e.organization_id
            ), 
            CTE_CST_OUTSIDE_PROCESSING_COSTS_U1 as (
              SELECT 
                SUM(e.standard_cost) as standard_cost, 
                e.inventory_item_id, 
                e.organization_id 
              FROM 
                (select * from bec_ods.cst_elemental_costs where is_deleted_flg <> 'Y') e, 
                (
                  select 
                    distinct inventory_item_id, 
                    organization_id 
                  from 
                    bec_dwh.FACT_CST_ONHAND_QTY_UNION1_STG
                ) mmt 
              WHERE 
                1 = 1 
                AND e.cost_element_id = 4 
                AND e.inventory_item_id = mmt.inventory_item_id 
                AND e.organization_id = mmt.organization_id 
                AND e.creation_date = (
                  SELECT 
                    MAX(creation_date) 
                  FROM 
                    (select * from bec_ods.cst_elemental_costs where is_deleted_flg <> 'Y') e1 
                  WHERE 
                    e1.inventory_item_id = mmt.inventory_item_id 
                    AND e1.organization_id = mmt.organization_id 
                    AND e1.creation_date <= GETDATE()
                ) 
              Group by 
                e.inventory_item_id, 
                e.organization_id
            ), 
            CTE_CST_OVERHEAD_COSTS_U1 as (
              SELECT 
                SUM(e.standard_cost) as standard_cost, 
                e.inventory_item_id, 
                e.organization_id 
              FROM 
                (select * from bec_ods.cst_elemental_costs where is_deleted_flg <> 'Y') e, 
                (
                  select 
                    distinct inventory_item_id, 
                    organization_id 
                  from 
                    bec_dwh.FACT_CST_ONHAND_QTY_UNION1_STG
                ) mmt 
              WHERE 
                1 = 1 
                AND e.cost_element_id = 5 
                AND e.inventory_item_id = mmt.inventory_item_id 
                AND e.organization_id = mmt.organization_id 
                AND e.creation_date = (
                  SELECT 
                    MAX(creation_date) 
                  FROM 
                    (select * from bec_ods.cst_elemental_costs where is_deleted_flg <> 'Y') e1 
                  WHERE 
                    e1.inventory_item_id = mmt.inventory_item_id 
                    AND e1.organization_id = mmt.organization_id 
                    AND e1.creation_date <= GETDATE()
                ) 
              Group by 
                e.inventory_item_id, 
                e.organization_id
            ) 
            SELECT 
              mmt.segment1 AS part_number, 
              mmt.cat_seg1, 
              mmt.cat_seg2, 
              mmt.category, 
              mmt.organization_name, 
              mmt.operating_unit operating_unit, 
              mmt.description, 
              mmt.primary_unit_of_measure unit_of_measure, 
              mmt.primary_uom_code uom_code, 
              mmt.inventory_item_status_code, 
              decode(
                mmt.planning_make_buy_code, 1, 'Make', 
                2, 'Buy'
              ) planning_make_buy_code, 
              mmt.mrp_planning_code, 
              (
                SELECT 
                  SUM(primary_transaction_quantity) 
                FROM 
                  (select * from bec_ods.mtl_onhand_quantities_detail where is_deleted_flg <> 'Y') 
                WHERE 
                  inventory_item_id = mmt.inventory_item_id 
                  AND organization_id = mmt.organization_id 
                  AND subinventory_code = mmt.subinventory_code 
                  AND nvl(locator_id, -1) = nvl(mmt.locator_id, -1) 
                  AND create_transaction_id = mmt.create_transaction_id 
                  AND nvl(lot_number, '@#$') = nvl(mmt.lot_number, '@#$') 
                  AND date_received <= GETDATE() 
                  AND last_update_date <= GETDATE()
              ) quantity, 
              (
                CASE WHEN ccsc.standard_cost = 0 THEN 0 ELSE ccmc.standard_cost END
              ) material_cost, 
              (
                CASE WHEN ccsc.standard_cost = 0 THEN 0 ELSE ccmoc.standard_cost END
              ) material_overhead_cost, 
              (
                CASE WHEN ccsc.standard_cost = 0 THEN 0 ELSE ccrc.standard_cost END
              ) resource_cost, 
              (
                CASE WHEN ccsc.standard_cost = 0 THEN 0 ELSE ccopc.standard_cost END
              ) outside_processing_cost, 
              (
                CASE WHEN ccsc.standard_cost = 0 THEN 0 ELSE ccoc.standard_cost END
              ) overhead_cost, 
              (
                CASE WHEN ccsc.standard_cost = 0 THEN 0 ELSE ccec.standard_cost END
              ) item_cost, 
              mmt.transaction_date date_received, 
              DATEDIFF(
                'DAYS', 
                nvl(
                  nvl(
                    (
                      CASE WHEN mmt.planning_make_buy_code = 2 THEN (
                        SELECT 
                          (
                            MAX(t1.transaction_date)
                          ) 
                        FROM 
                          (select * from bec_ods.mtl_transaction_lot_numbers where is_deleted_flg <> 'Y') l1, 
                          bec_dwh.FACT_CST_ONHAND_QTY_STG t1 
                        WHERE 
                          l1.transaction_id = t1.transaction_id 
                          AND t1.transaction_type_id IN(18) 
                          AND t1.inventory_item_id = mmt.inventory_item_id 
                          AND(
                            nvl(l1.lot_number, '@#$') = nvl(mmt.lot_number, '#$%')
                          )
                      ) ELSE (
                        SELECT 
                          (
                            MAX(t1.transaction_date)
                          ) 
                        FROM 
                          (select * from bec_ods.mtl_transaction_lot_numbers where is_deleted_flg <> 'Y') l1, 
                          bec_dwh.FACT_CST_ONHAND_QTY_STG t1 
                        WHERE 
                          l1.transaction_id = t1.transaction_id 
                          AND t1.transaction_type_id IN(44) 
                          AND t1.inventory_item_id = mmt.inventory_item_id 
                          AND(
                            nvl(l1.lot_number, '@#$') = nvl(mmt.lot_number, '#$%')
                          )
                      ) END
                    ), 
                    (
                      SELECT 
                        (
                          MAX(t1.transaction_date)
                        ) 
                      FROM 
                        (select * from bec_ods.mtl_transaction_lot_numbers where is_deleted_flg <> 'Y') l1, 
                        bec_dwh.FACT_CST_ONHAND_QTY_STG t1 
                      WHERE 
                        l1.transaction_id = t1.transaction_id 
                        AND t1.transaction_type_id IN(40, 41, 42) --                                            AND t1.organization_id =
                        --                                                   msi.organization_id
                        AND t1.inventory_item_id = mmt.inventory_item_id 
                        AND(
                          nvl(l1.lot_number, '@#$') = nvl(mmt.lot_number, '#$%')
                        )
                    )
                  ), 
                  mmt.transaction_date
                )-1, 
                GETDATE()
              ) as source_date_aging, 
              mmt.organization_id, 
              mmt.transaction_source_type_id, 
              mmt.transaction_type_id, 
              mmt.lot_number, 
              NULL serial_number, 
              mmt.locator, 
              mmt.subinventory_code AS subinventory, 
              mmt.material_account, 
              decode(
                mmt.asset_inventory :: VARCHAR, 1, 
                'Asset', 2, 'Expense', asset_inventory :: VARCHAR
              ) subinventory_type, 
              mmt.attribute10 subinventory_category, 
              mmt.attribute11 subinventory_subcategory, 
              mmt.vmi_flag 
            FROM 
              bec_dwh.FACT_CST_ONHAND_QTY_UNION1_STG mmt, 
              CTE_CST_STANDARD_COSTS_U1 ccsc, 
              CTE_CST_ELEMENTAL_COSTS_U1 ccec, 
              CTE_CST_MATERIAL_COSTS_U1 ccmc, 
              CTE_CST_MATERIAL_OVERHEAD_COSTS_U1 ccmoc, 
              CTE_CST_RESOURCE_COSTS_U1 ccrc, 
              CTE_CST_OUTSIDE_PROCESSING_COSTS_U1 ccopc, 
              CTE_CST_OVERHEAD_COSTS_U1 ccoc 
            WHERE 
              ccsc.inventory_item_id(+) = mmt.inventory_item_id 
              AND ccsc.organization_id(+) = mmt.organization_id 
              AND ccec.inventory_item_id(+) = mmt.inventory_item_id 
              AND ccec.organization_id(+) = mmt.organization_id 
              AND ccmc.inventory_item_id(+) = mmt.inventory_item_id 
              AND ccmc.organization_id(+) = mmt.organization_id 
              AND ccmoc.inventory_item_id(+) = mmt.inventory_item_id 
              AND ccmoc.organization_id(+) = mmt.organization_id 
              AND ccrc.inventory_item_id(+) = mmt.inventory_item_id 
              AND ccrc.organization_id(+) = mmt.organization_id 
              AND ccopc.inventory_item_id(+) = mmt.inventory_item_id 
              AND ccopc.organization_id(+) = mmt.organization_id 
              AND ccoc.inventory_item_id(+) = mmt.inventory_item_id 
              AND ccoc.organization_id(+) = mmt.organization_id
          ) 
          UNION ALL 
            (
              With CTE_CST_STANDARD_COSTS_U2 as (
                SELECT 
                  standard_cost, 
                  inventory_item_id, 
                  organization_id 
                FROM 
                  (select * from bec_ods.cst_standard_costs where is_deleted_flg <> 'Y') c 
                WHERE 
                  standard_cost_revision_date <= GETDATE() 
                  AND cost_update_id = (
                    SELECT 
                      MAX(cost_update_id) 
                    FROM 
                      (select * from bec_ods.cst_standard_costs where is_deleted_flg <> 'Y') c1 
                    WHERE 
                      inventory_item_id = c.inventory_item_id 
                      AND organization_id = c.organization_id 
                      AND standard_cost_revision_date <= GETDATE()
                  )
              ), 
              CTE_CST_ELEMENTAL_COSTS_U2 as (
                SELECT 
                  SUM(e.standard_cost) as standard_cost, 
                  e.inventory_item_id, 
                  e.organization_id 
                FROM 
                  (select * from bec_ods.cst_elemental_costs where is_deleted_flg <> 'Y') e, 
                  (
                    select 
                      distinct inventory_item_id, 
                      organization_id 
                    from 
                      bec_dwh.FACT_CST_ONHAND_QTY_UNION2_STG
                  ) mmt 
                WHERE 
                  1 = 1 
                  AND e.inventory_item_id = mmt.inventory_item_id 
                  AND e.organization_id = mmt.organization_id 
                  AND e.creation_date = (
                    SELECT 
                      MAX(creation_date) 
                    FROM 
                      (select * from bec_ods.cst_elemental_costs where is_deleted_flg <> 'Y') e1 
                    WHERE 
                      e1.inventory_item_id = mmt.inventory_item_id 
                      AND e1.organization_id = mmt.organization_id 
                      AND e1.creation_date <= GETDATE()
                  ) 
                Group by 
                  e.inventory_item_id, 
                  e.organization_id
              ), 
              CTE_CST_MATERIAL_COSTS_U2 as (
                SELECT 
                  SUM(e.standard_cost) as standard_cost, 
                  e.inventory_item_id, 
                  e.organization_id 
                FROM 
                  (select * from bec_ods.cst_elemental_costs where is_deleted_flg <> 'Y') e, 
                  (
                    select 
                      distinct inventory_item_id, 
                      organization_id 
                    from 
                      bec_dwh.FACT_CST_ONHAND_QTY_UNION2_STG
                  ) mmt 
                WHERE 
                  1 = 1 
                  AND e.cost_element_id = 1 
                  AND e.inventory_item_id = mmt.inventory_item_id 
                  AND e.organization_id = mmt.organization_id 
                  AND e.creation_date = (
                    SELECT 
                      MAX(creation_date) 
                    FROM 
                      (select * from bec_ods.cst_elemental_costs where is_deleted_flg <> 'Y') e1 
                    WHERE 
                      e1.inventory_item_id = mmt.inventory_item_id 
                      AND e1.organization_id = mmt.organization_id 
                      AND e1.creation_date <= GETDATE()
                  ) 
                Group by 
                  e.inventory_item_id, 
                  e.organization_id
              ), 
              CTE_CST_MATERIAL_OVERHEAD_COSTS_U2 as (
                SELECT 
                  SUM(e.standard_cost) as standard_cost, 
                  e.inventory_item_id, 
                  e.organization_id 
                FROM 
                  (select * from bec_ods.cst_elemental_costs where is_deleted_flg <> 'Y') e, 
                  (
                    select 
                      distinct inventory_item_id, 
                      organization_id 
                    from 
                      bec_dwh.FACT_CST_ONHAND_QTY_UNION2_STG
                  ) mmt 
                WHERE 
                  1 = 1 
                  AND e.cost_element_id = 2 
                  AND e.inventory_item_id = mmt.inventory_item_id 
                  AND e.organization_id = mmt.organization_id 
                  AND e.creation_date = (
                    SELECT 
                      MAX(creation_date) 
                    FROM 
                      (select * from bec_ods.cst_elemental_costs where is_deleted_flg <> 'Y') e1 
                    WHERE 
                      e1.inventory_item_id = mmt.inventory_item_id 
                      AND e1.organization_id = mmt.organization_id 
                      AND e1.creation_date <= GETDATE()
                  ) 
                Group by 
                  e.inventory_item_id, 
                  e.organization_id
              ), 
              CTE_CST_RESOURCE_COSTS_U2 as (
                SELECT 
                  SUM(e.standard_cost) as standard_cost, 
                  e.inventory_item_id, 
                  e.organization_id 
                FROM 
                  (select * from bec_ods.cst_elemental_costs where is_deleted_flg <> 'Y') e, 
                  (
                    select 
                      distinct inventory_item_id, 
                      organization_id 
                    from 
                      bec_dwh.FACT_CST_ONHAND_QTY_UNION2_STG
                  ) mmt 
                WHERE 
                  1 = 1 
                  AND e.cost_element_id = 3 
                  AND e.inventory_item_id = mmt.inventory_item_id 
                  AND e.organization_id = mmt.organization_id 
                  AND e.creation_date = (
                    SELECT 
                      MAX(creation_date) 
                    FROM 
                      (select * from bec_ods.cst_elemental_costs where is_deleted_flg <> 'Y') e1 
                    WHERE 
                      e1.inventory_item_id = mmt.inventory_item_id 
                      AND e1.organization_id = mmt.organization_id 
                      AND e1.creation_date <= GETDATE()
                  ) 
                Group by 
                  e.inventory_item_id, 
                  e.organization_id
              ), 
              CTE_CST_OUTSIDE_PROCESSING_COSTS_U2 as (
                SELECT 
                  SUM(e.standard_cost) as standard_cost, 
                  e.inventory_item_id, 
                  e.organization_id 
                FROM 
                  (select * from bec_ods.cst_elemental_costs where is_deleted_flg <> 'Y') e, 
                  (
                    select 
                      distinct inventory_item_id, 
                      organization_id 
                    from 
                      bec_dwh.FACT_CST_ONHAND_QTY_UNION2_STG
                  ) mmt 
                WHERE 
                  1 = 1 
                  AND e.cost_element_id = 4 
                  AND e.inventory_item_id = mmt.inventory_item_id 
                  AND e.organization_id = mmt.organization_id 
                  AND e.creation_date = (
                    SELECT 
                      MAX(creation_date) 
                    FROM 
                      (select * from bec_ods.cst_elemental_costs where is_deleted_flg <> 'Y') e1 
                    WHERE 
                      e1.inventory_item_id = mmt.inventory_item_id 
                      AND e1.organization_id = mmt.organization_id 
                      AND e1.creation_date <= GETDATE()
                  ) 
                Group by 
                  e.inventory_item_id, 
                  e.organization_id
              ), 
              CTE_CST_OVERHEAD_COSTS_U2 as (
                SELECT 
                  SUM(e.standard_cost) as standard_cost, 
                  e.inventory_item_id, 
                  e.organization_id 
                FROM 
                  (select * from bec_ods.cst_elemental_costs where is_deleted_flg <> 'Y') e, 
                  (
                    select 
                      distinct inventory_item_id, 
                      organization_id 
                    from 
                      bec_dwh.FACT_CST_ONHAND_QTY_UNION2_STG
                  ) mmt 
                WHERE 
                  1 = 1 
                  AND e.cost_element_id = 5 
                  AND e.inventory_item_id = mmt.inventory_item_id 
                  AND e.organization_id = mmt.organization_id 
                  AND e.creation_date = (
                    SELECT 
                      MAX(creation_date) 
                    FROM 
                      (select * from bec_ods.cst_elemental_costs where is_deleted_flg <> 'Y') e1 
                    WHERE 
                      e1.inventory_item_id = mmt.inventory_item_id 
                      AND e1.organization_id = mmt.organization_id 
                      AND e1.creation_date <= GETDATE()
                  ) 
                Group by 
                  e.inventory_item_id, 
                  e.organization_id
              ) 
              SELECT 
                mmt.segment1 AS part_number, 
                mmt.cat_seg1, 
                mmt.cat_seg2, 
                mmt.category, 
                mmt.organization_name, 
                mmt.operating_unit operating_unit, 
                mmt.description, 
                mmt.primary_unit_of_measure unit_of_measure, 
                mmt.primary_uom_code uom_code, 
                mmt.inventory_item_status_code, 
                decode(
                  mmt.planning_make_buy_code, 1, 'Make', 
                  2, 'Buy'
                ) planning_make_buy_code, 
                mmt.mrp_planning_code, 
                1 quantity, 
                (
                  CASE WHEN ccsc.standard_cost = 0 THEN 0 ELSE ccmc.standard_cost END
                ) material_cost, 
                (
                  CASE WHEN ccsc.standard_cost = 0 THEN 0 ELSE ccmoc.standard_cost END
                ) material_overhead_cost, 
                (
                  CASE WHEN ccsc.standard_cost = 0 THEN 0 ELSE ccrc.standard_cost END
                ) resource_cost, 
                (
                  CASE WHEN ccsc.standard_cost = 0 THEN 0 ELSE ccopc.standard_cost END
                ) outside_processing_cost, 
                (
                  CASE WHEN ccsc.standard_cost = 0 THEN 0 ELSE ccoc.standard_cost END
                ) overhead_cost, 
                (
                  CASE WHEN ccsc.standard_cost = 0 THEN 0 ELSE ccec.standard_cost END
                ) item_cost, 
                mmt.transaction_date date_received, 
                trunc(
                  DATEDIFF(
                    'DAYS', 
                    mmt.initialization_date, 
                    GETDATE()
                  )
                )-1 source_date_aging, 
                mmt.organization_id, 
                mmt.transaction_source_type_id, 
                mmt.transaction_type_id, 
                mmt.lot_number, 
                mmt.serial_number serial_number, 
                mmt.locator, 
                mmt.current_subinventory_code AS subinventory, 
                mmt.material_account, 
                decode(
                  mmt.asset_inventory :: VARCHAR, 1, 
                  'Asset', 2, 'Expense', asset_inventory :: VARCHAR
                ) subinventory_type, 
                mmt.attribute10 subinventory_category, 
                mmt.attribute11 subinventory_subcategory, 
                mmt.vmi_flag 
              FROM 
                bec_dwh.fact_cst_onhand_qty_union2_stg mmt, 
                CTE_CST_STANDARD_COSTS_U2 ccsc, 
                CTE_CST_ELEMENTAL_COSTS_U2 ccec, 
                CTE_CST_MATERIAL_COSTS_U2 ccmc, 
                CTE_CST_MATERIAL_OVERHEAD_COSTS_U2 ccmoc, 
                CTE_CST_RESOURCE_COSTS_U2 ccrc, 
                CTE_CST_OUTSIDE_PROCESSING_COSTS_U2 ccopc, 
                CTE_CST_OVERHEAD_COSTS_U2 ccoc 
              WHERE 
                ccsc.inventory_item_id(+) = mmt.inventory_item_id 
                AND ccsc.organization_id(+) = mmt.organization_id 
                AND ccec.inventory_item_id(+) = mmt.inventory_item_id 
                AND ccec.organization_id(+) = mmt.organization_id 
                AND ccmc.inventory_item_id(+) = mmt.inventory_item_id 
                AND ccmc.organization_id(+) = mmt.organization_id 
                AND ccmoc.inventory_item_id(+) = mmt.inventory_item_id 
                AND ccmoc.organization_id(+) = mmt.organization_id 
                AND ccrc.inventory_item_id(+) = mmt.inventory_item_id 
                AND ccrc.organization_id(+) = mmt.organization_id 
                AND ccopc.inventory_item_id(+) = mmt.inventory_item_id 
                AND ccopc.organization_id(+) = mmt.organization_id 
                AND ccoc.inventory_item_id(+) = mmt.inventory_item_id 
                AND ccoc.organization_id(+) = mmt.organization_id
            ) 
          UNION ALL 
            (
              With CTE_CST_STANDARD_COSTS_U3 as (
                SELECT 
                  standard_cost, 
                  inventory_item_id, 
                  organization_id 
                FROM 
                  (select * from bec_ods.cst_standard_costs where is_deleted_flg <> 'Y') c 
                WHERE 
                  standard_cost_revision_date <= GETDATE() 
                  AND cost_update_id = (
                    SELECT 
                      MAX(cost_update_id) 
                    FROM 
                      (select * from bec_ods.cst_standard_costs where is_deleted_flg <> 'Y') c1 
                    WHERE 
                      inventory_item_id = c.inventory_item_id 
                      AND organization_id = c.organization_id 
                      AND standard_cost_revision_date <= GETDATE()
                  )
              ), 
              CTE_CST_ELEMENTAL_COSTS_U3 as (
                SELECT 
                  SUM(e.standard_cost) as standard_cost, 
                  e.inventory_item_id, 
                  e.organization_id 
                FROM 
                  (select * from bec_ods.cst_elemental_costs where is_deleted_flg <> 'Y') e, 
                  (
                    select 
                      distinct inventory_item_id, 
                      organization_id 
                    from 
                      bec_dwh.FACT_CST_ONHAND_QTY_UNION3_STG
                  ) mmt 
                WHERE 
                  1 = 1 
                  AND e.inventory_item_id = mmt.inventory_item_id 
                  AND e.organization_id = mmt.organization_id 
                  AND e.creation_date = (
                    SELECT 
                      MAX(creation_date) 
                    FROM 
                      (select * from bec_ods.cst_elemental_costs where is_deleted_flg <> 'Y') e1 
                    WHERE 
                      e1.inventory_item_id = mmt.inventory_item_id 
                      AND e1.organization_id = mmt.organization_id 
                      AND e1.creation_date <= GETDATE()
                  ) 
                Group by 
                  e.inventory_item_id, 
                  e.organization_id
              ), 
              CTE_CST_MATERIAL_COSTS_U3 as (
                SELECT 
                  SUM(e.standard_cost) as standard_cost, 
                  e.inventory_item_id, 
                  e.organization_id 
                FROM 
                  (select * from bec_ods.cst_elemental_costs where is_deleted_flg <> 'Y') e, 
                  (
                    select 
                      distinct inventory_item_id, 
                      organization_id 
                    from 
                      bec_dwh.FACT_CST_ONHAND_QTY_UNION3_STG
                  ) mmt 
                WHERE 
                  1 = 1 
                  AND e.cost_element_id = 1 
                  AND e.inventory_item_id = mmt.inventory_item_id 
                  AND e.organization_id = mmt.organization_id 
                  AND e.creation_date = (
                    SELECT 
                      MAX(creation_date) 
                    FROM 
                      (select * from bec_ods.cst_elemental_costs where is_deleted_flg <> 'Y') e1 
                    WHERE 
                      e1.inventory_item_id = mmt.inventory_item_id 
                      AND e1.organization_id = mmt.organization_id 
                      AND e1.creation_date <= GETDATE()
                  ) 
                Group by 
                  e.inventory_item_id, 
                  e.organization_id
              ), 
              CTE_CST_MATERIAL_OVERHEAD_COSTS_U3 as (
                SELECT 
                  SUM(e.standard_cost) as standard_cost, 
                  e.inventory_item_id, 
                  e.organization_id 
                FROM 
                  (select * from bec_ods.cst_elemental_costs where is_deleted_flg <> 'Y') e, 
                  (
                    select 
                      distinct inventory_item_id, 
                      organization_id 
                    from 
                      bec_dwh.FACT_CST_ONHAND_QTY_UNION3_STG
                  ) mmt 
                WHERE 
                  1 = 1 
                  AND e.cost_element_id = 2 
                  AND e.inventory_item_id = mmt.inventory_item_id 
                  AND e.organization_id = mmt.organization_id 
                  AND e.creation_date = (
                    SELECT 
                      MAX(creation_date) 
                    FROM 
                      (select * from bec_ods.cst_elemental_costs where is_deleted_flg <> 'Y') e1 
                    WHERE 
                      e1.inventory_item_id = mmt.inventory_item_id 
                      AND e1.organization_id = mmt.organization_id 
                      AND e1.creation_date <= GETDATE()
                  ) 
                Group by 
                  e.inventory_item_id, 
                  e.organization_id
              ), 
              CTE_CST_RESOURCE_COSTS_U3 as (
                SELECT 
                  SUM(e.standard_cost) as standard_cost, 
                  e.inventory_item_id, 
                  e.organization_id 
                FROM 
                  (select * from bec_ods.cst_elemental_costs where is_deleted_flg <> 'Y') e, 
                  (
                    select 
                      distinct inventory_item_id, 
                      organization_id 
                    from 
                      bec_dwh.FACT_CST_ONHAND_QTY_UNION3_STG
                  ) mmt 
                WHERE 
                  1 = 1 
                  AND e.cost_element_id = 3 
                  AND e.inventory_item_id = mmt.inventory_item_id 
                  AND e.organization_id = mmt.organization_id 
                  AND e.creation_date = (
                    SELECT 
                      MAX(creation_date) 
                    FROM 
                      (select * from bec_ods.cst_elemental_costs where is_deleted_flg <> 'Y') e1 
                    WHERE 
                      e1.inventory_item_id = mmt.inventory_item_id 
                      AND e1.organization_id = mmt.organization_id 
                      AND e1.creation_date <= GETDATE()
                  ) 
                Group by 
                  e.inventory_item_id, 
                  e.organization_id
              ), 
              CTE_CST_OUTSIDE_PROCESSING_COSTS_U3 as (
                SELECT 
                  SUM(e.standard_cost) as standard_cost, 
                  e.inventory_item_id, 
                  e.organization_id 
                FROM 
                  (select * from bec_ods.cst_elemental_costs where is_deleted_flg <> 'Y') e, 
                  (
                    select 
                      distinct inventory_item_id, 
                      organization_id 
                    from 
                      bec_dwh.FACT_CST_ONHAND_QTY_UNION3_STG
                  ) mmt 
                WHERE 
                  1 = 1 
                  AND e.cost_element_id = 4 
                  AND e.inventory_item_id = mmt.inventory_item_id 
                  AND e.organization_id = mmt.organization_id 
                  AND e.creation_date = (
                    SELECT 
                      MAX(creation_date) 
                    FROM 
                      (select * from bec_ods.cst_elemental_costs where is_deleted_flg <> 'Y') e1 
                    WHERE 
                      e1.inventory_item_id = mmt.inventory_item_id 
                      AND e1.organization_id = mmt.organization_id 
                      AND e1.creation_date <= GETDATE()
                  ) 
                Group by 
                  e.inventory_item_id, 
                  e.organization_id
              ), 
              CTE_CST_OVERHEAD_COSTS_U3 as (
                SELECT 
                  SUM(e.standard_cost) as standard_cost, 
                  e.inventory_item_id, 
                  e.organization_id 
                FROM 
                  (select * from bec_ods.cst_elemental_costs where is_deleted_flg <> 'Y') e, 
                  (
                    select 
                      distinct inventory_item_id, 
                      organization_id 
                    from 
                      bec_dwh.FACT_CST_ONHAND_QTY_UNION3_STG
                  ) mmt 
                WHERE 
                  1 = 1 
                  AND e.cost_element_id = 5 
                  AND e.inventory_item_id = mmt.inventory_item_id 
                  AND e.organization_id = mmt.organization_id 
                  AND e.creation_date = (
                    SELECT 
                      MAX(creation_date) 
                    FROM 
                      (select * from bec_ods.cst_elemental_costs where is_deleted_flg <> 'Y') e1 
                    WHERE 
                      e1.inventory_item_id = mmt.inventory_item_id 
                      AND e1.organization_id = mmt.organization_id 
                      AND e1.creation_date <= GETDATE()
                  ) 
                Group by 
                  e.inventory_item_id, 
                  e.organization_id
              ) 
              SELECT 
                mmt.segment1 AS part_number, 
                mmt.cat_seg1, 
                mmt.cat_seg2, 
                mmt.category, 
                mmt.organization_name, 
                mmt.operating_unit operating_unit, 
                mmt.description, 
                mmt.primary_unit_of_measure unit_of_measure, 
                mmt.primary_uom_code uom_code, 
                mmt.inventory_item_status_code, 
                decode(
                  mmt.planning_make_buy_code, 1, 'Make', 
                  2, 'Buy'
                ) planning_make_buy_code, 
                mmt.mrp_planning_code, 
                1 quantity, 
                (
                  CASE WHEN ccsc.standard_cost = 0 THEN 0 ELSE ccmc.standard_cost END
                ) material_cost, 
                (
                  CASE WHEN ccsc.standard_cost = 0 THEN 0 ELSE ccmoc.standard_cost END
                ) material_overhead_cost, 
                (
                  CASE WHEN ccsc.standard_cost = 0 THEN 0 ELSE ccrc.standard_cost END
                ) resource_cost, 
                (
                  CASE WHEN ccsc.standard_cost = 0 THEN 0 ELSE ccopc.standard_cost END
                ) outside_processing_cost, 
                (
                  CASE WHEN ccsc.standard_cost = 0 THEN 0 ELSE ccoc.standard_cost END
                ) overhead_cost, 
                (
                  CASE WHEN ccsc.standard_cost = 0 THEN 0 ELSE ccec.standard_cost END
                ) item_cost, 
                mmt.transaction_date date_received, 
                trunc(
                  DATEDIFF(
                    'DAYS', 
                    mmt.initialization_date, 
                    GETDATE()
                  )
                )-1 source_date_aging, 
                mmt.organization_id, 
                mmt.transaction_source_type_id, 
                mmt.transaction_type_id, 
                mmt.lot_number, 
                mmt.serial_number serial_number, 
                mmt.locator, 
                mmt.current_subinventory_code AS subinventory, 
                mmt.material_account, 
                decode(
                  mmt.asset_inventory :: VARCHAR, 1, 
                  'Asset', 2, 'Expense', asset_inventory :: VARCHAR
                ) subinventory_type, 
                mmt.attribute10 subinventory_category, 
                mmt.attribute11 subinventory_subcategory, 
                mmt.vmi_flag 
              FROM 
                bec_dwh.FACT_CST_ONHAND_QTY_UNION3_STG mmt, 
                CTE_CST_STANDARD_COSTS_U3 ccsc, 
                CTE_CST_ELEMENTAL_COSTS_U3 ccec, 
                CTE_CST_MATERIAL_COSTS_U3 ccmc, 
                CTE_CST_MATERIAL_OVERHEAD_COSTS_U3 ccmoc, 
                CTE_CST_RESOURCE_COSTS_U3 ccrc, 
                CTE_CST_OUTSIDE_PROCESSING_COSTS_U3 ccopc, 
                CTE_CST_OVERHEAD_COSTS_U3 ccoc 
              WHERE 
                ccsc.inventory_item_id(+) = mmt.inventory_item_id 
                AND ccsc.organization_id(+) = mmt.organization_id 
                AND ccec.inventory_item_id(+) = mmt.inventory_item_id 
                AND ccec.organization_id(+) = mmt.organization_id 
                AND ccmc.inventory_item_id(+) = mmt.inventory_item_id 
                AND ccmc.organization_id(+) = mmt.organization_id 
                AND ccmoc.inventory_item_id(+) = mmt.inventory_item_id 
                AND ccmoc.organization_id(+) = mmt.organization_id 
                AND ccrc.inventory_item_id(+) = mmt.inventory_item_id 
                AND ccrc.organization_id(+) = mmt.organization_id 
                AND ccopc.inventory_item_id(+) = mmt.inventory_item_id 
                AND ccopc.organization_id(+) = mmt.organization_id 
                AND ccoc.inventory_item_id(+) = mmt.inventory_item_id 
                AND ccoc.organization_id(+) = mmt.organization_id
            )
        )
    )
);

commit;

end;

update
	bec_etl_ctrl.batch_dw_info
set
	load_type = 'I',
	last_refresh_date = getdate()
where
	dw_table_name = 'fact_cst_onhand_qty'
	and batch_name = 'costing';

commit;
