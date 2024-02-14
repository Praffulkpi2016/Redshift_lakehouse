TRUNCATE table gold_bec_dwh_rpt.FACT_INV_ITEM_COST_RT_TMP;
/* Insert records into temp table */
INSERT INTO gold_bec_dwh_rpt.FACT_INV_ITEM_COST_RT_TMP
(
  SELECT DISTINCT
    cic.inventory_item_id,
    cic.organization_id
  FROM silver_bec_ods.cst_item_costs AS cic
  /* gold_bec_dwh.fact_item_master msi */
  WHERE
    1 = 1
    AND (
      cic.kca_seq_date > (
        SELECT
          (
            executebegints - prune_days
          )
        FROM bec_etl_ctrl.batch_dw_info
        WHERE
          dw_table_name = 'fact_inv_item_cost_rt' AND batch_name = 'inv'
      )
    )
);
DELETE FROM gold_bec_dwh_rpt.FACT_INV_ITEM_COST_RT
WHERE
  EXISTS(
    SELECT
      1
    FROM gold_bec_dwh_rpt.FACT_INV_ITEM_COST_RT_TMP AS tmp
    WHERE
      1 = 1
      AND tmp.inventory_item_id = FACT_INV_ITEM_COST_RT.inventory_item_id
      AND tmp.organization_id = FACT_INV_ITEM_COST_RT.organization_id
  );
INSERT INTO gold_bec_dwh_rpt.FACT_INV_ITEM_COST_RT
(
  SELECT
    cic.organization_id,
    cic.inventory_item_id,
    msi.part_number,
    msi.description,
    msi.primary_uom_code,
    msi.inventory_item_status_code AS status,
    msi.planning_make_buy_code AS TYPE1,
    cct.cost_type,
    msi.planner_code,
    msi.organization_code,
    msi.organization_name,
    CASE
      WHEN cic.based_on_rollup_flag = 1
      THEN 'Yes'
      WHEN cic.based_on_rollup_flag = 2
      THEN 'No'
      WHEN cic.based_on_rollup_flag = 3
      THEN 'Default'
      ELSE CAST(cic.based_on_rollup_flag AS CHAR)
    END AS based_on_rollup_flag,
    cic.item_cost AS extended_cost,
    COALESCE(cic.material_cost, 0) AS tot_material_cost,
    cic.material_overhead_cost AS total_material_overhead,
    cic.resource_cost,
    cic.overhead_cost,
    cic.outside_processing_cost,
    mic.item_category_segment1,
    mic.item_category_segment2,
    mic.item_category_segment3,
    mic.item_category_segment4,
    COALESCE(
      (
        SELECT
          SUM(item_cost)
        FROM gold_bec_dwh.DIM_ITEM_COST_DETAILS AS ccd
        WHERE
          ccd.inventory_item_id = cic.inventory_item_id
          AND ccd.organization_id = cic.organization_id
          AND ccd.cost_element_id = 1
          AND ccd.cost_type_id = cic.cost_type_id
          AND ccd.resource_code = 'FinanceAdj'
      ),
      0
    ) AS mat_financeadj,
    COALESCE(
      (
        SELECT
          SUM(item_cost)
        FROM gold_bec_dwh.DIM_ITEM_COST_DETAILS AS ccd
        WHERE
          ccd.inventory_item_id = cic.inventory_item_id
          AND ccd.organization_id = cic.organization_id
          AND ccd.cost_element_id = 1
          AND ccd.cost_type_id = cic.cost_type_id
          AND ccd.resource_code = 'Mat OSP'
      ),
      0
    ) AS mat_osp,
    COALESCE(
      (
        SELECT
          SUM(item_cost)
        FROM gold_bec_dwh.DIM_ITEM_COST_DETAILS AS ccd
        WHERE
          ccd.inventory_item_id = cic.inventory_item_id
          AND ccd.organization_id = cic.organization_id
          AND ccd.cost_element_id = 1
          AND ccd.cost_type_id = cic.cost_type_id
          AND ccd.resource_code = 'Material'
      ),
      0
    ) AS mat_material,
    COALESCE(
      (
        SELECT
          SUM(item_cost)
        FROM gold_bec_dwh.DIM_ITEM_COST_DETAILS AS ccd
        WHERE
          ccd.inventory_item_id = cic.inventory_item_id
          AND ccd.organization_id = cic.organization_id
          AND ccd.cost_element_id = 2
          AND ccd.cost_type_id = cic.cost_type_id
          AND ccd.resource_code = 'MOH'
      ),
      0
    ) AS MOH,
    COALESCE(
      (
        SELECT
          SUM(item_cost)
        FROM gold_bec_dwh.DIM_ITEM_COST_DETAILS AS ccd
        WHERE
          ccd.inventory_item_id = cic.inventory_item_id
          AND ccd.organization_id = cic.organization_id
          AND ccd.cost_element_id = 2
          AND ccd.cost_type_id = cic.cost_type_id
          AND ccd.resource_code = 'FOH'
      ),
      0
    ) AS FOH,
    COALESCE(
      (
        SELECT
          SUM(item_cost)
        FROM gold_bec_dwh.DIM_ITEM_COST_DETAILS AS ccd
        WHERE
          ccd.inventory_item_id = cic.inventory_item_id
          AND ccd.organization_id = cic.organization_id
          AND ccd.cost_element_id = 2
          AND ccd.cost_type_id = cic.cost_type_id
          AND ccd.resource_code = 'Fab L'
      ),
      0
    ) AS Fab,
    COALESCE(
      (
        SELECT
          SUM(item_cost)
        FROM gold_bec_dwh.DIM_ITEM_COST_DETAILS AS ccd
        WHERE
          ccd.inventory_item_id = cic.inventory_item_id
          AND ccd.organization_id = cic.organization_id
          AND ccd.cost_element_id = 2
          AND ccd.cost_type_id = cic.cost_type_id
          AND ccd.resource_code = 'Assy-L'
      ),
      0
    ) AS aoh,
    COALESCE(
      (
        SELECT
          SUM(item_cost)
        FROM gold_bec_dwh.DIM_ITEM_COST_DETAILS AS ccd
        WHERE
          ccd.inventory_item_id = cic.inventory_item_id
          AND ccd.organization_id = cic.organization_id
          AND ccd.cost_element_id = 2
          AND ccd.cost_type_id = cic.cost_type_id
          AND ccd.resource_code = 'Stk Yield'
      ),
      0
    ) AS StkYield,
    COALESCE(
      (
        SELECT
          SUM(item_cost)
        FROM gold_bec_dwh.DIM_ITEM_COST_DETAILS AS ccd
        WHERE
          ccd.inventory_item_id = cic.inventory_item_id
          AND ccd.organization_id = cic.organization_id
          AND ccd.cost_element_id = 2
          AND ccd.cost_type_id = cic.cost_type_id
          AND ccd.resource_code = 'IFM Yield'
      ),
      0
    ) AS IFMYield,
    msi.program_name /* new enhancement */
  FROM bec_Dwh.fact_item_master AS msi, (
    SELECT
      *
    FROM silver_bec_ods.cst_item_costs
    WHERE
      is_deleted_flg <> 'Y'
  ) AS cic, (
    SELECT
      *
    FROM silver_bec_ods.cst_cost_types
    WHERE
      is_deleted_flg <> 'Y'
  ) AS cct, (
    SELECT
      *
    FROM gold_bec_dwh.dim_inv_item_category_set
    WHERE
      category_set_id = 1 AND is_deleted_flg <> 'Y'
  ) AS mic, gold_bec_dwh_rpt.FACT_INV_ITEM_COST_RT_TMP AS TMP
  WHERE
    1 = 1
    AND TMP.inventory_item_id = cic.inventory_item_id
    AND TMP.organization_id = cic.organization_id
    AND msi.inventory_item_id = cic.inventory_item_id
    AND msi.organization_id = cic.organization_id
    AND cic.cost_type_id = cct.cost_type_id
    AND cic.ORGANIZATION_ID = mic.ORGANIZATION_ID()
    AND cic.INVENTORY_ITEM_ID = mic.INVENTORY_ITEM_ID()
);
UPDATE bec_etl_ctrl.batch_dw_info SET load_type = 'I', last_refresh_date = CURRENT_TIMESTAMP()
WHERE
  dw_table_name = 'fact_inv_item_cost_rt' AND batch_name = 'inv';