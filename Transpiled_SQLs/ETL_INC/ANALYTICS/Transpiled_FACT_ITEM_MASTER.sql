TRUNCATE table gold_bec_dwh.fact_item_master_tmp;
/* Insert records into temp table */
INSERT INTO gold_bec_dwh.fact_item_master_tmp
(
  SELECT DISTINCT
    inventory_item_id,
    organization_id
  FROM silver_bec_ods.mtl_system_items_b
  WHERE
    1 = 1
    AND kca_seq_date > (
      SELECT
        (
          executebegints - prune_days
        )
      FROM bec_etl_ctrl.batch_dw_info
      WHERE
        dw_table_name = 'fact_item_master' AND batch_name = 'inv'
    )
);
/* delete records from fact table */
DELETE FROM gold_bec_dwh.FACT_ITEM_MASTER
WHERE
  EXISTS(
    SELECT
      1
    FROM gold_bec_dwh.fact_item_master_tmp AS tmp
    WHERE
      tmp.inventory_item_id = fact_item_master.inventory_item_id
      AND tmp.organization_id = fact_item_master.organization_id
  );
/* Insert records into fact table */
INSERT INTO gold_bec_dwh.fact_item_master
(
  SELECT DISTINCT
    mtl.inventory_item_id,
    (
      SELECT
        system_id
      FROM bec_etl_ctrl.etlsourceappid
      WHERE
        source_system = 'EBS'
    ) || '-' || mtl.inventory_item_id AS INVENTORY_ITEM_ID_KEY,
    mtl.organization_id,
    (
      SELECT
        system_id
      FROM bec_etl_ctrl.etlsourceappid
      WHERE
        source_system = 'EBS'
    ) || '-' || mtl.organization_id AS ORGANIZATION_ID_KEY,
    mtl.segment1 AS part_number,
    mtl.description,
    mtlp.organization_code,
    rcvh.routing_name,
    mtl.inventory_item_status_code,
    bomr.revision_id,
    bomr.current_revision,
    mtl.item_type,
    mtl.planner_code,
    mtl.buyer_id,
    mtl.primary_uom_code,
    (
      SELECT
        mfl.meaning
      FROM (
        SELECT
          *
        FROM silver_bec_ods.fnd_lookup_values
        WHERE
          is_deleted_flg <> 'Y'
      ) AS mfl
      WHERE
        mfl.lookup_type = 'MRP_PLANNING_CODE' AND mtl.mrp_planning_code = mfl.lookup_code
    ) AS mrp_planning,
    mtl.max_minmax_quantity,
    mtl.min_minmax_quantity,
    (
      SELECT
        mfl.meaning
      FROM (
        SELECT
          *
        FROM silver_bec_ods.fnd_lookup_values
        WHERE
          is_deleted_flg <> 'Y'
      ) AS mfl
      WHERE
        mfl.lookup_type = 'WIP_SUPPLY' AND mtl.wip_supply_type = mfl.lookup_code
    ) AS wip_supply_type,
    (
      SELECT
        mfl.meaning
      FROM (
        SELECT
          *
        FROM silver_bec_ods.fnd_lookup_values
        WHERE
          is_deleted_flg <> 'Y'
      ) AS mfl
      WHERE
        mfl.lookup_type = 'MTL_PLANNING_MAKE_BUY'
        AND mfl.lookup_code = mtl.planning_make_buy_code
    ) AS planning_make_buy_code,
    mtl.preprocessing_lead_time,
    mtl.full_lead_time,
    mtl.postprocessing_lead_time,
    mtl.fixed_lead_time,
    mtl.variable_lead_time,
    mtl.cum_manufacturing_lead_time,
    mtl.cumulative_total_lead_time,
    mtl.lead_time_lot_size,
    mtl.shrinkage_rate,
    mtl.fixed_days_supply,
    mtl.fixed_order_quantity,
    mtl.fixed_lot_multiplier,
    ood.organization_name,
    mc.segment3 || '.' || mc.segment4 || '.' || mc.segment1 || '.' || mc.segment2 || '.' || mc.segment5 || '.' || mc.segment6 AS CATEGORY,
    CASE WHEN mtl.lot_control_code = 1 THEN 'No Control' ELSE 'Full Control' END AS lot_control,
    CASE
      WHEN mtl.serial_number_control_code = 2
      THEN 'Predefined'
      WHEN mtl.serial_number_control_code = 1
      THEN 'No Control'
      WHEN mtl.serial_number_control_code = 5
      THEN 'At Receipt'
      ELSE NULL
    END AS serial_control,
    mtl.minimum_order_quantity,
    mtl.maximum_order_quantity,
    CASE
      WHEN mtl.planning_time_fence_code = 4
      THEN 'User Defined'
      WHEN mtl.planning_time_fence_code = 3
      THEN 'Total Lead_time'
    END AS planning_time_fence_code,
    mtl.planning_time_fence_days,
    CASE WHEN mtl.release_time_fence_code = 4 THEN 'User Defined' ELSE NULL END AS release_time_fence_code,
    mtl.release_time_fence_days,
    COALESCE(mtl.comms_nl_trackable_flag, 'N') AS ib_trackable_flag,
    mtl.creation_date,
    mcat.segment1 AS FPA_Categ_Seg1,
    mcat.segment2 AS FPA_Categ_Seg2,
    mir.revision, /* Added for QuickSight */
    mtl.attribute5 AS program_name, /* New enhancement */
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
    ) || '-' || COALESCE(mtl.inventory_item_id, 0) || '-' || COALESCE(mtl.organization_id, 0) || '-' || COALESCE(bomr.revision_id, 0) AS dw_load_id,
    CURRENT_TIMESTAMP() AS dw_insert_date,
    CURRENT_TIMESTAMP() AS dw_update_date
  FROM (
    SELECT
      *
    FROM silver_bec_ods.mtl_system_items_b
    WHERE
      is_deleted_flg <> 'Y'
  ) AS mtl, (
    SELECT
      *
    FROM silver_bec_ods.mtl_parameters
    WHERE
      is_deleted_flg <> 'Y'
  ) AS mtlp, (
    SELECT
      *
    FROM silver_bec_ods.rcv_routing_headers
    WHERE
      is_deleted_flg <> 'Y'
  ) AS rcvh, (
    SELECT
      *
    FROM silver_bec_ods.BOM_ITEM_CURRENT_REV_VIEW
    WHERE
      is_deleted_flg <> 'Y'
  ) AS bomr, (
    SELECT
      *
    FROM silver_bec_ods.org_organization_definitions
    WHERE
      is_deleted_flg <> 'Y'
  ) AS ood, (
    SELECT
      *
    FROM silver_bec_ods.mtl_categories_b
    WHERE
      is_deleted_flg <> 'Y'
  ) AS mc, (
    SELECT
      *
    FROM silver_bec_ods.mtl_item_categories
    WHERE
      is_deleted_flg <> 'Y'
  ) AS mic, (
    SELECT
      *
    FROM silver_bec_ods.mtl_item_revisions_b
    WHERE
      is_deleted_flg <> 'Y'
  ) AS mir, (
    SELECT
      mc.segment1,
      mc.segment2,
      mic.inventory_item_id,
      mic.organization_id
    FROM (
      SELECT
        *
      FROM silver_bec_ods.mtl_categories_b
      WHERE
        is_deleted_flg <> 'Y'
    ) AS mc, (
      SELECT
        *
      FROM silver_bec_ods.mtl_item_categories
      WHERE
        is_deleted_flg <> 'Y'
    ) AS mic
    WHERE
      mic.category_id = mc.CATEGORY_ID() AND mic.category_set_id = 1100000081
  ) AS mcat
  WHERE
    mtl.organization_id = mtlp.ORGANIZATION_ID()
    AND mtl.receiving_routing_id = rcvh.ROUTING_HEADER_ID()
    AND mtl.inventory_item_id = bomr.INVENTORY_ITEM_ID()
    AND mtl.organization_id = bomr.organization_id
    AND mtl.organization_id = ood.organization_id
    AND mtl.inventory_item_id = mic.INVENTORY_ITEM_ID()
    AND mtl.organization_id = mic.ORGANIZATION_ID()
    AND mic.category_id = mc.CATEGORY_ID()
    AND mic.category_set_id = 1
    AND mtl.inventory_item_id = mir.inventory_item_id
    AND mtl.organization_id = mir.organization_id
    AND bomr.revision_id = mir.REVISION_ID()
    AND mtl.inventory_item_id = mcat.INVENTORY_ITEM_ID()
    AND mtl.organization_id = mcat.ORGANIZATION_ID()
  ORDER BY
    mtl.segment1 ASC NULLS LAST
);
UPDATE bec_etl_ctrl.batch_dw_info SET load_type = 'I', last_refresh_date = CURRENT_TIMESTAMP()
WHERE
  dw_table_name = 'fact_item_master' AND batch_name = 'inv';