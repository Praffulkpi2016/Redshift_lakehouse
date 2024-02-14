TRUNCATE table gold_bec_dwh.fact_open_wo_summary_tmp;
/* Insert records into temp table */
INSERT INTO gold_bec_dwh.fact_open_wo_summary_tmp
(
  SELECT DISTINCT
    b.WIP_ENTITY_ID
  FROM (
    SELECT
      *
    FROM silver_bec_ods.wip_operations
    WHERE
      is_deleted_flg <> 'Y'
  ) AS a, (
    SELECT
      *
    FROM silver_bec_ods.wip_entities
    WHERE
      is_deleted_flg <> 'Y'
  ) AS b, (
    SELECT
      wip_entity_id,
      primary_item_id,
      organization_id,
      net_quantity,
      quantity_completed,
      kca_seq_date
    FROM silver_bec_ods.wip_discrete_jobs
    WHERE
      is_deleted_flg <> 'Y' AND status_type = 3
  ) AS c, (
    SELECT
      *
    FROM gold_bec_dwh.dim_inv_item_category_set
    WHERE
      item_category_segment1 = 'Stack'
  ) AS d
  WHERE
    a.wip_entity_id = b.wip_entity_id
    AND b.wip_entity_id = c.wip_entity_id
    AND c.primary_item_id = d.inventory_item_id
    AND c.organization_id = d.organization_id
    AND (
      a.kca_seq_date > (
        SELECT
          (
            executebegints - prune_days
          )
        FROM bec_etl_ctrl.batch_dw_info
        WHERE
          dw_table_name = 'fact_open_wo_summary' AND batch_name = 'wip'
      )
      OR b.kca_seq_date > (
        SELECT
          (
            executebegints - prune_days
          )
        FROM bec_etl_ctrl.batch_dw_info
        WHERE
          dw_table_name = 'fact_open_wo_summary' AND batch_name = 'wip'
      )
      OR c.kca_seq_date > (
        SELECT
          (
            executebegints - prune_days
          )
        FROM bec_etl_ctrl.batch_dw_info
        WHERE
          dw_table_name = 'fact_open_wo_summary' AND batch_name = 'wip'
      )
    )
);
/* delete records from fact table */
DELETE FROM gold_bec_dwh.fact_open_wo_summary
WHERE
  EXISTS(
    SELECT
      1
    FROM gold_bec_dwh.fact_open_wo_summary_tmp AS tmp
    WHERE
      COALESCE(tmp.WIP_ENTITY_ID, 0) = COALESCE(fact_open_wo_summary.WIP_ENTITY_ID, 0)
  );
/* Insert records into fact table */
INSERT INTO gold_bec_dwh.fact_open_wo_summary
(
  SELECT
    wip_entity_id,
    primary_item_id,
    organization_id,
    work_order,
    assembly_quantity,
    assembly_completed_qty,
    operation_seq_num,
    quantity_in_queue,
    quantity_running,
    quantity_scrapped,
    quantity_waiting_to_move,
    quantity_rejected,
    quantity_completed, /* audit COLUMNS */
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
    ) || '-' || COALESCE(wip_entity_id, 0) || '-' || COALESCE(operation_seq_num, 0) AS dw_load_id,
    CURRENT_TIMESTAMP() AS dw_insert_date,
    CURRENT_TIMESTAMP() AS dw_update_date
  FROM (
    SELECT
      b.WIP_ENTITY_ID,
      c.primary_item_id,
      c.organization_id,
      b.wip_entity_name AS work_order,
      c.net_quantity AS assembly_quantity,
      c.quantity_completed AS assembly_completed_qty,
      a.operation_seq_num,
      SUM(a.quantity_in_queue) AS quantity_in_queue,
      SUM(a.quantity_running) AS quantity_running,
      SUM(a.quantity_scrapped) AS quantity_scrapped,
      SUM(a.quantity_waiting_to_move) AS quantity_waiting_to_move,
      SUM(a.quantity_rejected) AS quantity_rejected,
      SUM(a.quantity_completed) AS quantity_completed
    FROM (
      SELECT
        *
      FROM silver_bec_ods.wip_operations
      WHERE
        is_deleted_flg <> 'Y'
    ) AS a, (
      SELECT
        *
      FROM silver_bec_ods.wip_entities
      WHERE
        is_deleted_flg <> 'Y'
    ) AS b, (
      SELECT
        wip_entity_id,
        primary_item_id,
        organization_id,
        net_quantity,
        quantity_completed
      FROM silver_bec_ods.wip_discrete_jobs
      WHERE
        is_deleted_flg <> 'Y' AND status_type = 3
    ) AS c, (
      SELECT
        *
      FROM gold_bec_dwh.dim_inv_item_category_set
      WHERE
        item_category_segment1 = 'Stack'
    ) AS d, gold_bec_dwh.fact_open_wo_summary_tmp AS tmp
    WHERE
      a.wip_entity_id = b.wip_entity_id
      AND b.wip_entity_id = c.wip_entity_id
      AND c.primary_item_id = d.inventory_item_id
      AND c.organization_id = d.organization_id
      AND COALESCE(tmp.WIP_ENTITY_ID, 0) = COALESCE(b.WIP_ENTITY_ID, 0)
    GROUP BY
      a.operation_seq_num,
      b.WIP_ENTITY_ID,
      c.primary_item_id,
      c.organization_id,
      b.wip_entity_name,
      c.net_quantity,
      c.quantity_completed
  )
);
UPDATE bec_etl_ctrl.batch_dw_info SET load_type = 'I', last_refresh_date = CURRENT_TIMESTAMP()
WHERE
  dw_table_name = 'fact_open_wo_summary' AND batch_name = 'wip';