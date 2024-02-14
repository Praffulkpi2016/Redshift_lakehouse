DROP table IF EXISTS gold_bec_dwh.FACT_WIP_PERIOD_VALUE;
CREATE TABLE gold_bec_dwh.fact_wip_period_value AS
(
  WITH totals AS (
    SELECT
      wdj.organization_id,
      wdj.WIP_ENTITY_ID,
      oap2.ACCT_PERIOD_ID,
      oap2.period_name,
      SUM(
        ROUND(
          (
            COALESCE(wpb.tl_resource_in, 0) + COALESCE(wpb.tl_overhead_in, 0) + COALESCE(wpb.tl_outside_processing_in, 0) + COALESCE(wpb.pl_material_in, 0) + COALESCE(wpb.pl_resource_in, 0) + COALESCE(wpb.pl_overhead_in, 0) + COALESCE(wpb.pl_outside_processing_in, 0) + COALESCE(wpb.pl_material_overhead_in, 0) + COALESCE(wpb.tl_scrap_in, 0)
          ),
          2
        )
      ) AS total_incured,
      SUM(
        ROUND(
          (
            COALESCE(wpb.tl_material_var, 0) + COALESCE(wpb.tl_resource_var, 0) + COALESCE(wpb.tl_overhead_var, 0) + COALESCE(wpb.tl_outside_processing_var, 0) + COALESCE(wpb.pl_material_var, 0) + COALESCE(wpb.pl_resource_var, 0) + COALESCE(wpb.pl_overhead_var, 0) + COALESCE(wpb.pl_outside_processing_var, 0) + COALESCE(wpb.tl_material_overhead_var, 0) + COALESCE(wpb.pl_material_overhead_var, 0) + COALESCE(wpb.tl_scrap_var, 0)
          ),
          2
        )
      ) AS total_variance,
      SUM(
        ROUND(
          (
            COALESCE(wpb.tl_resource_out, 0) + COALESCE(wpb.tl_overhead_out, 0) + COALESCE(wpb.tl_outside_processing_out, 0) + COALESCE(wpb.pl_material_out, 0) + COALESCE(wpb.pl_material_overhead_out, 0) + COALESCE(wpb.pl_resource_out, 0) + COALESCE(wpb.pl_overhead_out, 0) + COALESCE(wpb.pl_outside_processing_out, 0) + COALESCE(wpb.tl_material_overhead_out, 0) + COALESCE(wpb.tl_material_out, 0) + COALESCE(wpb.tl_scrap_out, 0)
          ),
          2
        )
      ) AS total_relived
    FROM silver_bec_ods.FND_LOOKUP_VALUES AS ml, silver_bec_ods.wip_period_balances AS wpb, silver_bec_ods.org_acct_periods AS oap, silver_bec_ods.mtl_system_items_b AS msi, silver_bec_ods.wip_entities AS we, silver_bec_ods.wip_discrete_jobs AS wdj, silver_bec_ods.ORG_ACCT_PERIODS AS oap2
    WHERE
      wpb.wip_entity_id = wdj.wip_entity_id
      AND wpb.organization_id = wdj.organization_id
      AND ml.lookup_type = 'WIP_CLASS_TYPE'
      AND ml.lookup_code = wpb.class_type
      AND oap.organization_id = wdj.organization_id
      AND oap.acct_period_id = wpb.acct_period_id
      AND oap.schedule_close_date <= COALESCE(oap2.PERIOD_CLOSE_DATE, oap2.schedule_close_date)
      AND we.wip_entity_id = wdj.wip_entity_id
      AND we.organization_id = wdj.organization_id
      AND wdj.primary_item_id = msi.INVENTORY_ITEM_ID()
      AND msi.ORGANIZATION_ID() = wdj.organization_id /* and wdj.WIP_ENTITY_ID = 12693719 */
      AND oap2.organization_id = wdj.ORGANIZATION_ID
      AND oap2.period_start_date > CURRENT_TIMESTAMP() - 365
    GROUP BY
      wdj.organization_id,
      wdj.WIP_ENTITY_ID,
      oap2.period_name,
      oap2.ACCT_PERIOD_ID
  ), wip_operations AS (
    SELECT
      MAX(operation_seq_num) AS operation_seq_num,
      wip_entity_id
    FROM silver_bec_ods.wip_operations
    WHERE
      NOT date_last_moved IS NULL
    GROUP BY
      wip_entity_id
  )
  SELECT
    wdj.organization_id,
    ood.organization_code,
    ood.organization_name,
    totals.period_name,
    we.wip_entity_name,
    we.wip_entity_id,
    msi.segment1 AS assembly,
    msi.description AS assembly_description,
    we.creation_date,
    wdj.actual_start_date,
    ml.meaning AS class_type,
    wdj.class_code AS class_code,
    wsg.schedule_group_name,
    wdj.scheduled_start_date,
    wdj.scheduled_completion_date,
    wdj.date_released,
    wdj.date_completed,
    wdj.date_closed,
    wo.operation_seq_num,
    fu.user_name AS created_by,
    (
      SELECT
        meaning
      FROM silver_bec_ods.fnd_lookup_values
      WHERE
        lookup_type = 'WIP_JOB_STATUS' AND lookup_code = wdj.status_type
    ) AS job_status_type,
    wdj.start_quantity,
    wdj.quantity_completed,
    wdj.quantity_scrapped,
    wdj.net_quantity,
    totals.total_incured,
    totals.total_relived,
    totals.total_variance,
    totals.total_incured - totals.total_relived - totals.total_variance AS ending_balance,
    (
      SELECT
        system_id
      FROM bec_etl_ctrl.etlsourceappid
      WHERE
        source_system = 'EBS'
    ) || '-' || wdj.organization_id AS organization_id_KEY,
    (
      SELECT
        system_id
      FROM bec_etl_ctrl.etlsourceappid
      WHERE
        source_system = 'EBS'
    ) || '-' || we.wip_entity_id AS wip_entity_id_KEY,
    'N' AS IS_DELETED_FLG,
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
    ) || '-' || COALESCE(we.wip_entity_id, 0) || '-' || COALESCE(totals.period_name, '1900-01-01 12:00:00') AS dw_load_id,
    CURRENT_TIMESTAMP() AS dw_insert_date,
    CURRENT_TIMESTAMP() AS dw_update_date
  FROM silver_bec_ods.wip_discrete_jobs AS wdj, silver_bec_ods.wip_entities AS we, silver_bec_ods.mtl_system_items_b AS msi, silver_bec_ods.wip_period_balances AS wpb, silver_bec_ods.org_organization_definitions AS ood, silver_bec_ods.fnd_lookup_values AS ml, silver_bec_ods.wip_schedule_groups AS wsg, silver_bec_ods.fnd_user AS fu, totals, wip_operations AS wo
  WHERE
    wdj.wip_entity_id = we.wip_entity_id
    AND wdj.organization_id = we.organization_id
    AND wdj.primary_item_id = msi.INVENTORY_ITEM_ID()
    AND wdj.organization_id = msi.ORGANIZATION_ID()
    AND wdj.organization_id = ood.organization_id
    AND wdj.wip_entity_id = wpb.wip_entity_id
    AND wdj.organization_id = wpb.organization_id
    AND ml.lookup_type = 'WIP_CLASS_TYPE'
    AND wpb.class_type = ml.lookup_code
    AND wdj.schedule_group_id = wsg.SCHEDULE_GROUP_ID()
    AND wdj.organization_id = wsg.ORGANIZATION_ID()
    AND wdj.created_by = fu.user_id
    AND totals.organization_id = wdj.organization_id
    AND totals.wip_entity_id = we.wip_entity_id
    AND totals.acct_period_id = wpb.acct_period_id
    AND wdj.wip_entity_id = wo.WIP_ENTITY_ID()
);
UPDATE bec_etl_ctrl.batch_dw_info SET load_type = 'I', last_refresh_date = CURRENT_TIMESTAMP()
WHERE
  dw_table_name = 'fact_wip_period_value' AND batch_name = 'inv';