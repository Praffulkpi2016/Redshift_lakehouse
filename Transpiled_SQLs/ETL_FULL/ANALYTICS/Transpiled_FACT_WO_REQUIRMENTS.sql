DROP table IF EXISTS gold_bec_dwh.FACT_WO_REQUIRMENTS;
CREATE TABLE gold_bec_dwh.FACT_WO_REQUIRMENTS AS
(
  SELECT
    wdj.WIP_ENTITY_ID,
    wdj.ORGANIZATION_ID,
    wdj.PRIMARY_ITEM_ID AS INVENTORY_ITEM_ID,
    we.wip_entity_name AS job_no,
    we.creation_date,
    we.description,
    msi1.segment1 AS primary_part_no,
    msi1.description AS primary_part_description,
    (
      SELECT
        meaning
      FROM silver_bec_ods.fnd_lookup_values
      WHERE
        lookup_type = 'WIP_DISCRETE_JOB' AND lookup_code = we.entity_type
    ) AS job_type,
    wdj.class_code,
    wdj.scheduled_start_date,
    wdj.date_released,
    wdj.scheduled_completion_date,
    wdj.date_completed,
    wdj.start_quantity,
    CASE
      WHEN wdj.start_quantity - wdj.quantity_completed - wdj.quantity_scrapped = 0
      THEN 0
      ELSE wdj.start_quantity - wdj.quantity_completed - wdj.quantity_scrapped
    END AS `QUANTITY_REMAINING`,
    CASE WHEN wdj.quantity_completed = 0 THEN 0 ELSE wdj.quantity_completed END AS `QUANTITY_COMPLETED`,
    CASE WHEN wdj.quantity_scrapped = 0 THEN 0 ELSE wdj.quantity_scrapped END AS `QUANTITY_SCRAPPED`,
    wo.operation_seq_num,
    wo.description AS operation_description,
    CASE WHEN wo.quantity_in_queue = 0 THEN 0 ELSE wo.quantity_in_queue END AS qty_in_queue,
    CASE WHEN wo.quantity_running = 0 THEN 0 ELSE wo.quantity_running END AS qty_running,
    CASE
      WHEN wo.quantity_waiting_to_move = 0
      THEN 0
      ELSE wo.quantity_waiting_to_move
    END AS qty_waiting_to_move,
    CASE WHEN wo.quantity_rejected = 0 THEN 0 ELSE wo.quantity_rejected END AS qty_rejected,
    wdj.net_quantity,
    msi2.segment1 AS component,
    msi2.description AS comp_description,
    wro.required_quantity,
    wro.quantity_per_assembly,
    wro.quantity_issued,
    (
      SELECT
        meaning
      FROM silver_bec_ods.fnd_lookup_values
      WHERE
        lookup_type = 'WIP_SUPPLY' AND lookup_code = wro.wip_supply_type
    ) AS supply_type,
    cic.item_cost,
    (
      COALESCE(wro.required_quantity, 0) * (
        cic.item_cost
      )
    ) AS sched_amt,
    (
      COALESCE(wro.quantity_issued, 0) * (
        cic.item_cost
      )
    ) AS actual_amt,
    (
      COALESCE(wro.required_quantity, 0) * (
        cic.item_cost
      )
    ) - (
      COALESCE(wro.quantity_issued, 0) * (
        cic.item_cost
      )
    ) AS VARIANCE,
    wdj.bom_revision,
    wsg.schedule_group_name,
    wdj.attribute1 AS project_no,
    wdj.attribute2 AS task_no,
    (
      SELECT
        meaning
      FROM silver_bec_ods.fnd_lookup_values
      WHERE
        lookup_type = 'WIP_JOB_STATUS' AND lookup_code = wdj.status_type
    ) AS job_status_type,
    ood.ORGANIZATION_NAME,
    ood.ORGANIZATION_CODE,
    wro.inventory_item_id AS part_inventory_item_id,
    msi2.primary_uom_code AS item_primary_uom_code,
    wro.date_required,
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
    ) || '-' || COALESCE(part_inventory_item_id, 0) || '-' || COALESCE(wdj.ORGANIZATION_ID, 0) || '-' || COALESCE(wdj.WIP_ENTITY_ID, 0) || '-' || COALESCE(wo.operation_seq_num, 0) || '-' || COALESCE(JOB_NO, 'NA') AS dw_load_id,
    CURRENT_TIMESTAMP() AS dw_insert_date,
    CURRENT_TIMESTAMP() AS dw_update_date
  FROM (
    SELECT
      *
    FROM silver_bec_ods.wip_entities
    WHERE
      IS_DELETED_FLG <> 'Y'
  ) AS we, (
    SELECT
      *
    FROM silver_bec_ods.wip_discrete_jobs
    WHERE
      IS_DELETED_FLG <> 'Y'
  ) AS wdj, (
    SELECT
      *
    FROM silver_bec_ods.wip_operations
    WHERE
      IS_DELETED_FLG <> 'Y'
  ) AS wo, (
    SELECT
      *
    FROM silver_bec_ods.wip_requirement_operations
    WHERE
      IS_DELETED_FLG <> 'Y'
  ) AS wro, (
    SELECT
      *
    FROM silver_bec_ods.wip_schedule_groups
    WHERE
      IS_DELETED_FLG <> 'Y'
  ) AS wsg, (
    SELECT
      *
    FROM silver_bec_ods.mtl_system_items_b
    WHERE
      IS_DELETED_FLG <> 'Y'
  ) AS msi1, (
    SELECT
      *
    FROM silver_bec_ods.mtl_system_items_b
    WHERE
      IS_DELETED_FLG <> 'Y'
  ) AS msi2, (
    SELECT
      *
    FROM silver_bec_ods.cst_item_costs
    WHERE
      IS_DELETED_FLG <> 'Y'
  ) AS cic, (
    SELECT
      *
    FROM silver_bec_ods.org_organization_definitions
    WHERE
      IS_DELETED_FLG <> 'Y'
  ) AS ood
  WHERE
    we.wip_entity_id = wdj.wip_entity_id
    AND we.organization_id = wdj.organization_id
    AND wdj.primary_item_id = msi1.inventory_item_id
    AND wdj.organization_id = msi1.organization_id
    AND wdj.schedule_group_id = wsg.SCHEDULE_GROUP_ID()
    AND wdj.wip_entity_id = wo.wip_entity_id
    AND wdj.organization_id = wo.organization_id
    AND wo.wip_entity_id = wro.wip_entity_id
    AND wo.operation_seq_num = wro.operation_seq_num
    AND wo.organization_id = wro.organization_id
    AND wro.inventory_item_id = msi2.INVENTORY_ITEM_ID()
    AND wro.organization_id = msi2.ORGANIZATION_ID()
    AND wro.inventory_item_id = cic.INVENTORY_ITEM_ID()
    AND wro.organization_id = cic.ORGANIZATION_ID()
    AND cic.COST_TYPE_ID() = 1
    AND we.ORGANIZATION_ID = ood.ORGANIZATION_ID
);
UPDATE bec_etl_ctrl.batch_dw_info SET load_type = 'I', last_refresh_date = CURRENT_TIMESTAMP()
WHERE
  dw_table_name = 'fact_wo_requirments' AND batch_name = 'wip';