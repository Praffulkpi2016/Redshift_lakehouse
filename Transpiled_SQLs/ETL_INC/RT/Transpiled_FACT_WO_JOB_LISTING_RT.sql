TRUNCATE table gold_bec_dwh_rpt.FACT_WO_JOB_LISTING_RT;
INSERT INTO gold_bec_dwh_rpt.FACT_WO_JOB_LISTING_RT
(
  SELECT DISTINCT
    wjl.JOB_NAME,
    wjl.creation_date,
    wjl.created_by,
    wjl.description,
    wjl.ASS_ITEM_ID,
    wjl.ASSEMBLY AS part_number,
    wjl.ASSEMBLY_DESCRIPTION AS part_desp,
    CASE WHEN firm = 1 THEN 'Yes' WHEN firm = 2 THEN 'No' END AS firm,
    mp.employee_id,
    wjl.planner_code,
    pov.agent_name AS buyer,
    JOB_TYPE,
    class_code,
    SCHEDULED_START_DATE,
    SCHEDULE_GROUP_NAME,
    date_released,
    scheduled_completion_date,
    ACT_DATE_COMPLETED AS date_completed,
    date_closed,
    SCHED_QTY AS start_quantity,
    CASE
      WHEN wjl.SCHED_QTY - wjl.QTY_COMPLETED - wjl.QTY_SCRAPPED = 0
      THEN NULL
      ELSE wjl.SCHED_QTY - wjl.QTY_COMPLETED - wjl.QTY_SCRAPPED
    END AS QUANTITY_REMAINING,
    QTY_COMPLETED,
    QTY_SCRAPPED,
    net_quantity,
    bom_revision,
    wjl.project_no,
    wjl.task_no,
    JOB_STATUS,
    lu.meaning AS supply_type,
    wjl.organization_id,
    cic.item_cost,
    wjl.SCHED_QTY * cic.item_cost AS ext_cost
  FROM gold_bec_dwh.FACT_WO_JOB_LISTING AS wjl, silver_bec_ods.mtl_planners AS mp, silver_bec_ods.po_agents_v AS pov, silver_bec_ods.cst_item_costs AS cic, silver_bec_ods.FND_LOOKUP_VALUES AS lu
  WHERE
    wjl.buyer_id = pov.AGENT_ID()
    AND wjl.ASS_ITEM_ID = cic.INVENTORY_ITEM_ID()
    AND wjl.organization_id = cic.ORGANIZATION_ID()
    AND cic.COST_TYPE_ID() = 1
    AND wjl.planner_code = mp.PLANNER_CODE()
    AND wjl.organization_id = mp.ORGANIZATION_ID()
    AND lu.lookup_type = 'WIP_SUPPLY'
    AND lu.lookup_code = wjl.wip_supply_type
);
UPDATE bec_etl_ctrl.batch_dw_info SET load_type = 'I', last_refresh_date = CURRENT_TIMESTAMP()
WHERE
  dw_table_name = 'fact_wo_job_listing_rt' AND batch_name = 'wip';