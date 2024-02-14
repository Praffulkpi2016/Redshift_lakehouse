TRUNCATE table gold_bec_dwh.fact_wo_job_listing_tmp;
/* Insert records into temp table */
INSERT INTO gold_bec_dwh.fact_wo_job_listing_tmp
(
  SELECT DISTINCT
    DJ.WIP_ENTITY_ID
  FROM (
    SELECT
      *
    FROM silver_bec_ods.WIP_DISCRETE_JOBS
    WHERE
      IS_DELETED_FLG <> 'Y'
  ) AS DJ, (
    SELECT
      WIP_ENTITY_ID,
      ORGANIZATION_ID,
      WIP_ENTITY_NAME,
      ENTITY_TYPE,
      CREATED_BY,
      kca_seq_date
    FROM silver_bec_ods.WIP_ENTITIES
    WHERE
      IS_DELETED_FLG <> 'Y'
  ) AS WE, (
    SELECT
      INVENTORY_ITEM_ID,
      ORGANIZATION_ID,
      SEGMENT1,
      DESCRIPTION,
      PRIMARY_UOM_CODE,
      planner_code,
      buyer_id,
      kca_seq_date
    FROM silver_bec_ods.MTL_SYSTEM_ITEMS_B
    WHERE
      IS_DELETED_FLG <> 'Y'
  ) AS MSI, (
    SELECT
      *
    FROM silver_bec_ods.WIP_SCHEDULE_GROUPS
    WHERE
      IS_DELETED_FLG <> 'Y'
  ) AS SG, (
    SELECT
      REQUIRED_QUANTITY,
      QUANTITY_ISSUED,
      OPERATION_SEQ_NUM,
      WIP_ENTITY_ID,
      INVENTORY_ITEM_ID,
      ORGANIZATION_ID,
      REPETITIVE_SCHEDULE_ID,
      kca_seq_date
    FROM silver_bec_ods.WIP_REQUIREMENT_OPERATIONS
    WHERE
      IS_DELETED_FLG <> 'Y'
  ) AS WRO
  WHERE
    1 = 1
    AND DJ.WIP_ENTITY_ID = WE.WIP_ENTITY_ID
    AND DJ.ORGANIZATION_ID = WE.ORGANIZATION_ID
    AND DJ.ORGANIZATION_ID = SG.ORGANIZATION_ID()
    AND DJ.SCHEDULE_GROUP_ID = SG.SCHEDULE_GROUP_ID()
    AND DJ.PRIMARY_ITEM_ID = MSI.INVENTORY_ITEM_ID()
    AND DJ.ORGANIZATION_ID = MSI.ORGANIZATION_ID()
    AND DJ.WIP_ENTITY_ID = WRO.WIP_ENTITY_ID()
    AND DJ.ORGANIZATION_ID = WRO.ORGANIZATION_ID()
    AND (
      DJ.kca_seq_date > (
        SELECT
          (
            executebegints - prune_days
          )
        FROM bec_etl_ctrl.batch_dw_info
        WHERE
          dw_table_name = 'fact_wo_job_listing' AND batch_name = 'wip'
      )
      OR WE.kca_seq_date > (
        SELECT
          (
            executebegints - prune_days
          )
        FROM bec_etl_ctrl.batch_dw_info
        WHERE
          dw_table_name = 'fact_wo_job_listing' AND batch_name = 'wip'
      )
      OR MSI.kca_seq_date > (
        SELECT
          (
            executebegints - prune_days
          )
        FROM bec_etl_ctrl.batch_dw_info
        WHERE
          dw_table_name = 'fact_wo_job_listing' AND batch_name = 'wip'
      )
      OR SG.kca_seq_date > (
        SELECT
          (
            executebegints - prune_days
          )
        FROM bec_etl_ctrl.batch_dw_info
        WHERE
          dw_table_name = 'fact_wo_job_listing' AND batch_name = 'wip'
      )
      OR WRO.kca_seq_date > (
        SELECT
          (
            executebegints - prune_days
          )
        FROM bec_etl_ctrl.batch_dw_info
        WHERE
          dw_table_name = 'fact_wo_job_listing' AND batch_name = 'wip'
      )
    )
);
/* delete records from fact table */
DELETE FROM gold_bec_dwh.fact_wo_job_listing
WHERE
  EXISTS(
    SELECT
      1
    FROM gold_bec_dwh.fact_wo_job_listing_tmp AS tmp
    WHERE
      COALESCE(tmp.WIP_ENTITY_ID, 0) = COALESCE(fact_wo_job_listing.WIP_ENTITY_ID, 0)
  );
/* Insert records into fact table */
INSERT INTO gold_bec_dwh.fact_wo_job_listing
(
  SELECT
    DJ.WIP_ENTITY_ID,
    DJ.ORGANIZATION_ID,
    (
      SELECT
        system_id
      FROM bec_etl_ctrl.etlsourceappid
      WHERE
        source_system = 'EBS'
    ) || '-' || DJ.ORGANIZATION_ID AS organization_id_key,
    DJ.PRIMARY_ITEM_ID AS ASS_ITEM_ID,
    MSI1.INVENTORY_ITEM_ID AS COMP_ITEM_ID,
    SG.SCHEDULE_GROUP_ID,
    WE.WIP_ENTITY_NAME AS JOB_NAME,
    ML1.MEANING AS JOB_TYPE,
    MSI.SEGMENT1 AS ASSEMBLY,
    MSI.DESCRIPTION AS ASSEMBLY_DESCRIPTION,
    MSI.PRIMARY_UOM_CODE AS UOM,
    ML2.MEANING AS JOB_STATUS,
    MSI1.SEGMENT1 AS COMPONENT_ITEM,
    MSI1.DESCRIPTION AS COMPONENT_ITEM_DESCRIPTION,
    WE.ENTITY_TYPE,
    WE.CREATED_BY,
    WRO.REQUIRED_QUANTITY,
    WRO.QUANTITY_ISSUED,
    WRO.OPERATION_SEQ_NUM,
    DJ.START_QUANTITY AS SCHED_QTY,
    DJ.QUANTITY_COMPLETED AS QTY_COMPLETED,
    DJ.QUANTITY_SCRAPPED AS QTY_SCRAPPED,
    DJ.SCHEDULED_START_DATE AS SCHEDULED_START_DATE,
    DJ.SCHEDULED_COMPLETION_DATE AS SCHEDULED_COMPLETION_DATE,
    SG.SCHEDULE_GROUP_NAME AS SCHEDULE_GROUP_NAME,
    DJ.BUILD_SEQUENCE AS BUILD_SEQUENCE,
    DJ.DATE_COMPLETED AS ACT_DATE_COMPLETED,
    DJ.DATE_CLOSED AS DATE_CLOSED,
    DJ.CREATION_DATE,
    DJ.DATE_RELEASED,
    dj.description,
    dj.firm_planned_flag AS firm,
    msi.planner_code,
    dj.class_code,
    dj.net_quantity,
    dj.bom_revision,
    dj.wip_supply_type,
    dj.project_id,
    dj.task_id,
    dj.attribute1 AS project_no,
    dj.attribute2 AS task_no,
    msi.buyer_id,
    WRO.REPETITIVE_SCHEDULE_ID,
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
    ) || '-' || COALESCE(MSI1.INVENTORY_ITEM_ID, 0) || '-' || COALESCE(DJ.ORGANIZATION_ID, 0) || '-' || COALESCE(DJ.WIP_ENTITY_ID, 0) || '-' || COALESCE(WRO.OPERATION_SEQ_NUM, 0) || '-' || COALESCE(SG.SCHEDULE_GROUP_ID, 0) || '-' || COALESCE(WRO.REPETITIVE_SCHEDULE_ID, 0) AS dw_load_id,
    CURRENT_TIMESTAMP() AS dw_insert_date,
    CURRENT_TIMESTAMP() AS dw_update_date
  FROM (
    SELECT
      *
    FROM silver_bec_ods.WIP_DISCRETE_JOBS
    WHERE
      IS_DELETED_FLG <> 'Y'
  ) AS DJ, (
    SELECT
      WIP_ENTITY_ID,
      ORGANIZATION_ID,
      WIP_ENTITY_NAME,
      ENTITY_TYPE,
      CREATED_BY
    FROM silver_bec_ods.WIP_ENTITIES
    WHERE
      IS_DELETED_FLG <> 'Y'
  ) AS WE, (
    SELECT
      INVENTORY_ITEM_ID,
      ORGANIZATION_ID,
      SEGMENT1,
      DESCRIPTION,
      PRIMARY_UOM_CODE,
      planner_code,
      buyer_id
    FROM silver_bec_ods.MTL_SYSTEM_ITEMS_B
    WHERE
      IS_DELETED_FLG <> 'Y'
  ) AS MSI, (
    SELECT
      INVENTORY_ITEM_ID,
      ORGANIZATION_ID,
      SEGMENT1,
      DESCRIPTION
    FROM silver_bec_ods.MTL_SYSTEM_ITEMS_B
    WHERE
      IS_DELETED_FLG <> 'Y'
  ) AS MSI1, (
    SELECT
      *
    FROM silver_bec_ods.WIP_SCHEDULE_GROUPS
    WHERE
      IS_DELETED_FLG <> 'Y'
  ) AS SG, (
    SELECT
      REQUIRED_QUANTITY,
      QUANTITY_ISSUED,
      OPERATION_SEQ_NUM,
      WIP_ENTITY_ID,
      INVENTORY_ITEM_ID,
      ORGANIZATION_ID,
      REPETITIVE_SCHEDULE_ID
    FROM silver_bec_ods.WIP_REQUIREMENT_OPERATIONS
    WHERE
      IS_DELETED_FLG <> 'Y'
  ) AS WRO, (
    SELECT
      MEANING,
      LOOKUP_CODE
    FROM silver_bec_ods.FND_LOOKUP_VALUES
    WHERE
      IS_DELETED_FLG <> 'Y' AND LOOKUP_TYPE = 'WIP_DISCRETE_JOB'
  ) AS ML1, (
    SELECT
      MEANING,
      LOOKUP_CODE
    FROM silver_bec_ods.FND_LOOKUP_VALUES
    WHERE
      IS_DELETED_FLG <> 'Y' AND LOOKUP_TYPE = 'WIP_JOB_STATUS'
  ) AS ML2, gold_bec_dwh.fact_wo_job_listing_tmp AS tmp
  WHERE
    1 = 1
    AND DJ.WIP_ENTITY_ID = WE.WIP_ENTITY_ID
    AND DJ.ORGANIZATION_ID = WE.ORGANIZATION_ID
    AND DJ.ORGANIZATION_ID = SG.ORGANIZATION_ID()
    AND DJ.SCHEDULE_GROUP_ID = SG.SCHEDULE_GROUP_ID()
    AND DJ.PRIMARY_ITEM_ID = MSI.INVENTORY_ITEM_ID()
    AND DJ.ORGANIZATION_ID = MSI.ORGANIZATION_ID()
    AND DJ.WIP_ENTITY_ID = WRO.WIP_ENTITY_ID()
    AND DJ.ORGANIZATION_ID = WRO.ORGANIZATION_ID()
    AND WRO.INVENTORY_ITEM_ID = MSI1.INVENTORY_ITEM_ID()
    AND WRO.ORGANIZATION_ID = MSI1.ORGANIZATION_ID()
    AND DJ.JOB_TYPE = ML1.LOOKUP_CODE
    AND DJ.STATUS_TYPE = ML2.LOOKUP_CODE
    AND COALESCE(tmp.WIP_ENTITY_ID, 0) = COALESCE(DJ.WIP_ENTITY_ID, 0)
);
UPDATE bec_etl_ctrl.batch_dw_info SET load_type = 'I', last_refresh_date = CURRENT_TIMESTAMP()
WHERE
  dw_table_name = 'fact_wo_job_listing' AND batch_name = 'wip';