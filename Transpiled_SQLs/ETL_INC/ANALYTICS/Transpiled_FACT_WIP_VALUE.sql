/* Delete Records */
DELETE FROM gold_bec_dwh.FACT_WIP_VALUE
WHERE
  (COALESCE(organization_id, 0), COALESCE(wip_entity_id, 0), COALESCE(primary_item_id, 0), COALESCE(operation_seq_num, 0) /*	nvl(acct_period_id, 0), */) IN (
    SELECT
      COALESCE(ODS.organization_id, 0) AS organization_id,
      COALESCE(ODS.wip_entity_id, 0) AS wip_entity_id,
      COALESCE(ODS.primary_item_id, 0) AS ass_item_id,
      COALESCE(ODS.operation_seq_num, 0) AS operation_seq_num /*	nvl(ODS.acct_period_id, 0) as acct_period_id, */
    FROM gold_bec_dwh.FACT_WIP_VALUE AS dw, (
      SELECT
        W.WIP_ENTITY_ID,
        W.ORGANIZATION_ID,
        W.PRIMARY_ITEM_ID,
        WRO.OPERATION_SEQ_NUM
      FROM BEC_ODS.WIP_PERIOD_BALANCES AS P, BEC_ODS.WIP_DISCRETE_JOBS AS W, BEC_ODS.WIP_ENTITIES AS E, BEC_ODS.WIP_REQUIREMENT_OPERATIONS AS WRO, BEC_ODS.FND_USER AS U, BEC_ODS.MTL_SYSTEM_ITEMS_B AS M, BEC_ODS.WIP_SCHEDULE_GROUPS AS SG, BEC_ODS.ORG_ORGANIZATION_DEFINITIONS AS O, BEC_ODS.FND_LOOKUP_VALUES AS ML, BEC_ODS.FND_LOOKUP_VALUES AS ML2
      WHERE
        1 = 1
        AND W.WIP_ENTITY_ID = P.WIP_ENTITY_ID
        AND W.WIP_ENTITY_ID = E.WIP_ENTITY_ID
        AND W.ORGANIZATION_ID = E.ORGANIZATION_ID
        AND W.PRIMARY_ITEM_ID = M.INVENTORY_ITEM_ID()
        AND W.ORGANIZATION_ID = M.ORGANIZATION_ID()
        AND O.ORGANIZATION_ID = E.ORGANIZATION_ID
        AND O.ORGANIZATION_ID = P.ORGANIZATION_ID
        AND W.WIP_ENTITY_ID = WRO.WIP_ENTITY_ID()
        AND W.ORGANIZATION_ID = WRO.ORGANIZATION_ID()
        AND W.PRIMARY_ITEM_ID = WRO.INVENTORY_ITEM_ID()
        AND W.ORGANIZATION_ID = SG.ORGANIZATION_ID()
        AND W.SCHEDULE_GROUP_ID = SG.SCHEDULE_GROUP_ID()
        AND ML.LANGUAGE = 'US'
        AND ML.LOOKUP_TYPE = 'WIP_CLASS_TYPE'
        AND ML.LOOKUP_CODE = P.CLASS_TYPE
        AND ML2.LANGUAGE = 'US'
        AND ML2.LOOKUP_CODE = W.STATUS_TYPE
        AND ML2.LOOKUP_TYPE = 'WIP_JOB_STATUS'
        AND E.CREATED_BY = U.USER_ID()
        AND (
          W.kca_seq_date > (
            SELECT
              (
                executebegints - prune_days
              )
            FROM bec_etl_ctrl.batch_dw_info
            WHERE
              dw_table_name = 'fact_wip_value' AND batch_name = 'wip'
          )
          OR P.is_deleted_flg = 'Y'
          OR W.is_deleted_flg = 'Y'
          OR E.is_deleted_flg = 'Y'
          OR WRO.is_deleted_flg = 'Y'
          OR U.is_deleted_flg = 'Y'
          OR M.is_deleted_flg = 'Y'
          OR SG.is_deleted_flg = 'Y'
          OR O.is_deleted_flg = 'Y'
          OR ML.is_deleted_flg = 'Y'
          OR ML2.is_deleted_flg = 'Y'
        )
      GROUP BY
        WRO.OPERATION_SEQ_NUM,
        W.WIP_ENTITY_ID,
        W.ORGANIZATION_ID,
        W.PRIMARY_ITEM_ID
    ) AS ods
    WHERE
      dw.dw_load_id = (
        SELECT
          system_id
        FROM bec_etl_ctrl.etlsourceappid
        WHERE
          source_system = 'EBS'
      ) || '-' || COALESCE(ODS.organization_id, 0) || '-' || COALESCE(ODS.wip_entity_id, 0) || '-' || COALESCE(ODS.primary_item_id, 0) || '-' || COALESCE(ODS.operation_seq_num, 0)
  );
/* Insert records */
INSERT INTO gold_bec_dwh.FACT_WIP_VALUE (
  wip_entity_id,
  organization_id,
  primary_item_id,
  schedule_group_id,
  job_number,
  assembly_item,
  assembly_item_description,
  job_creation_date,
  date_released,
  scheduled_start_date,
  scheduled_completion_date,
  date_closed,
  job_created_by,
  operation_seq_num,
  class_type,
  class_code,
  prelim_status,
  schedule_group_name,
  start_quantity,
  quantity_completed,
  quantity_scrapped,
  quantity_end_balance,
  net_quantity,
  scheduled_end_date,
  organization_code,
  costs_incurred,
  costs_relieved,
  variances_relieved,
  end_variance,
  is_deleted_flg,
  source_app_id,
  dw_load_id,
  dw_insert_date,
  dw_update_date
)
(
  SELECT
    W.WIP_ENTITY_ID,
    W.ORGANIZATION_ID,
    W.PRIMARY_ITEM_ID,
    SG.SCHEDULE_GROUP_ID,
    E.WIP_ENTITY_NAME AS JOB_NUMBER,
    M.SEGMENT1 AS ASSEMBLY_ITEM,
    M.DESCRIPTION AS ASSEMBLY_ITEM_DESCRIPTION,
    W.CREATION_DATE AS JOB_CREATION_DATE,
    W.DATE_RELEASED,
    W.SCHEDULED_START_DATE,
    W.SCHEDULED_COMPLETION_DATE,
    W.DATE_CLOSED,
    U.USER_NAME AS JOB_CREATED_BY,
    WRO.OPERATION_SEQ_NUM,
    ML.MEANING AS CLASS_TYPE,
    W.CLASS_CODE,
    ML2.MEANING AS PRELIM_STATUS,
    SG.SCHEDULE_GROUP_NAME,
    W.START_QUANTITY,
    W.QUANTITY_COMPLETED,
    W.QUANTITY_SCRAPPED,
    (
      W.START_QUANTITY - W.QUANTITY_COMPLETED - W.QUANTITY_SCRAPPED
    ) AS QUANTITY_END_BALANCE,
    W.NET_QUANTITY,
    W.SCHEDULED_COMPLETION_DATE AS SCHEDULED_END_DATE,
    O.ORGANIZATION_CODE,
    SUM(
      P.TL_RESOURCE_IN + P.TL_OVERHEAD_IN + P.TL_OUTSIDE_PROCESSING_IN + P.PL_MATERIAL_IN + P.PL_RESOURCE_IN + P.PL_OVERHEAD_IN + P.PL_OUTSIDE_PROCESSING_IN + P.PL_MATERIAL_OVERHEAD_IN
    ) AS COSTS_INCURRED, /* PRD.PERIOD_NAME, */
    SUM(
      P.TL_RESOURCE_OUT + P.TL_OVERHEAD_OUT + P.TL_OUTSIDE_PROCESSING_OUT + P.PL_MATERIAL_OUT + P.PL_RESOURCE_OUT + P.PL_OVERHEAD_OUT + P.PL_OUTSIDE_PROCESSING_OUT + P.PL_MATERIAL_OVERHEAD_OUT
    ) AS COSTS_RELIEVED,
    SUM(
      P.TL_RESOURCE_VAR + P.TL_OVERHEAD_VAR + P.TL_OUTSIDE_PROCESSING_VAR + P.PL_MATERIAL_VAR + P.PL_RESOURCE_VAR + P.PL_OVERHEAD_VAR + P.PL_OUTSIDE_PROCESSING_VAR + P.PL_MATERIAL_OVERHEAD_VAR
    ) AS VARIANCES_RELIEVED,
    SUM(
      P.TL_RESOURCE_IN + P.TL_OVERHEAD_IN + P.TL_OUTSIDE_PROCESSING_IN + P.PL_MATERIAL_IN + P.PL_RESOURCE_IN + P.PL_OVERHEAD_IN + P.PL_OUTSIDE_PROCESSING_IN + P.PL_MATERIAL_OVERHEAD_IN - (
        P.TL_RESOURCE_OUT + P.TL_OVERHEAD_OUT + P.TL_OUTSIDE_PROCESSING_OUT + P.PL_MATERIAL_OUT + P.PL_RESOURCE_OUT + P.PL_OVERHEAD_OUT + P.PL_OUTSIDE_PROCESSING_OUT + P.PL_MATERIAL_OVERHEAD_OUT
      ) - (
        P.TL_RESOURCE_VAR + P.TL_OVERHEAD_VAR + P.TL_OUTSIDE_PROCESSING_VAR + P.PL_MATERIAL_VAR + P.PL_RESOURCE_VAR + P.PL_OVERHEAD_VAR + P.PL_OUTSIDE_PROCESSING_VAR + P.PL_MATERIAL_OVERHEAD_VAR
      )
    ) AS END_VARIANCE,
    'N' AS is_deleted_flg,
    (
      SELECT
        SYSTEM_ID
      FROM BEC_ETL_CTRL.ETLSOURCEAPPID
      WHERE
        SOURCE_SYSTEM = 'EBS'
    ) AS SOURCE_APP_ID,
    (
      SELECT
        SYSTEM_ID
      FROM BEC_ETL_CTRL.ETLSOURCEAPPID
      WHERE
        SOURCE_SYSTEM = 'EBS'
    ) || '-' || COALESCE(W.ORGANIZATION_ID, 0) || '-' || COALESCE(W.WIP_ENTITY_ID, 0) || '-' || COALESCE(W.PRIMARY_ITEM_ID, 0) || '-' || COALESCE(WRO.OPERATION_SEQ_NUM, 0) AS DW_LOAD_ID,
    CURRENT_TIMESTAMP() AS DW_INSERT_DATE,
    CURRENT_TIMESTAMP() AS DW_UPDATE_DATE
  FROM (
    SELECT
      *
    FROM BEC_ODS.WIP_PERIOD_BALANCES
    WHERE
      IS_DELETED_FLG <> 'Y'
  ) AS P, (
    SELECT
      *
    FROM BEC_ODS.WIP_DISCRETE_JOBS
    WHERE
      IS_DELETED_FLG <> 'Y'
  ) AS W, (
    SELECT
      *
    FROM BEC_ODS.WIP_ENTITIES
    WHERE
      IS_DELETED_FLG <> 'Y'
  ) AS E, (
    SELECT
      *
    FROM BEC_ODS.WIP_REQUIREMENT_OPERATIONS
    WHERE
      IS_DELETED_FLG <> 'Y'
  ) AS WRO, (
    SELECT
      *
    FROM BEC_ODS.FND_USER
    WHERE
      IS_DELETED_FLG <> 'Y'
  ) AS U, (
    SELECT
      *
    FROM BEC_ODS.MTL_SYSTEM_ITEMS_B
    WHERE
      IS_DELETED_FLG <> 'Y'
  ) AS M, (
    SELECT
      *
    FROM BEC_ODS.WIP_SCHEDULE_GROUPS
    WHERE
      IS_DELETED_FLG <> 'Y'
  ) AS SG, (
    SELECT
      *
    FROM BEC_ODS.ORG_ORGANIZATION_DEFINITIONS
    WHERE
      IS_DELETED_FLG <> 'Y'
  ) AS O, (
    SELECT
      *
    FROM BEC_ODS.FND_LOOKUP_VALUES
    WHERE
      IS_DELETED_FLG <> 'Y'
  ) AS ML, (
    SELECT
      *
    FROM BEC_ODS.FND_LOOKUP_VALUES
    WHERE
      IS_DELETED_FLG <> 'Y'
  ) AS ML2
  WHERE
    1 = 1
    AND W.WIP_ENTITY_ID = P.WIP_ENTITY_ID
    AND W.WIP_ENTITY_ID = E.WIP_ENTITY_ID
    AND W.ORGANIZATION_ID = E.ORGANIZATION_ID
    AND W.PRIMARY_ITEM_ID = M.INVENTORY_ITEM_ID()
    AND W.ORGANIZATION_ID = M.ORGANIZATION_ID()
    AND O.ORGANIZATION_ID = E.ORGANIZATION_ID
    AND O.ORGANIZATION_ID = P.ORGANIZATION_ID
    AND W.WIP_ENTITY_ID = WRO.WIP_ENTITY_ID()
    AND W.ORGANIZATION_ID = WRO.ORGANIZATION_ID()
    AND W.PRIMARY_ITEM_ID = WRO.INVENTORY_ITEM_ID()
    AND W.ORGANIZATION_ID = SG.ORGANIZATION_ID()
    AND W.SCHEDULE_GROUP_ID = SG.SCHEDULE_GROUP_ID()
    AND ML.LANGUAGE = 'US'
    AND ML.LOOKUP_TYPE = 'WIP_CLASS_TYPE'
    AND ML.LOOKUP_CODE = P.CLASS_TYPE
    AND ML2.LANGUAGE = 'US'
    AND ML2.LOOKUP_CODE = W.STATUS_TYPE
    AND ML2.LOOKUP_TYPE = 'WIP_JOB_STATUS'
    AND E.CREATED_BY = U.USER_ID()
    AND (
      W.kca_seq_date > (
        SELECT
          (
            executebegints - prune_days
          )
        FROM bec_etl_ctrl.batch_dw_info
        WHERE
          dw_table_name = 'fact_wip_value' AND batch_name = 'wip'
      )
    ) /*	AND E.ORGANIZATION_ID IN (106) */ /*	AND E.WIP_ENTITY_NAME = 'PRG-141445-B28' 	--'TEC-B3-143893-MG5' */
  GROUP BY
    O.ORGANIZATION_CODE,
    E.WIP_ENTITY_NAME,
    W.CREATION_DATE,
    W.DATE_RELEASED,
    W.STATUS_TYPE,
    M.SEGMENT1,
    M.DESCRIPTION,
    W.CLASS_CODE,
    W.SCHEDULED_START_DATE,
    W.SCHEDULED_COMPLETION_DATE,
    W.START_QUANTITY,
    W.QUANTITY_COMPLETED,
    W.QUANTITY_SCRAPPED,
    W.STATUS_TYPE,
    U.USER_NAME,
    WRO.OPERATION_SEQ_NUM,
    ML.MEANING,
    SG.SCHEDULE_GROUP_NAME,
    W.SCHEDULED_COMPLETION_DATE,
    ML2.MEANING,
    W.WIP_ENTITY_ID,
    W.ORGANIZATION_ID,
    W.DATE_CLOSED,
    W.PRIMARY_ITEM_ID,
    SG.SCHEDULE_GROUP_ID,
    W.NET_QUANTITY
);
UPDATE bec_etl_ctrl.batch_dw_info SET last_refresh_date = CURRENT_TIMESTAMP()
WHERE
  dw_table_name = 'fact_wip_value' AND batch_name = 'wip';