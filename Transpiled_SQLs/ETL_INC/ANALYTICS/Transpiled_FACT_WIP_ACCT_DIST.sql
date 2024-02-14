/* Delete Records */
DELETE FROM gold_bec_dwh.fact_wip_acct_dist
WHERE
  (COALESCE(transaction_id, 0), COALESCE(wip_entity_id, 0), COALESCE(ORGANIZATION_ID, 0), COALESCE(wip_sub_ledger_id, 0), COALESCE(reference_account, 0)) IN (
    SELECT
      COALESCE(ODS.transaction_id, 0) AS transaction_id,
      COALESCE(ODS.wip_entity_id, 0) AS wip_entity_id,
      COALESCE(ODS.ORGANIZATION_ID, 0) AS ORGANIZATION_ID,
      COALESCE(ODS.wip_sub_ledger_id, 0) AS wip_sub_ledger_id,
      COALESCE(ODS.reference_account, 0) AS reference_account
    FROM gold_bec_dwh.fact_wip_acct_dist AS dw, (
      SELECT
        wta.transaction_id,
        wta.wip_entity_id,
        wta.organization_id,
        wta.wip_sub_ledger_id,
        wta.reference_account
      FROM silver_bec_ods.wip_transaction_accounts AS wta, (
        SELECT
          WT.TRANSACTION_ID,
          WT.LAST_UPDATE_DATE,
          WT.ORGANIZATION_ID,
          WT.WIP_ENTITY_ID,
          CASE WHEN WT.TRANSACTION_TYPE = 2 THEN NULL ELSE ML3.MEANING END AS STANDARD_RATE_MEANING,
          WE.WIP_ENTITY_NAME,
          (
            SELECT
              department_code
            FROM silver_bec_ods.bom_departments
            WHERE
              department_id = CASE
                WHEN we.entity_type = 6
                THEN COALESCE(wt.charge_department_id, wt.department_id)
                WHEN we.entity_type = 7
                THEN COALESCE(wt.charge_department_id, wt.department_id)
                ELSE wt.department_id
              END
          ) AS DEPARTMENT_CODE,
          ML1.MEANING AS TRANSACTION_TYPE_MEANING,
          WT.OPERATION_SEQ_NUM,
          WT.RESOURCE_SEQ_NUM,
          ML1.is_deleted_flg,
          ML3.is_deleted_flg AS is_deleted_flg1,
          WT.is_deleted_flg AS is_deleted_flg2,
          WE.is_deleted_flg AS is_deleted_flg3
        FROM BEC_ODS.FND_LOOKUP_VALUES AS ML1, BEC_ODS.FND_LOOKUP_VALUES AS ML3, BEC_ODS.WIP_TRANSACTIONS AS WT, BEC_ODS.WIP_ENTITIES AS WE
        WHERE
          1 = 1
          AND ML1.LOOKUP_TYPE = 'WIP_TRANSACTION_TYPE'
          AND ML1.LOOKUP_CODE = WT.TRANSACTION_TYPE
          AND ML3.LOOKUP_TYPE = 'SYS_YES_NO'
          AND ML3.LOOKUP_CODE = COALESCE(WT.STANDARD_RATE_FLAG, 1)
          AND WT.WIP_ENTITY_ID = WE.WIP_ENTITY_ID
      ) AS wtv, BEC_ODS.wip_discrete_jobs AS w, BEC_ODS.bom_resources AS br
      WHERE
        wta.transaction_id = wtv.transaction_id
        AND wta.organization_id = wtv.organization_id
        AND wta.wip_entity_id = w.WIP_ENTITY_ID()
        AND wta.resource_id = br.RESOURCE_ID()
        AND (
          wta.kca_seq_date > (
            SELECT
              (
                executebegints - prune_days
              )
            FROM bec_etl_ctrl.batch_dw_info
            WHERE
              dw_table_name = 'fact_wip_acct_dist' AND batch_name = 'costing'
          )
          OR wta.IS_DELETED_FLG = 'Y'
          OR wtv.IS_DELETED_FLG = 'Y'
          OR wtv.IS_DELETED_FLG1 = 'Y'
          OR wtv.IS_DELETED_FLG2 = 'Y'
          OR wtv.IS_DELETED_FLG3 = 'Y'
          OR w.IS_DELETED_FLG = 'Y'
          OR br.IS_DELETED_FLG = 'Y'
        )
    ) AS ods
    WHERE
      dw.dw_load_id = (
        SELECT
          system_id
        FROM bec_etl_ctrl.etlsourceappid
        WHERE
          source_system = 'EBS'
      ) || '-' || COALESCE(ODS.transaction_id, 0) || '-' || COALESCE(ODS.wip_entity_id, 0) || '-' || COALESCE(ODS.ORGANIZATION_ID, 0) || '-' || COALESCE(ODS.wip_sub_ledger_id, 0) || '-' || COALESCE(ODS.reference_account, 0)
  );
/* Insert records */
INSERT INTO gold_bec_dwh.fact_wip_acct_dist (
  transaction_id,
  wip_entity_id,
  inventory_item_id,
  organization_id,
  wip_sub_ledger_id,
  reference_account,
  wip_entity_name,
  class_code,
  transaction_date,
  transaction_type_meaning,
  operation_seq_num,
  department_code,
  resource_seq_num,
  resource_code,
  resource_uom,
  SUBINVENTORY,
  basis,
  activity,
  standard_rate_meaning,
  rate_or_amount,
  primary_quantity,
  base_transaction_value,
  is_deleted_flg,
  source_app_id,
  dw_load_id,
  dw_insert_date,
  dw_update_date
)
(
  SELECT
    wta.transaction_id,
    wta.wip_entity_id,
    w.primary_item_id AS inventory_item_id,
    wta.organization_id,
    wta.wip_sub_ledger_id,
    wta.reference_account,
    wtv.wip_entity_name,
    w.class_code,
    wta.transaction_date,
    wtv.transaction_type_meaning,
    wtv.operation_seq_num,
    wtv.department_code,
    wtv.resource_seq_num,
    br.resource_code,
    br.unit_of_measure AS resource_uom,
    W.COMPLETION_SUBINVENTORY AS SUBINVENTORY,
    CASE
      WHEN wta.basis_type = 1
      THEN 'Item'
      WHEN wta.basis_type = 2
      THEN 'Lot'
      WHEN wta.basis_type = 3
      THEN 'Resource Units'
      WHEN wta.basis_type = 4
      THEN 'Resource Value'
      WHEN wta.basis_type = 5
      THEN 'Total Value'
      WHEN wta.basis_type = 6
      THEN 'Activity'
      ELSE CAST(wta.basis_type AS STRING)
    END AS basis,
    CASE
      WHEN wta.activity_id = 1
      THEN 'Run'
      WHEN wta.activity_id = 2
      THEN 'Prerun'
      WHEN wta.activity_id = 3
      THEN 'Postrun'
      WHEN wta.activity_id = 4
      THEN 'Move'
      WHEN wta.activity_id = 5
      THEN 'Queue'
      ELSE CAST(wta.activity_id AS STRING)
    END AS activity,
    wtv.standard_rate_meaning,
    wta.rate_or_amount AS rate_or_amount,
    wta.primary_quantity,
    wta.base_transaction_value,
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
    ) || '-' || COALESCE(wta.transaction_id, 0) || '-' || COALESCE(wta.wip_entity_id, 0) || '-' || COALESCE(wta.ORGANIZATION_ID, 0) || '-' || COALESCE(wta.wip_sub_ledger_id, 0) || '-' || COALESCE(wta.reference_account, 0) AS dw_load_id,
    CURRENT_TIMESTAMP() AS dw_insert_date,
    CURRENT_TIMESTAMP() AS dw_update_date
  FROM (
    SELECT
      *
    FROM silver_bec_ods.wip_transaction_accounts
    WHERE
      is_deleted_flg <> 'Y'
  ) AS wta, (
    SELECT
      WT.TRANSACTION_ID,
      WT.LAST_UPDATE_DATE,
      WT.ORGANIZATION_ID,
      WT.WIP_ENTITY_ID,
      CASE WHEN WT.TRANSACTION_TYPE = 2 THEN NULL ELSE ML3.MEANING END AS STANDARD_RATE_MEANING,
      WE.WIP_ENTITY_NAME,
      (
        SELECT
          department_code
        FROM (
          SELECT
            *
          FROM silver_bec_ods.bom_departments
          WHERE
            is_deleted_flg <> 'Y'
        )
        WHERE
          department_id = CASE
            WHEN we.entity_type = 6
            THEN COALESCE(wt.charge_department_id, wt.department_id)
            WHEN we.entity_type = 7
            THEN COALESCE(wt.charge_department_id, wt.department_id)
            ELSE wt.department_id
          END
      ) AS DEPARTMENT_CODE,
      ML1.MEANING AS TRANSACTION_TYPE_MEANING,
      WT.OPERATION_SEQ_NUM,
      WT.RESOURCE_SEQ_NUM
    FROM (
      SELECT
        *
      FROM BEC_ODS.FND_LOOKUP_VALUES
      WHERE
        is_deleted_flg <> 'Y'
    ) AS ML1, (
      SELECT
        *
      FROM BEC_ODS.FND_LOOKUP_VALUES
      WHERE
        is_deleted_flg <> 'Y'
    ) AS ML3, (
      SELECT
        *
      FROM BEC_ODS.WIP_TRANSACTIONS
      WHERE
        is_deleted_flg <> 'Y'
    ) AS WT, (
      SELECT
        *
      FROM BEC_ODS.WIP_ENTITIES
      WHERE
        is_deleted_flg <> 'Y'
    ) AS WE
    WHERE
      1 = 1
      AND ML1.LOOKUP_TYPE = 'WIP_TRANSACTION_TYPE'
      AND ML1.LOOKUP_CODE = WT.TRANSACTION_TYPE
      AND ML3.LOOKUP_TYPE = 'SYS_YES_NO'
      AND ML3.LOOKUP_CODE = COALESCE(WT.STANDARD_RATE_FLAG, 1)
      AND WT.WIP_ENTITY_ID = WE.WIP_ENTITY_ID
  ) AS wtv, (
    SELECT
      *
    FROM BEC_ODS.wip_discrete_jobs
    WHERE
      is_deleted_flg <> 'Y'
  ) AS w, (
    SELECT
      *
    FROM BEC_ODS.bom_resources
    WHERE
      is_deleted_flg <> 'Y'
  ) AS br
  WHERE
    wta.transaction_id = wtv.transaction_id
    AND wta.organization_id = wtv.organization_id
    AND wta.wip_entity_id = w.WIP_ENTITY_ID()
    AND wta.resource_id = br.RESOURCE_ID()
    AND (
      wta.kca_seq_date > (
        SELECT
          (
            executebegints - prune_days
          )
        FROM bec_etl_ctrl.batch_dw_info
        WHERE
          dw_table_name = 'fact_wip_acct_dist' AND batch_name = 'costing'
      )
    )
);
UPDATE bec_etl_ctrl.batch_dw_info SET last_refresh_date = CURRENT_TIMESTAMP()
WHERE
  dw_table_name = 'fact_wip_acct_dist' AND batch_name = 'costing';