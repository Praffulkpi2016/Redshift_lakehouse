DROP table IF EXISTS gold_bec_dwh.fact_wip_acct_dist;
CREATE TABLE gold_bec_dwh.fact_wip_acct_dist AS
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
);
UPDATE bec_etl_ctrl.batch_dw_info SET load_type = 'I', last_refresh_date = CURRENT_TIMESTAMP()
WHERE
  dw_table_name = 'fact_wip_acct_dist' AND batch_name = 'costing';