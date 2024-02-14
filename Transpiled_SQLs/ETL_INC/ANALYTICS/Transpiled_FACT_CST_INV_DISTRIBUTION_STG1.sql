TRUNCATE table gold_bec_dwh.fact_cst_inv_distribution_stg1_tmp;
/* Insert records into temp table */
INSERT INTO gold_bec_dwh.fact_cst_inv_distribution_stg1_tmp
(
  SELECT DISTINCT
    transaction_id,
    inventory_item_id,
    organization_id
  FROM silver_bec_ods.mtl_transaction_accounts AS mta
  WHERE
    1 = 1
    AND COALESCE(mta.kca_seq_date, '2020-01-01 12:00:00.000') > (
      SELECT
        (
          executebegints - prune_days
        )
      FROM bec_etl_ctrl.batch_dw_info
      WHERE
        dw_table_name = 'fact_cst_inv_distribution_stg1' AND batch_name = 'inv'
    )
);
/* delete records from fact table */
DELETE FROM gold_bec_dwh.fact_cst_inv_distribution_stg1
WHERE
  EXISTS(
    SELECT
      1
    FROM gold_bec_dwh.fact_cst_inv_distribution_stg1_tmp AS tmp
    WHERE
      tmp.transaction_id = fact_cst_inv_distribution_stg1.transaction_id
      AND tmp.inventory_item_id = fact_cst_inv_distribution_stg1.inventory_item_id
      AND tmp.organization_id = fact_cst_inv_distribution_stg1.organization_id
  );
/* Insert records into fact table */
INSERT INTO gold_bec_dwh.fact_cst_inv_distribution_stg1
(
  SELECT DISTINCT
    transaction_organization_id,
    transaction_id,
    organization_id,
    inventory_item_id,
    revision,
    subinventory_code,
    locator_id,
    transaction_type_id,
    transaction_source_type_id,
    transaction_source_id,
    transaction_source_name,
    transaction_date,
    transaction_quantity,
    transaction_uom,
    primary_quantity,
    operation_seq_num,
    currency_code,
    currency_conversion_date,
    currency_conversion_type,
    currency_conversion_rate,
    department_id,
    reason_id,
    transaction_reference,
    reference_account,
    accounting_line_type,
    transaction_value,
    base_transaction_value,
    basis_type,
    cost_element_id,
    activity_id,
    rate_or_amount,
    gl_batch_id,
    resource_id,
    unit_cost,
    last_update_date,
    last_updated_by,
    creation_date,
    created_by,
    last_update_login,
    request_id,
    program_application_id,
    program_id,
    program_update_date,
    mta_transaction_date,
    parent_transaction_id,
    logical_trx_type_code,
    'N' AS Update_flg,
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
    ) || '-' || COALESCE(transaction_id, 0) || '-' || COALESCE(REFERENCE_ACCOUNT, 0) || '-' || COALESCE(COST_ELEMENT_ID, 0) || '-' || COALESCE(accounting_line_type, 0) || '-' || COALESCE(primary_quantity, 0) || '-' || COALESCE(unit_cost, 0) AS dw_load_id,
    CURRENT_TIMESTAMP() AS dw_insert_date,
    CURRENT_TIMESTAMP() AS dw_update_date
  FROM (
    SELECT DISTINCT
      mmt.organization_id AS TRANSACTION_ORGANIZATION_ID,
      mta.transaction_id AS TRANSACTION_ID,
      MTA.ORGANIZATION_ID AS ORGANIZATION_ID,
      MMT.INVENTORY_ITEM_ID AS INVENTORY_ITEM_ID,
      MMT.REVISION AS REVISION,
      MMT.SUBINVENTORY_CODE AS SUBINVENTORY_CODE,
      MMT.LOCATOR_ID AS LOCATOR_ID,
      MMT.TRANSACTION_TYPE_ID AS TRANSACTION_TYPE_ID,
      MTA.TRANSACTION_SOURCE_TYPE_ID AS TRANSACTION_SOURCE_TYPE_ID,
      MMT.TRANSACTION_SOURCE_ID AS TRANSACTION_SOURCE_ID,
      MMT.TRANSACTION_SOURCE_NAME AS TRANSACTION_SOURCE_NAME,
      MMT.TRANSACTION_DATE AS TRANSACTION_DATE,
      MMT.TRANSACTION_QUANTITY AS TRANSACTION_QUANTITY,
      MMT.TRANSACTION_UOM AS TRANSACTION_UOM,
      CASE
        WHEN MTA.ACCOUNTING_LINE_TYPE = 1
        THEN MTA.PRIMARY_QUANTITY
        WHEN MTA.ACCOUNTING_LINE_TYPE = 14
        THEN MTA.PRIMARY_QUANTITY
        WHEN MTA.ACCOUNTING_LINE_TYPE = 3
        THEN MTA.PRIMARY_QUANTITY
        ELSE CASE
          WHEN SIGN(COALESCE(MTA.BASE_TRANSACTION_VALUE, 0)) = -1
          OR (
            SIGN(COALESCE(MTA.BASE_TRANSACTION_VALUE, 0)) IS NULL AND -1 IS NULL
          )
          THEN -1 * ABS(MTA.PRIMARY_QUANTITY)
          ELSE MTA.Primary_quantity
        END
      END AS PRIMARY_QUANTITY,
      MMT.OPERATION_SEQ_NUM AS OPERATION_SEQ_NUM,
      COALESCE(MTA.CURRENCY_CODE, MMT.CURRENCY_CODE) AS CURRENCY_CODE,
      COALESCE(MTA.CURRENCY_CONVERSION_DATE, MMT.CURRENCY_CONVERSION_DATE) AS CURRENCY_CONVERSION_DATE,
      MTA.CURRENCY_CONVERSION_TYPE AS CURRENCY_CONVERSION_TYPE,
      COALESCE(MTA.CURRENCY_CONVERSION_RATE, MMT.CURRENCY_CONVERSION_RATE) AS CURRENCY_CONVERSION_RATE,
      MMT.DEPARTMENT_ID AS DEPARTMENT_ID,
      MMT.REASON_ID AS REASON_ID,
      MMT.TRANSACTION_REFERENCE AS TRANSACTION_REFERENCE,
      MTA.REFERENCE_ACCOUNT AS REFERENCE_ACCOUNT,
      MTA.ACCOUNTING_LINE_TYPE AS ACCOUNTING_LINE_TYPE,
      MTA.TRANSACTION_VALUE AS TRANSACTION_VALUE,
      MTA.BASE_TRANSACTION_VALUE AS BASE_TRANSACTION_VALUE,
      MTA.BASIS_TYPE AS BASIS_TYPE,
      MTA.COST_ELEMENT_ID AS COST_ELEMENT_ID,
      MTA.ACTIVITY_ID AS ACTIVITY_ID,
      MTA.RATE_OR_AMOUNT AS RATE_OR_AMOUNT,
      MTA.GL_BATCH_ID AS GL_BATCH_ID,
      MTA.RESOURCE_ID AS RESOURCE_ID,
      CASE
        WHEN MMT.TRANSACTION_TYPE_ID = 24
        THEN NULL
        WHEN MMT.TRANSACTION_TYPE_ID = 80
        THEN NULL
        ELSE CASE
          WHEN MTA.PRIMARY_QUANTITY = 0
          THEN NULL
          WHEN MTA.PRIMARY_QUANTITY IS NULL
          THEN NULL
          ELSE MTA.RATE_OR_AMOUNT
        END
      END AS UNIT_COST,
      MTA.LAST_UPDATE_DATE,
      MTA.LAST_UPDATED_BY,
      MTA.CREATION_DATE,
      MTA.CREATED_BY,
      MTA.LAST_UPDATE_LOGIN,
      MTA.REQUEST_ID,
      MTA.PROGRAM_APPLICATION_ID,
      MTA.PROGRAM_ID,
      MTA.PROGRAM_UPDATE_DATE,
      MTA.TRANSACTION_DATE AS MTA_TRANSACTION_DATE,
      MMT.PARENT_TRANSACTION_ID,
      MMT.LOGICAL_TRX_TYPE_CODE
    FROM (
      SELECT
        *
      FROM silver_bec_ods.MTL_TRANSACTION_ACCOUNTS
      WHERE
        is_deleted_flg <> 'Y'
    ) AS MTA, (
      SELECT
        *
      FROM silver_bec_ods.MTL_MATERIAL_TRANSACTIONS
      WHERE
        is_deleted_flg <> 'Y'
    ) AS MMT, (
      SELECT
        *
      FROM silver_bec_ods.MTL_PARAMETERS
      WHERE
        is_deleted_flg <> 'Y'
    ) AS MP, (
      SELECT
        *
      FROM silver_bec_ods.MTL_PARAMETERS
      WHERE
        is_deleted_flg <> 'Y'
    ) AS MP1, gold_bec_dwh.fact_cst_inv_distribution_stg1_tmp AS tmp
    WHERE
      mta.transaction_id = mmt.transaction_id
      AND (
        NOT mmt.transaction_action_id IN (2, 28, 5, 3)
        OR (
          mmt.transaction_action_id IN (2, 28, 5)
          AND mmt.primary_quantity < 0
          AND (
            (
              (
                mmt.transaction_type_id <> 68 OR mta.accounting_line_type <> 13
              )
              AND mmt.primary_quantity = mta.primary_quantity
            )
            OR (
              mp.primary_cost_method <> 1
              AND mmt.transaction_type_id = 68
              AND mta.accounting_line_type = 13
              AND (
                (
                  mmt.cost_group_id <> mmt.transfer_cost_group_id
                  AND mmt.primary_quantity = CASE
                    WHEN SIGN(mmt.primary_quantity) = -1
                    OR (
                      SIGN(mmt.primary_quantity) IS NULL AND -1 IS NULL
                    )
                    THEN mmt.primary_quantity
                    ELSE NULL
                  END
                )
                OR (
                  mmt.cost_group_id = mmt.transfer_cost_group_id
                  AND mmt.primary_quantity = -1 * mta.primary_quantity
                )
              )
            )
            OR (
              mp.primary_cost_method = 1
              AND mmt.transaction_type_id = 68
              AND mta.accounting_line_type = 13
              AND (
                (
                  mmt.cost_group_id <> mmt.transfer_cost_group_id
                  AND mmt.project_id = mta.transaction_source_id
                )
                OR (
                  mmt.cost_group_id = mmt.transfer_cost_group_id
                  AND mmt.primary_quantity = -1 * mta.primary_quantity
                )
              )
            )
          )
        )
        OR (
          mmt.transaction_action_id = 3
          AND mp.primary_cost_method = 1
          AND mp1.primary_cost_method = 1
          AND mmt.organization_id = mta.organization_id
        )
        OR (
          mmt.transaction_action_id = 3
          AND (
            mp.primary_cost_method <> 1 OR mp1.primary_cost_method <> 1
          )
        )
      )
      AND MP.ORGANIZATION_ID = MMT.ORGANIZATION_ID
      AND MP1.ORGANIZATION_ID() = MMT.TRANSFER_ORGANIZATION_ID
      AND mta.inventory_item_id = mmt.inventory_item_id
      AND mta.transaction_date BETWEEN FLOOR(mmt.transaction_date) AND FLOOR(mmt.transaction_date) + 0.99999
      AND mta.transaction_id = tmp.transaction_id
      AND mta.inventory_item_id = tmp.inventory_item_id
      AND mta.organization_id = tmp.organization_id
    UNION ALL
    SELECT DISTINCT
      mmt.organization_id AS TRANSACTION_ORGANIZATION_ID,
      mta.transaction_id AS TRANSACTION_ID,
      MTA.ORGANIZATION_ID AS ORGANIZATION_ID,
      MMT.INVENTORY_ITEM_ID AS INVENTORY_ITEM_ID,
      MMT.REVISION AS REVISION,
      MMT.SUBINVENTORY_CODE,
      MMT.LOCATOR_ID,
      MMT.TRANSACTION_TYPE_ID,
      MMT.TRANSACTION_SOURCE_TYPE_ID,
      MMT.TRANSACTION_SOURCE_ID,
      MMT.TRANSACTION_SOURCE_NAME,
      MMT.TRANSACTION_DATE,
      MMT.TRANSACTION_QUANTITY,
      MMT.TRANSACTION_UOM,
      CASE
        WHEN MTA.ACCOUNTING_LINE_TYPE = 1
        THEN MTA.PRIMARY_QUANTITY
        WHEN MTA.ACCOUNTING_LINE_TYPE = 14
        THEN MTA.PRIMARY_QUANTITY
        WHEN MTA.ACCOUNTING_LINE_TYPE = 3
        THEN MTA.PRIMARY_QUANTITY
        ELSE CASE
          WHEN SIGN(COALESCE(MTA.BASE_TRANSACTION_VALUE, 0)) = -1
          OR (
            SIGN(COALESCE(MTA.BASE_TRANSACTION_VALUE, 0)) IS NULL AND -1 IS NULL
          )
          THEN -1 * ABS(MTA.PRIMARY_QUANTITY)
          ELSE MTA.PRIMARY_QUANTITY
        END
      END AS PRIMARY_QUANTITY,
      MMT.OPERATION_SEQ_NUM AS OPERATION_SEQ_NUM,
      COALESCE(MTA.CURRENCY_CODE, MMT.CURRENCY_CODE) AS CURRENCY_CODE,
      COALESCE(MTA.CURRENCY_CONVERSION_DATE, MMT.CURRENCY_CONVERSION_DATE) AS CURRENCY_CONVERSION_DATE,
      MTA.CURRENCY_CONVERSION_TYPE AS CURRENCY_CONVERSION_TYPE,
      COALESCE(MTA.CURRENCY_CONVERSION_RATE, MMT.CURRENCY_CONVERSION_RATE) AS CURRENCY_CONVERSION_RATE,
      MMT.DEPARTMENT_ID,
      MMT.REASON_ID,
      MMT.TRANSACTION_REFERENCE,
      MTA.REFERENCE_ACCOUNT,
      MTA.ACCOUNTING_LINE_TYPE,
      MTA.TRANSACTION_VALUE,
      MTA.BASE_TRANSACTION_VALUE,
      MTA.BASIS_TYPE,
      MTA.COST_ELEMENT_ID,
      MTA.ACTIVITY_ID,
      MTA.RATE_OR_AMOUNT,
      MTA.GL_BATCH_ID,
      MTA.RESOURCE_ID,
      CASE
        WHEN MMT.TRANSACTION_TYPE_ID = 24
        THEN NULL
        WHEN MMT.TRANSACTION_TYPE_ID = 80
        THEN NULL
        ELSE CASE
          WHEN MTA.PRIMARY_QUANTITY = 0
          THEN NULL
          WHEN MTA.PRIMARY_QUANTITY IS NULL
          THEN NULL
          ELSE MTA.RATE_OR_AMOUNT
        END
      END AS UNIT_COST,
      MTA.LAST_UPDATE_DATE,
      MTA.LAST_UPDATED_BY,
      MTA.CREATION_DATE,
      MTA.CREATED_BY,
      MTA.LAST_UPDATE_LOGIN,
      MTA.REQUEST_ID,
      MTA.PROGRAM_APPLICATION_ID,
      MTA.PROGRAM_ID,
      MTA.PROGRAM_UPDATE_DATE,
      MTA.TRANSACTION_DATE AS MTA_TRANSACTION_DATE,
      MMT.PARENT_TRANSACTION_ID,
      MMT.LOGICAL_TRX_TYPE_CODE
    FROM (
      SELECT
        *
      FROM silver_bec_ods.MTL_TRANSACTION_ACCOUNTS
      WHERE
        is_deleted_flg <> 'Y'
    ) AS MTA, (
      SELECT
        *
      FROM silver_bec_ods.MTL_MATERIAL_TRANSACTIONS
      WHERE
        is_deleted_flg <> 'Y'
    ) AS MMT, (
      SELECT
        *
      FROM silver_bec_ods.MTL_PARAMETERS
      WHERE
        is_deleted_flg <> 'Y'
    ) AS MP, (
      SELECT
        *
      FROM silver_bec_ods.MTL_PARAMETERS
      WHERE
        is_deleted_flg <> 'Y'
    ) AS MP1, gold_bec_dwh.fact_cst_inv_distribution_stg1_tmp AS tmp
    WHERE
      mta.transaction_id = mmt.transfer_transaction_id
      AND (
        (
          mmt.transaction_action_id IN (2, 28, 5)
          AND mmt.primary_quantity > 0
          AND (
            (
              (
                mmt.transaction_type_id <> 68 OR mta.accounting_line_type <> 13
              )
              AND mmt.primary_quantity = mta.primary_quantity
            )
            OR (
              mp.primary_cost_method <> 1
              AND mmt.transaction_type_id = 68
              AND mta.accounting_line_type = 13
              AND (
                (
                  mmt.cost_group_id <> mmt.transfer_cost_group_id
                  AND mmt.primary_quantity = CASE
                    WHEN SIGN(mmt.primary_quantity) = -1
                    OR (
                      SIGN(mmt.primary_quantity) IS NULL AND -1 IS NULL
                    )
                    THEN mmt.primary_quantity
                    ELSE NULL
                  END
                )
                OR (
                  mmt.cost_group_id = mmt.transfer_cost_group_id
                  AND mmt.primary_quantity = -1 * mta.primary_quantity
                )
              )
            )
            OR (
              mp.primary_cost_method = 1
              AND mmt.transaction_type_id = 68
              AND mta.accounting_line_type = 13
              AND (
                (
                  mmt.cost_group_id <> mmt.transfer_cost_group_id
                  AND mmt.project_id = mta.transaction_source_id
                )
                OR (
                  mmt.cost_group_id = mmt.transfer_cost_group_id
                  AND mmt.primary_quantity = -1 * mta.primary_quantity
                )
              )
            )
          )
        )
        OR (
          mmt.transaction_action_id = 3
          AND mp.primary_cost_method = 1
          AND mp1.primary_cost_method = 1
          AND mmt.organization_id = mta.organization_id
        )
      )
      AND MP.ORGANIZATION_ID = MMT.ORGANIZATION_ID
      AND MP1.ORGANIZATION_ID() = MMT.TRANSFER_ORGANIZATION_ID
      AND mta.inventory_item_id = mmt.inventory_item_id
      AND mta.transaction_date BETWEEN FLOOR(mmt.transaction_date) AND FLOOR(mmt.transaction_date) + 0.99999
      AND mta.transaction_id = tmp.transaction_id
      AND mta.inventory_item_id = tmp.inventory_item_id
      AND mta.organization_id = tmp.organization_id
  )
);
UPDATE bec_etl_ctrl.batch_dw_info SET load_type = 'I', last_refresh_date = CURRENT_TIMESTAMP()
WHERE
  dw_table_name = 'fact_cst_inv_distribution_stg1' AND batch_name = 'inv';