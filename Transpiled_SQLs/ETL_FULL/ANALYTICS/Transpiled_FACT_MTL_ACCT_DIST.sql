DROP table IF EXISTS gold_bec_dwh.FACT_MTL_ACCT_DIST;
CREATE TABLE gold_bec_dwh.fact_mtl_acct_dist AS
(
  SELECT
    Transaction_id,
    organization_id,
    (
      SELECT
        system_id
      FROM bec_etl_ctrl.etlsourceappid
      WHERE
        source_system = 'EBS'
    ) || '-' || COALESCE(organization_id, 0) AS organization_id_key,
    inv_sub_ledger_id,
    inventory_item_id,
    (
      SELECT
        system_id
      FROM bec_etl_ctrl.etlsourceappid
      WHERE
        source_system = 'EBS'
    ) || '-' || COALESCE(inventory_item_id, 0) AS inventory_item_id_key,
    BOM_department_id,
    (
      SELECT
        system_id
      FROM bec_etl_ctrl.etlsourceappid
      WHERE
        source_system = 'EBS'
    ) || '-' || COALESCE(BOM_department_id, 0) AS BOM_department_id_key,
    bom_resource_id,
    (
      SELECT
        system_id
      FROM bec_etl_ctrl.etlsourceappid
      WHERE
        source_system = 'EBS'
    ) || '-' || COALESCE(bom_resource_id, 0) AS bom_resource_id_key,
    item,
    item_description,
    revision,
    subinventory_code,
    locator_id,
    transaction_type_id,
    (
      SELECT
        system_id
      FROM bec_etl_ctrl.etlsourceappid
      WHERE
        source_system = 'EBS'
    ) || '-' || COALESCE(transaction_type_id, 0) AS transaction_type_id_key,
    transaction_type_name,
    transaction_source_type_id,
    (
      SELECT
        system_id
      FROM bec_etl_ctrl.etlsourceappid
      WHERE
        source_system = 'EBS'
    ) || '-' || COALESCE(transaction_source_type_id, 0) AS transaction_source_type_id_key,
    transaction_source_type_name,
    transaction_source_id,
    transaction_source_name,
    transaction_date,
    transaction_quantity,
    transaction_uom,
    PRIMARY_QUANTITY,
    primary_uom,
    operation_seq_num,
    CURRENCY_CODE,
    CURRENCY_CONVERSION_DATE,
    CURRENCY_CONVERSION_TYPE,
    CURRENCY_CONVERSION_RATE,
    department_code,
    reason_name,
    transaction_reference,
    reference_account,
    accounting_line_type,
    LINE_TYPE_NAME,
    transaction_value,
    base_transaction_value,
    BASIS_TYPE_NAME,
    cost_element_id,
    activity,
    rate_or_amount,
    gl_batch_id,
    resource_code,
    UNIT_COST,
    parent_transaction_id,
    organization_code,
    LOGICAL_TRX_TYPE,
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
    ) || '-' || COALESCE(transaction_id, 0) || '-' || COALESCE(INV_SUB_LEDGER_ID, 0) || '-' || COALESCE(cost_element_id, 0) || '-' || COALESCE(reference_account, 0) || '-' || COALESCE(resource_code, 'NA') AS dw_load_id,
    CURRENT_TIMESTAMP() AS dw_insert_date,
    CURRENT_TIMESTAMP() AS dw_update_date
  FROM (
    SELECT
      mta.transaction_id,
      mta.organization_id,
      mta.inv_sub_ledger_id,
      mmt.inventory_item_id,
      mmt.department_id AS BOM_department_id,
      mta.resource_id AS bom_resource_id,
      msi.segment1 AS item,
      REPLACE(
        REPLACE(REPLACE(REPLACE(msi.description, '&', ' AND '), '<', ' '), '>', ' '),
        'ï¿½',
        'DEG '
      ) AS item_description,
      mmt.revision,
      COALESCE(
        REPLACE(
          REPLACE(REPLACE(REPLACE(mmt.subinventory_code, '&', '^%$'), '<', ' '), '>', ' '),
          'ï¿½',
          'DEG '
        ),
        '.'
      ) AS subinventory_code,
      mmt.locator_id,
      mmt.transaction_type_id,
      mtt.transaction_type_name,
      mta.transaction_source_type_id,
      mtst.transaction_source_type_name,
      mmt.transaction_source_id,
      mmt.transaction_source_name,
      mmt.transaction_date,
      mmt.transaction_quantity,
      mmt.transaction_uom,
      CASE
        WHEN mta.accounting_line_type = 1
        THEN mta.primary_quantity
        WHEN mta.accounting_line_type = 14
        THEN mta.primary_quantity
        WHEN mta.accounting_line_type = 3
        THEN mta.primary_quantity
        ELSE CASE
          WHEN SIGN(COALESCE(mta.base_transaction_value, 0)) = -1
          OR (
            SIGN(COALESCE(mta.base_transaction_value, 0)) IS NULL AND -1 IS NULL
          )
          THEN -1 * ABS(mta.primary_quantity)
          ELSE mta.primary_quantity
        END
      END AS PRIMARY_QUANTITY,
      msi.primary_uom_code AS primary_uom,
      mmt.operation_seq_num,
      COALESCE(mta.currency_code, mmt.currency_code) AS CURRENCY_CODE,
      COALESCE(mta.currency_conversion_date, mmt.currency_conversion_date) AS CURRENCY_CONVERSION_DATE,
      glt.user_conversion_type AS CURRENCY_CONVERSION_TYPE,
      COALESCE(mta.currency_conversion_rate, mmt.currency_conversion_rate) AS CURRENCY_CONVERSION_RATE,
      bd.department_code,
      mtr.reason_name,
      mmt.transaction_reference,
      mta.reference_account,
      mta.accounting_line_type,
      lu1.meaning AS LINE_TYPE_NAME,
      mta.transaction_value,
      mta.base_transaction_value,
      lu2.meaning AS BASIS_TYPE_NAME,
      mta.cost_element_id,
      ca.activity,
      (
        COALESCE(
          ABS(mta.rate_or_amount),
          (
            mta.base_transaction_value / COALESCE(mta.primary_quantity, 1)
          )
        )
      ) AS rate_or_amount,
      mta.gl_batch_id,
      br.resource_code,
      CASE
        WHEN mmt.transaction_type_id = 24
        THEN NULL
        WHEN mmt.transaction_type_id = 80
        THEN NULL
        ELSE CASE
          WHEN mta.primary_quantity = 0
          THEN NULL
          WHEN mta.primary_quantity IS NULL
          THEN NULL
          ELSE mta.rate_or_amount
        END
      END AS UNIT_COST,
      mmt.parent_transaction_id,
      mp2.organization_code,
      lu3.meaning AS LOGICAL_TRX_TYPE
    FROM (
      SELECT
        *
      FROM silver_bec_ods.mtl_transaction_accounts
      WHERE
        is_deleted_flg <> 'Y'
    ) AS mta, (
      SELECT
        *
      FROM silver_bec_ods.mtl_material_transactions
      WHERE
        is_deleted_flg <> 'Y'
    ) AS mmt, (
      SELECT
        *
      FROM silver_bec_ods.mtl_parameters
      WHERE
        is_deleted_flg <> 'Y'
    ) AS mp, (
      SELECT
        *
      FROM silver_bec_ods.mtl_parameters
      WHERE
        is_deleted_flg <> 'Y'
    ) AS mp1, (
      SELECT
        *
      FROM silver_bec_ods.mtl_parameters
      WHERE
        is_deleted_flg <> 'Y'
    ) AS mp2, (
      SELECT
        *
      FROM silver_bec_ods.cst_activities
      WHERE
        is_deleted_flg <> 'Y'
    ) AS ca, (
      SELECT
        *
      FROM silver_bec_ods.mtl_system_items_b
      WHERE
        is_deleted_flg <> 'Y'
    ) AS msi, (
      SELECT
        *
      FROM silver_bec_ods.bom_departments
      WHERE
        is_deleted_flg <> 'Y'
    ) AS bd, (
      SELECT
        *
      FROM silver_bec_ods.bom_resources
      WHERE
        is_deleted_flg <> 'Y'
    ) AS br, (
      SELECT
        *
      FROM silver_bec_ods.mtl_transaction_reasons
      WHERE
        is_deleted_flg <> 'Y'
    ) AS mtr, (
      SELECT
        *
      FROM silver_bec_ods.mtl_txn_source_types
      WHERE
        is_deleted_flg <> 'Y'
    ) AS mtst, (
      SELECT
        *
      FROM silver_bec_ods.mtl_transaction_types
      WHERE
        is_deleted_flg <> 'Y'
    ) AS mtt, (
      SELECT
        *
      FROM silver_bec_ods.FND_LOOKUP_VALUES
      WHERE
        is_deleted_flg <> 'Y'
    ) AS lu1, (
      SELECT
        *
      FROM silver_bec_ods.FND_LOOKUP_VALUES
      WHERE
        is_deleted_flg <> 'Y'
    ) AS lu2, (
      SELECT
        *
      FROM silver_bec_ods.FND_LOOKUP_VALUES
      WHERE
        is_deleted_flg <> 'Y'
    ) AS lu3, (
      SELECT
        *
      FROM silver_bec_ods.gl_daily_conversion_types
      WHERE
        is_deleted_flg <> 'Y'
    ) AS glt
    WHERE
      1 = 1
      AND mta.transaction_id = mmt.transaction_id
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
      AND msi.inventory_item_id = mmt.inventory_item_id
      AND msi.organization_id = mmt.organization_id
      AND mp.organization_id = mmt.organization_id
      AND mp1.ORGANIZATION_ID() = mmt.transfer_organization_id
      AND mp2.organization_id = mta.organization_id
      AND mtt.transaction_type_id = mmt.transaction_type_id
      AND mtst.transaction_source_type_id = mta.transaction_source_type_id
      AND bd.DEPARTMENT_ID() = mmt.department_id
      AND mtr.REASON_ID() = mmt.reason_id
      AND br.RESOURCE_ID() = mta.resource_id
      AND lu1.lookup_type = 'CST_ACCOUNTING_LINE_TYPE'
      AND lu1.lookup_code = mta.accounting_line_type
      AND lu2.LOOKUP_CODE() = mta.basis_type
      AND lu2.LOOKUP_TYPE() = 'CST_BASIS_SHORT'
      AND lu3.LOOKUP_TYPE() = 'MTL_LOGICAL_TRANSACTION_CODE'
      AND lu3.LOOKUP_CODE() = mmt.logical_trx_type_code
      AND glt.CONVERSION_TYPE() = mta.currency_conversion_type
      AND ca.ACTIVITY_ID() = mta.activity_id
      AND mta.inventory_item_id = mmt.inventory_item_id
    UNION ALL
    SELECT
      mta.transaction_id,
      mta.organization_id,
      mta.inv_sub_ledger_id,
      mmt.inventory_item_id,
      mmt.department_id AS BOM_department_id,
      mta.resource_id AS bom_resource_id,
      msi.segment1 AS item,
      REPLACE(
        REPLACE(REPLACE(REPLACE(msi.description, '&', ' AND '), '<', ' '), '>', ' '),
        'ï¿½',
        'DEG '
      ) AS item_description,
      mmt.revision,
      COALESCE(
        REPLACE(
          REPLACE(REPLACE(REPLACE(mmt.subinventory_code, '&', '^%$'), '<', ' '), '>', ' '),
          'ï¿½',
          'DEG '
        ),
        '.'
      ) AS subinventory_code,
      mmt.locator_id,
      mmt.transaction_type_id,
      mtt.transaction_type_name,
      mmt.transaction_source_type_id,
      mtst.transaction_source_type_name,
      mmt.transaction_source_id,
      mmt.transaction_source_name,
      mmt.transaction_date,
      mmt.transaction_quantity,
      mmt.transaction_uom,
      CASE
        WHEN mta.accounting_line_type = 1
        THEN mta.primary_quantity
        WHEN mta.accounting_line_type = 14
        THEN mta.primary_quantity
        WHEN mta.accounting_line_type = 3
        THEN mta.primary_quantity
        ELSE CASE
          WHEN SIGN(COALESCE(mta.base_transaction_value, 0)) = -1
          OR (
            SIGN(COALESCE(mta.base_transaction_value, 0)) IS NULL AND -1 IS NULL
          )
          THEN -1 * ABS(mta.primary_quantity)
          ELSE mta.primary_quantity
        END
      END AS PRIMARY_QUANTITY,
      msi.primary_uom_code AS primary_uom,
      mmt.operation_seq_num,
      COALESCE(mta.currency_code, mmt.currency_code) AS CURRENCY_CODE,
      COALESCE(mta.currency_conversion_date, mmt.currency_conversion_date) AS CURRENCY_CONVERSION_DATE,
      glt.user_conversion_type AS CURRENCY_CONVERSION_TYPE,
      COALESCE(mta.currency_conversion_rate, mmt.currency_conversion_rate) AS CURRENCY_CONVERSION_RATE,
      bd.department_code,
      mtr.reason_name,
      mmt.transaction_reference,
      mta.reference_account,
      mta.accounting_line_type,
      lu1.meaning AS LINE_TYPE_NAME,
      mta.transaction_value,
      mta.base_transaction_value,
      lu2.meaning AS BASIS_TYPE_NAME,
      mta.cost_element_id,
      ca.activity,
      (
        COALESCE(
          ABS(mta.rate_or_amount),
          (
            mta.base_transaction_value / COALESCE(mta.primary_quantity, 1)
          )
        )
      ) AS rate_or_amount,
      mta.gl_batch_id,
      br.resource_code,
      CASE
        WHEN mmt.transaction_type_id = 24
        THEN NULL
        WHEN mmt.transaction_type_id = 80
        THEN NULL
        ELSE CASE
          WHEN mta.primary_quantity = 0
          THEN NULL
          WHEN mta.primary_quantity IS NULL
          THEN NULL /* Commented and replaced for bug 8543247 - decode(MTA.ACCOUNTING_LINE_TYPE, 1, MTA.BASE_TRANSACTION_VALUE/MTA.PRIMARY_QUANTITY, 14, MTA.BASE_TRANSACTION_VALUE/MTA.PRIMARY_QUANTITY, 3, MTA.BASE_TRANSACTION_VALUE/MTA.PRIMARY_QUANTITY, ABS ( MTA.BASE_TRANSACTION_VALUE / MTA.PRIMARY_QUANTITY ) ) */
          ELSE mta.rate_or_amount
        END
      END AS UNIT_COST,
      mmt.parent_transaction_id,
      mp2.organization_code,
      lu3.meaning AS LOGICAL_TRX_TYPE
    FROM (
      SELECT
        *
      FROM silver_bec_ods.mtl_transaction_accounts
      WHERE
        is_deleted_flg <> 'Y'
    ) AS mta, (
      SELECT
        *
      FROM silver_bec_ods.mtl_material_transactions
      WHERE
        is_deleted_flg <> 'Y'
    ) AS mmt, (
      SELECT
        *
      FROM silver_bec_ods.mtl_parameters
      WHERE
        is_deleted_flg <> 'Y'
    ) AS mp, (
      SELECT
        *
      FROM silver_bec_ods.mtl_parameters
      WHERE
        is_deleted_flg <> 'Y'
    ) AS mp1, (
      SELECT
        *
      FROM silver_bec_ods.mtl_parameters
      WHERE
        is_deleted_flg <> 'Y'
    ) AS mp2, (
      SELECT
        *
      FROM silver_bec_ods.cst_activities
      WHERE
        is_deleted_flg <> 'Y'
    ) AS ca, (
      SELECT
        *
      FROM silver_bec_ods.mtl_system_items_b
      WHERE
        is_deleted_flg <> 'Y'
    ) AS msi, (
      SELECT
        *
      FROM silver_bec_ods.bom_departments
      WHERE
        is_deleted_flg <> 'Y'
    ) AS bd, (
      SELECT
        *
      FROM silver_bec_ods.bom_resources
      WHERE
        is_deleted_flg <> 'Y'
    ) AS br, (
      SELECT
        *
      FROM silver_bec_ods.mtl_transaction_reasons
      WHERE
        is_deleted_flg <> 'Y'
    ) AS mtr, (
      SELECT
        *
      FROM silver_bec_ods.mtl_txn_source_types
      WHERE
        is_deleted_flg <> 'Y'
    ) AS mtst, (
      SELECT
        *
      FROM silver_bec_ods.mtl_transaction_types
      WHERE
        is_deleted_flg <> 'Y'
    ) AS mtt, (
      SELECT
        *
      FROM silver_bec_ods.FND_LOOKUP_VALUES
      WHERE
        is_deleted_flg <> 'Y'
    ) AS lu1, (
      SELECT
        *
      FROM silver_bec_ods.FND_LOOKUP_VALUES
      WHERE
        is_deleted_flg <> 'Y'
    ) AS lu2, (
      SELECT
        *
      FROM silver_bec_ods.FND_LOOKUP_VALUES
      WHERE
        is_deleted_flg <> 'Y'
    ) AS lu3, (
      SELECT
        *
      FROM silver_bec_ods.gl_daily_conversion_types
      WHERE
        is_deleted_flg <> 'Y'
    ) AS glt
    WHERE
      1 = 1
      AND mta.transaction_id = mmt.transfer_transaction_id
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
      AND msi.inventory_item_id = mmt.inventory_item_id
      AND msi.organization_id = mmt.organization_id
      AND mp.organization_id = mmt.organization_id
      AND mp1.ORGANIZATION_ID() = mmt.transfer_organization_id
      AND mp2.organization_id = mta.organization_id
      AND mtt.transaction_type_id = mmt.transaction_type_id
      AND mtst.transaction_source_type_id = mmt.transaction_source_type_id
      AND bd.DEPARTMENT_ID() = mmt.department_id
      AND mtr.REASON_ID() = mmt.reason_id
      AND br.RESOURCE_ID() = mta.resource_id
      AND lu1.lookup_type = 'CST_ACCOUNTING_LINE_TYPE'
      AND lu1.lookup_code = mta.accounting_line_type
      AND lu2.LOOKUP_CODE() = mta.basis_type
      AND lu2.LOOKUP_TYPE() = 'CST_BASIS_SHORT'
      AND lu3.LOOKUP_TYPE() = 'MTL_LOGICAL_TRANSACTION_CODE'
      AND lu3.LOOKUP_CODE() = mmt.logical_trx_type_code
      AND glt.CONVERSION_TYPE() = mta.currency_conversion_type
      AND ca.ACTIVITY_ID() = mta.activity_id
      AND mta.inventory_item_id = mmt.inventory_item_id
  )
);
UPDATE bec_etl_ctrl.batch_dw_info SET load_type = 'I', last_refresh_date = CURRENT_TIMESTAMP()
WHERE
  dw_table_name = 'fact_mtl_acct_dist' AND batch_name = 'costing';