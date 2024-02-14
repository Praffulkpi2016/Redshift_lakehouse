DROP table IF EXISTS BEC_DWH.FACT_CST_INV_DISTRIBUTION_STG;
CREATE TABLE gold_bec_dwh.fact_cst_inv_distribution_stg AS
(
  SELECT DISTINCT
    REFERENCE_ACCOUNT,
    transaction_organization_id,
    transaction_id,
    ORGANIZATION_ID,
    INVENTORY_ITEM_ID,
    TRANSACTION_SOURCE_ID,
    COST_ELEMENT_ID,
    transaction_type_id,
    accounting_line_type,
    transaction_reference,
    transaction_date,
    subinventory_code,
    trx_source_line_id,
    transaction_source_type_id,
    rcv_transaction_id,
    source_line_id,
    created_by,
    creation_date,
    last_update_date,
    transaction_cost,
    primary_quantity,
    unit_cost,
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
    SELECT
      MTA.REFERENCE_ACCOUNT,
      mmt.organization_id AS transaction_organization_id,
      mta.transaction_id,
      MTA.ORGANIZATION_ID,
      MMT.INVENTORY_ITEM_ID,
      MMT.TRANSACTION_SOURCE_ID,
      MTA.COST_ELEMENT_ID,
      mmt.transaction_type_id,
      mta.accounting_line_type, /* Join with lookup 'CST_ACCOUNTING_LINE_TYPE' */
      mmt.transaction_reference,
      mta.transaction_date,
      mmt.subinventory_code,
      mmt.trx_source_line_id,
      mmt.transaction_source_type_id,
      mmt.rcv_transaction_id,
      mmt.source_line_id,
      mmt.created_by,
      mmt.creation_date,
      mmt.last_update_date,
      mmt.transaction_cost,
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
      END AS primary_quantity,
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
      END AS unit_cost
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
    ) AS MP1
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
    UNION ALL
    SELECT
      MTA.REFERENCE_ACCOUNT,
      mmt.organization_id AS transaction_organization_id,
      mta.transaction_id,
      MTA.ORGANIZATION_ID,
      MMT.INVENTORY_ITEM_ID,
      MMT.TRANSACTION_SOURCE_ID,
      MTA.COST_ELEMENT_ID,
      mmt.transaction_type_id,
      mta.accounting_line_type, /* Join with lookup 'CST_ACCOUNTING_LINE_TYPE' */
      mmt.transaction_reference,
      mta.transaction_date,
      mmt.subinventory_code,
      mmt.trx_source_line_id,
      mmt.transaction_source_type_id,
      mmt.rcv_transaction_id,
      mmt.source_line_id,
      mmt.created_by,
      mmt.creation_date,
      mmt.last_update_date,
      mmt.transaction_cost,
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
      END AS primary_quantity,
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
      END AS unit_cost
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
    ) AS MP1
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
  )
);
UPDATE bec_etl_ctrl.batch_dw_info SET load_type = 'I', last_refresh_date = CURRENT_TIMESTAMP()
WHERE
  dw_table_name = 'fact_cst_inv_distribution_stg' AND batch_name = 'inv';