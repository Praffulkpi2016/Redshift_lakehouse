DROP table IF EXISTS BEC_DWH.FACT_CST_INV_DISTRIBUTION_STG2;
CREATE TABLE gold_bec_dwh.fact_cst_inv_distribution_stg2 AS
(
  SELECT DISTINCT
    transaction_source_type_id,
    transaction_id,
    reference_account,
    transaction_date,
    creation_date,
    last_update_date,
    inventory_item_id,
    organization_id,
    subinventory_code,
    primary_quantity,
    serial_number,
    cost_element_id,
    transaction_cost,
    unit_cost,
    trx_value,
    transaction_source_id,
    mmt_transaction_source_id,
    trx_source_line_id,
    created_by,
    rcv_transaction_id,
    source_line_id,
    transaction_organization_id,
    transaction_type_id,
    accounting_line_type,
    transaction_reference,
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
    WITH CTE_mtl_unit_transactions AS (
      SELECT
        transaction_id,
        serial_number
      FROM (
        SELECT
          *
        FROM silver_bec_ods.mtl_unit_transactions
        WHERE
          is_deleted_flg <> 'Y'
      )
      GROUP BY
        transaction_id,
        serial_number
    )
    (
      SELECT
        mmt.transaction_source_type_id,
        mmt.transaction_id,
        mtrx.reference_account,
        mtrx.transaction_date,
        mmt.creation_date,
        mmt.last_update_date,
        mtrx.inventory_item_id,
        mtrx.organization_id,
        mtrx.subinventory_code,
        (
          mtrx.primary_quantity / ABS(mtrx.primary_quantity)
        ) AS primary_quantity,
        mut.serial_number,
        mtrx.cost_element_id,
        mmt.transaction_cost,
        CASE
          WHEN mtrx.TRANSACTION_TYPE_ID = 24
          THEN mmt.transaction_cost
          ELSE mtrx.unit_cost
        END AS unit_cost,
        CASE
          WHEN mtrx.TRANSACTION_TYPE_ID = 24
          THEN (
            mmt.transaction_cost * (
              mtrx.primary_quantity / ABS(mtrx.primary_quantity)
            )
          )
          ELSE (
            mtrx.unit_cost * (
              mtrx.primary_quantity / ABS(mtrx.primary_quantity)
            )
          )
        END AS trx_value,
        mtrx.transaction_source_id,
        mmt.transaction_source_type_id AS mmt_transaction_source_id, /* use this decode statement for next toget source value */
        mmt.trx_source_line_id,
        mmt.created_by,
        mmt.rcv_transaction_id,
        mmt.source_line_id,
        mtrx.transaction_organization_id,
        mtrx.TRANSACTION_TYPE_ID,
        mtrx.ACCOUNTING_LINE_TYPE,
        mtrx.transaction_reference
      FROM gold_bec_dwh.fact_cst_inv_distribution_stg1 AS mtrx, (
        SELECT
          *
        FROM silver_bec_ods.mtl_material_transactions
        WHERE
          is_deleted_flg <> 'Y'
      ) AS mmt, CTE_mtl_unit_transactions AS mut
      WHERE
        mmt.TRANSACTION_ID() = mtrx.transaction_id
        AND mmt.transaction_id = mut.TRANSACTION_ID()
        AND NOT mut.serial_number IS NULL
      UNION ALL
      SELECT
        mmt.transaction_source_type_id,
        mmt.transaction_id,
        mtrx.reference_account,
        mtrx.transaction_date,
        mmt.creation_date,
        mmt.last_update_date,
        mtrx.inventory_item_id,
        mtrx.organization_id,
        mtrx.subinventory_code,
        mtrx.primary_quantity AS primary_quantity,
        mut.serial_number,
        mtrx.cost_element_id,
        mmt.transaction_cost,
        CASE
          WHEN mtrx.TRANSACTION_TYPE_ID = 24
          THEN mmt.transaction_cost
          ELSE mtrx.unit_cost
        END AS unit_cost,
        CASE
          WHEN mtrx.TRANSACTION_TYPE_ID = 24
          THEN mmt.transaction_cost * mtrx.primary_quantity
          ELSE mtrx.unit_cost * mtrx.primary_quantity
        END AS trx_value,
        mtrx.transaction_source_id,
        mmt.transaction_source_type_id AS mmt_transaction_source_id,
        mmt.trx_source_line_id,
        mmt.created_by,
        mmt.rcv_transaction_id,
        mmt.source_line_id,
        mtrx.transaction_organization_id,
        mtrx.TRANSACTION_TYPE_ID,
        mtrx.ACCOUNTING_LINE_TYPE,
        mtrx.transaction_reference
      FROM gold_bec_dwh.fact_cst_inv_distribution_stg1 AS mtrx, (
        SELECT
          *
        FROM silver_bec_ods.mtl_material_transactions
        WHERE
          is_deleted_flg <> 'Y'
      ) AS mmt, CTE_mtl_unit_transactions AS mut
      WHERE
        mmt.TRANSACTION_ID() = mtrx.transaction_id
        AND mmt.transaction_id = mut.TRANSACTION_ID()
        AND mut.serial_number IS NULL
    )
  )
);
UPDATE bec_etl_ctrl.batch_dw_info SET load_type = 'I', last_refresh_date = CURRENT_TIMESTAMP()
WHERE
  dw_table_name = 'fact_cst_inv_distribution_stg2' AND batch_name = 'inv';