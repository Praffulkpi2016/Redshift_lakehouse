/* delete records */
DELETE FROM gold_bec_dwh.DIM_MTL_TRANSACTION_ACCOUNTS
WHERE
  (COALESCE(transaction_id, 0), COALESCE(meaning, 'NA'), COALESCE(account, 'NA')) IN (
    SELECT
      COALESCE(ods.transaction_id, 0) AS transaction_id,
      COALESCE(ods.meaning, 'NA') AS meaning,
      COALESCE(ods.account, 'NA') AS account
    FROM gold_bec_dwh.DIM_MTL_TRANSACTION_ACCOUNTS AS dw, (
      SELECT
        c.transaction_id,
        lkp.meaning,
        c.accounting_line_type,
        SUM(c.primary_quantity) AS primary_quantity,
        gcc.segment1 || '.' || gcc.segment2 || '.' || gcc.segment3 AS account,
        SUM(c.base_transaction_value) AS trans_value
      FROM silver_bec_ods.mtl_transaction_accounts AS c, silver_bec_ods.gl_code_combinations AS gcc, silver_bec_ods.fnd_lookup_values AS lkp
      WHERE
        1 = 1
        AND c.reference_account = gcc.code_combination_id
        AND lkp.lookup_code = c.accounting_line_type
        AND lkp.lookup_type = 'CST_ACCOUNTING_LINE_TYPE'
        AND lkp.meaning IN ('Intransit Inventory', 'Profit in inventory', 'Account')
        AND c.primary_quantity > 0
        AND (
          c.kca_seq_date > (
            SELECT
              (
                executebegints - prune_days
              )
            FROM bec_etl_ctrl.batch_dw_info
            WHERE
              dw_table_name = 'dim_mtl_transaction_accounts' AND batch_name = 'inv'
          )
          OR gcc.kca_seq_date > (
            SELECT
              (
                executebegints - prune_days
              )
            FROM bec_etl_ctrl.batch_dw_info
            WHERE
              dw_table_name = 'dim_mtl_transaction_accounts' AND batch_name = 'inv'
          )
          OR c.is_deleted_flg = 'Y'
          OR gcc.is_deleted_flg = 'Y'
          OR lkp.is_deleted_flg = 'Y'
        ) /*  and TRANSACTION_id in (8388683, 8264632) */
      GROUP BY
        c.transaction_id,
        gcc.segment1 || '.' || gcc.segment2 || '.' || gcc.segment3,
        lkp.meaning,
        c.accounting_line_type
    ) AS ods
    WHERE
      1 = 1
      AND dw.dw_load_id = (
        SELECT
          system_id
        FROM bec_etl_ctrl.etlsourceappid
        WHERE
          source_system = 'EBS'
      ) || '-' || COALESCE(ods.transaction_id, 0) || '-' || COALESCE(ods.meaning, 'NA') || '-' || COALESCE(ods.account, 'NA')
  );
/* Insert Records */
INSERT INTO gold_bec_dwh.DIM_MTL_TRANSACTION_ACCOUNTS (
  transaction_id,
  meaning,
  accounting_line_type,
  primary_quantity,
  account,
  trans_value,
  is_deleted_flg,
  source_app_id,
  dw_load_id,
  dw_insert_date,
  dw_update_date
)
(
  SELECT
    transaction_id,
    meaning,
    accounting_line_type,
    primary_quantity,
    account,
    trans_value,
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
    ) || '-' || COALESCE(transaction_id, 0) || '-' || COALESCE(meaning, 'NA') || '-' || COALESCE(account, 'NA') AS dw_load_id,
    CURRENT_TIMESTAMP() AS dw_insert_date,
    CURRENT_TIMESTAMP() AS dw_update_date
  FROM (
    SELECT
      c.transaction_id,
      lkp.meaning,
      c.accounting_line_type,
      SUM(c.primary_quantity) AS primary_quantity,
      gcc.segment1 || '.' || gcc.segment2 || '.' || gcc.segment3 AS account,
      SUM(c.base_transaction_value) AS trans_value
    FROM (
      SELECT
        *
      FROM silver_bec_ods.mtl_transaction_accounts
      WHERE
        is_deleted_flg <> 'Y'
    ) AS c, (
      SELECT
        *
      FROM silver_bec_ods.gl_code_combinations
      WHERE
        is_deleted_flg <> 'Y'
    ) AS gcc, (
      SELECT
        *
      FROM silver_bec_ods.fnd_lookup_values
      WHERE
        is_deleted_flg <> 'Y'
    ) AS lkp
    WHERE
      1 = 1
      AND c.reference_account = gcc.code_combination_id
      AND lkp.lookup_code = c.accounting_line_type
      AND lkp.lookup_type = 'CST_ACCOUNTING_LINE_TYPE'
      AND lkp.meaning IN ('Intransit Inventory', 'Profit in inventory', 'Account')
      AND c.primary_quantity > 0
      AND (
        c.kca_seq_date > (
          SELECT
            (
              executebegints - prune_days
            )
          FROM bec_etl_ctrl.batch_dw_info
          WHERE
            dw_table_name = 'dim_mtl_transaction_accounts' AND batch_name = 'inv'
        )
        OR gcc.kca_seq_date > (
          SELECT
            (
              executebegints - prune_days
            )
          FROM bec_etl_ctrl.batch_dw_info
          WHERE
            dw_table_name = 'dim_mtl_transaction_accounts' AND batch_name = 'inv'
        )
      ) /*  and TRANSACTION_id in (8388683, 8264632) */
    GROUP BY
      c.transaction_id,
      gcc.segment1 || '.' || gcc.segment2 || '.' || gcc.segment3,
      lkp.meaning,
      c.accounting_line_type
  )
);
UPDATE bec_etl_ctrl.batch_dw_info SET load_type = 'I', last_refresh_date = CURRENT_TIMESTAMP()
WHERE
  dw_table_name = 'dim_mtl_transaction_accounts' AND batch_name = 'inv';