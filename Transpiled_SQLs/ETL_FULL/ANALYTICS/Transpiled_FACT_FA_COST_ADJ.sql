DROP table IF EXISTS gold_bec_dwh.FACT_FA_COST_ADJ;
CREATE TABLE gold_bec_dwh.FACT_FA_COST_ADJ AS
(
  SELECT
    asset_type,
    asset_id,
    book_type_code,
    thid,
    category_id,
    code_combination_id,
    DATE_EFFECTIVE,
    GL_ACCOUNT,
    DEPRN_RESERVE_ACCT,
    GL_ACCOUNT_CCID,
    OLD_COST,
    NEW_COST,
    CHANGE,
    (
      SELECT
        system_id
      FROM bec_etl_ctrl.etlsourceappid
      WHERE
        source_system = 'EBS'
    ) || '-' || asset_id AS asset_id_key,
    (
      SELECT
        system_id
      FROM bec_etl_ctrl.etlsourceappid
      WHERE
        source_system = 'EBS'
    ) || '-' || category_id AS category_id_key,
    (
      SELECT
        system_id
      FROM bec_etl_ctrl.etlsourceappid
      WHERE
        source_system = 'EBS'
    ) || '-' || code_combination_id AS code_combination_id_key,
    'N' AS is_deleted_flg, /* audit columns */
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
    ) || '-' || COALESCE(thid, 0) || '-' || COALESCE(code_combination_id, 0) AS dw_load_id,
    CURRENT_TIMESTAMP() AS dw_insert_date,
    CURRENT_TIMESTAMP() AS dw_update_date
  FROM (
    SELECT
      asset_type,
      asset_id,
      book_type_code,
      thid,
      category_id,
      code_combination_id,
      DATE_EFFECTIVE,
      GL_ACCOUNT,
      DEPRN_RESERVE_ACCT,
      GL_ACCOUNT_CCID,
      SUM(
        CASE
          WHEN unit_sum = units OR (
            unit_sum IS NULL AND units IS NULL
          )
          THEN old_cost1 + old_cost - old_cost_rsum
          ELSE old_cost1
        END
      ) AS OLD_COST,
      SUM(
        CASE
          WHEN unit_sum = units OR (
            unit_sum IS NULL AND units IS NULL
          )
          THEN new_cost1 + new_cost - new_cost_rsum
          ELSE new_cost1
        END
      ) AS NEW_COST,
      SUM(
        CASE
          WHEN unit_sum = units OR (
            unit_sum IS NULL AND units IS NULL
          )
          THEN new_cost1 + new_cost - new_cost_rsum
          ELSE new_cost1
        END - CASE
          WHEN unit_sum = units OR (
            unit_sum IS NULL AND units IS NULL
          )
          THEN old_cost1 + old_cost - old_cost_rsum
          ELSE old_cost1
        END
      ) AS CHANGE
    FROM (
      SELECT
        ah.asset_type AS asset_type,
        ah.asset_id,
        th.book_type_code,
        th.transaction_header_id AS thid,
        ROUND((
          books_old.cost * COALESCE(dh.units_assigned, ah.units) / ah.units
        ), 4) AS old_cost1,
        (
          ROUND((
            books_old.cost * COALESCE(dh.units_assigned, ah.units) / ah.units
          ), 4) + ROUND(
            (
              (
                books_new.cost - books_old.cost
              ) * COALESCE(dh.units_assigned, ah.units) / ah.units
            ),
            4
          )
        ) AS new_cost1,
        SUM(
          ROUND((
            books_old.cost * COALESCE(dh.units_assigned, ah.units) / ah.units
          ), 4)
        ) OVER (PARTITION BY th.transaction_header_id, dh.asset_id) AS old_cost_rsum,
        SUM(
          (
            ROUND((
              books_old.cost * COALESCE(dh.units_assigned, ah.units) / ah.units
            ), 4) + ROUND(
              (
                (
                  books_new.cost - books_old.cost
                ) * COALESCE(dh.units_assigned, ah.units) / ah.units
              ),
              4
            )
          )
        ) OVER (PARTITION BY th.transaction_header_id, dh.asset_id) AS new_cost_rsum,
        SUM(COALESCE(dh.units_assigned, ah.units)) OVER (PARTITION BY th.transaction_header_id, dh.asset_id) AS unit_sum,
        ah.units AS units,
        books_old.cost AS old_cost,
        books_new.cost AS new_cost,
        ah.category_id,
        dh.code_combination_id,
        TH.DATE_EFFECTIVE,
        CASE WHEN ah.ASSET_TYPE = 'CIP' THEN fcb.CIP_COST_ACCT ELSE fcb.ASSET_COST_ACCT END AS GL_ACCOUNT,
        CASE WHEN ah.ASSET_TYPE = 'CIP' THEN NULL ELSE fcb.DEPRN_RESERVE_ACCT END AS DEPRN_RESERVE_ACCT,
        CASE
          WHEN ah.ASSET_TYPE = 'CIP'
          THEN fcb.WIP_COST_ACCOUNT_CCID
          ELSE fcb.ASSET_COST_ACCOUNT_CCID
        END AS GL_ACCOUNT_CCID
      FROM (
        SELECT
          *
        FROM silver_bec_ods.fa_asset_history
        WHERE
          is_deleted_flg <> 'Y'
      ) AS ah, (
        SELECT
          *
        FROM silver_bec_ods.fa_books
        WHERE
          is_deleted_flg <> 'Y'
      ) AS books_old, (
        SELECT
          *
        FROM silver_bec_ods.fa_books
        WHERE
          is_deleted_flg <> 'Y'
      ) AS books_new, (
        SELECT
          *
        FROM silver_bec_ods.fa_distribution_history
        WHERE
          is_deleted_flg <> 'Y'
      ) AS dh, (
        SELECT
          *
        FROM silver_bec_ods.fa_transaction_headers
        WHERE
          is_deleted_flg <> 'Y'
      ) AS th, (
        SELECT
          *
        FROM silver_bec_ods.fa_category_books
        WHERE
          is_deleted_flg <> 'Y'
      ) AS fcb
      WHERE
        1 = 1
        AND th.transaction_type_code IN ('ADJUSTMENT', 'CIP ADJUSTMENT')
        AND books_old.transaction_header_id_out = th.transaction_header_id
        AND books_old.book_type_code = th.book_type_code
        AND books_new.transaction_header_id_in = th.transaction_header_id
        AND books_new.book_type_code = th.book_type_code
        AND ah.asset_id = th.asset_id
        AND th.transaction_header_id >= ah.transaction_header_id_in
        AND th.transaction_header_id < COALESCE(ah.transaction_header_id_out, th.transaction_header_id + 1)
        AND th.asset_id = dh.asset_id
        AND th.book_type_code = dh.book_type_code
        AND th.transaction_header_id >= dh.transaction_header_id_in
        AND th.transaction_header_id < COALESCE(dh.transaction_header_id_out, th.transaction_header_id + 1)
        AND ROUND((
          books_old.cost * COALESCE(dh.units_assigned, ah.units) / ah.units
        ), 4) <> ROUND((
          books_new.cost * COALESCE(dh.units_assigned, ah.units) / ah.units
        ), 4)
        AND fcb.CATEGORY_ID = AH.CATEGORY_ID
        AND fcb.BOOK_TYPE_CODE = TH.BOOK_TYPE_CODE
    )
    GROUP BY
      asset_type,
      asset_id,
      book_type_code,
      thid,
      category_id,
      code_combination_id,
      DATE_EFFECTIVE,
      GL_ACCOUNT,
      DEPRN_RESERVE_ACCT,
      GL_ACCOUNT_CCID
  )
);
UPDATE bec_etl_ctrl.batch_dw_info SET load_type = 'I', last_refresh_date = CURRENT_TIMESTAMP()
WHERE
  dw_table_name = 'fact_fa_cost_adj' AND batch_name = 'fa';