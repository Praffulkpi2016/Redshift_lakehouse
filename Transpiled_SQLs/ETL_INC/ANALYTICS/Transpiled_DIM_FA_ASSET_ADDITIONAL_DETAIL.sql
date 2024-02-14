/* Delete Records */
DELETE FROM gold_bec_dwh.DIM_FA_ASSET_ADDITIONAL_DETAIL
WHERE
  (COALESCE(asset_id, 0), COALESCE(code_combination_id, 0)) IN (
    SELECT
      COALESCE(ods.asset_id, 0) AS asset_id,
      COALESCE(ods.code_combination_id, 0) AS code_combination_id
    FROM gold_bec_dwh.DIM_FA_ASSET_ADDITIONAL_DETAIL AS dw, (
      SELECT
        fact_asset_transaction.asset_id,
        fact_asset_transaction.code_combination_id
      FROM (
        SELECT
          fa_adjustments.source_type_code AS source_type_code,
          fa_adjustments.adjustment_type AS adjustment_type,
          fa_distribution_history.code_combination_id AS code_combination_id,
          fa_transaction_headers.transaction_header_id AS transaction_header_id,
          fa_adjustments.asset_id,
          fa_adjustments.last_update_date,
          fa_adjustments.kca_seq_date
        FROM silver_bec_ods.fa_distribution_history AS fa_distribution_history
        INNER JOIN silver_bec_ods.fa_adjustments AS fa_adjustments
          ON fa_adjustments.distribution_id = fa_distribution_history.distribution_id
        INNER JOIN silver_bec_ods.fa_transaction_headers AS fa_transaction_headers
          ON fa_transaction_headers.transaction_header_id = fa_adjustments.transaction_header_id
      ) AS fact_asset_transaction
      INNER JOIN (
        SELECT
          fdd.asset_id AS asset_id,
          fdd.deprn_run_date AS deprn_run_date
        FROM silver_bec_ods.fa_deprn_detail AS fdd
        INNER JOIN silver_bec_ods.fa_deprn_summary AS fds
          ON fdd.asset_id = fds.asset_id
          AND fdd.book_type_code = fds.book_type_code
          AND fdd.period_counter = fds.period_counter
      ) AS fact_deprn_hist
        ON fact_asset_transaction.asset_id = fact_deprn_hist.asset_id
      INNER JOIN silver_bec_ods.gl_code_combinations_kfv AS gl_code
        ON fact_asset_transaction.code_combination_id = gl_code.code_combination_id
        AND fact_asset_transaction.source_type_code = 'ADDITION'
        AND fact_asset_transaction.adjustment_type = 'COST'
      WHERE
        (
          fact_asset_transaction.kca_seq_date > (
            SELECT
              (
                executebegints - prune_days
              )
            FROM bec_etl_ctrl.batch_dw_info
            WHERE
              dw_table_name = 'dim_fa_asset_additional_detail' AND batch_name = 'fa'
          )
        )
      GROUP BY
        fact_asset_transaction.asset_id,
        fact_asset_transaction.code_combination_id
    ) AS ods
    WHERE
      dw.dw_load_id = (
        SELECT
          system_id
        FROM bec_etl_ctrl.etlsourceappid
        WHERE
          source_system = 'EBS'
      ) || '-' || COALESCE(ods.asset_id, 0) || '-' || COALESCE(ods.code_combination_id, 0)
  );
/* Insert records */
INSERT INTO gold_bec_dwh.DIM_FA_ASSET_ADDITIONAL_DETAIL (
  asset_id,
  code_combination_id,
  concatenated_segments,
  deprn_run_date,
  transaction_header_id,
  is_deleted_flg,
  source_app_id,
  dw_load_id,
  dw_insert_date,
  dw_update_date
)
(
  SELECT
    fact_asset_transaction.asset_id,
    fact_asset_transaction.code_combination_id,
    gl_code.segment1 || '-' || gl_code.segment2 || '-' || gl_code.segment3 || '-' || gl_code.segment4 || gl_code.segment5 || '-' || gl_code.segment6 AS concatenated_segments,
    MAX(fact_deprn_hist.deprn_run_date) AS deprn_run_date,
    MAX(fact_asset_transaction.transaction_header_id) AS transaction_header_id,
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
    ) || '-' || COALESCE(fact_asset_transaction.asset_id, 0) || '-' || COALESCE(fact_asset_transaction.code_combination_id, 0) AS dw_load_id,
    CURRENT_TIMESTAMP() AS dw_insert_date,
    CURRENT_TIMESTAMP() AS dw_update_date
  FROM (
    SELECT
      fa_adjustments.source_type_code AS source_type_code,
      fa_adjustments.adjustment_type AS adjustment_type,
      fa_distribution_history.code_combination_id AS code_combination_id,
      fa_transaction_headers.transaction_header_id AS transaction_header_id,
      fa_adjustments.asset_id,
      fa_adjustments.last_update_date,
      fa_adjustments.kca_seq_date
    FROM silver_bec_ods.fa_distribution_history AS fa_distribution_history
    INNER JOIN silver_bec_ods.fa_adjustments AS fa_adjustments
      ON fa_adjustments.distribution_id = fa_distribution_history.distribution_id
    INNER JOIN silver_bec_ods.fa_transaction_headers AS fa_transaction_headers
      ON fa_transaction_headers.transaction_header_id = fa_adjustments.transaction_header_id
  ) AS fact_asset_transaction
  INNER JOIN (
    SELECT
      fdd.asset_id AS asset_id,
      fdd.deprn_run_date AS deprn_run_date
    FROM silver_bec_ods.fa_deprn_detail AS fdd
    INNER JOIN silver_bec_ods.fa_deprn_summary AS fds
      ON fdd.asset_id = fds.asset_id
      AND fdd.book_type_code = fds.book_type_code
      AND fdd.period_counter = fds.period_counter
  ) AS fact_deprn_hist
    ON fact_asset_transaction.asset_id = fact_deprn_hist.asset_id
  INNER JOIN silver_bec_ods.gl_code_combinations_kfv AS gl_code
    ON fact_asset_transaction.code_combination_id = gl_code.code_combination_id
    AND fact_asset_transaction.source_type_code = 'ADDITION'
    AND fact_asset_transaction.adjustment_type = 'COST'
  WHERE
    (
      fact_asset_transaction.kca_seq_date > (
        SELECT
          (
            executebegints - prune_days
          )
        FROM bec_etl_ctrl.batch_dw_info
        WHERE
          dw_table_name = 'dim_fa_asset_additional_detail' AND batch_name = 'fa'
      )
    )
  GROUP BY
    fact_asset_transaction.asset_id,
    fact_asset_transaction.code_combination_id,
    gl_code.segment1 || '-' || gl_code.segment2 || '-' || gl_code.segment3 || '-' || gl_code.segment4 || gl_code.segment5 || '-' || gl_code.segment6
);
/* Soft delete */
UPDATE gold_bec_dwh.DIM_FA_ASSET_ADDITIONAL_DETAIL SET is_deleted_flg = 'Y'
WHERE
  NOT (COALESCE(asset_id, 0), COALESCE(code_combination_id, 0)) IN (
    SELECT
      COALESCE(ods.asset_id, 0) AS asset_id,
      COALESCE(ods.code_combination_id, 0) AS code_combination_id
    FROM gold_bec_dwh.DIM_FA_ASSET_ADDITIONAL_DETAIL AS dw, (
      SELECT
        fact_asset_transaction.asset_id,
        fact_asset_transaction.code_combination_id
      FROM (
        SELECT
          fa_adjustments.source_type_code AS source_type_code,
          fa_adjustments.adjustment_type AS adjustment_type,
          fa_distribution_history.code_combination_id AS code_combination_id,
          fa_transaction_headers.transaction_header_id AS transaction_header_id,
          fa_adjustments.asset_id
        FROM (
          SELECT
            *
          FROM silver_bec_ods.fa_distribution_history
          WHERE
            is_deleted_flg <> 'Y'
        ) AS fa_distribution_history
        INNER JOIN (
          SELECT
            *
          FROM silver_bec_ods.fa_adjustments
          WHERE
            is_deleted_flg <> 'Y'
        ) AS fa_adjustments
          ON fa_adjustments.distribution_id = fa_distribution_history.distribution_id
        INNER JOIN (
          SELECT
            *
          FROM silver_bec_ods.fa_transaction_headers
          WHERE
            is_deleted_flg <> 'Y'
        ) AS fa_transaction_headers
          ON fa_transaction_headers.transaction_header_id = fa_adjustments.transaction_header_id
      ) AS fact_asset_transaction
      INNER JOIN (
        SELECT
          fdd.asset_id AS asset_id,
          fdd.deprn_run_date AS deprn_run_date
        FROM (
          SELECT
            *
          FROM silver_bec_ods.fa_deprn_detail
          WHERE
            is_deleted_flg <> 'Y'
        ) AS fdd
        INNER JOIN (
          SELECT
            *
          FROM silver_bec_ods.fa_deprn_summary
          WHERE
            is_deleted_flg <> 'Y'
        ) AS fds
          ON fdd.asset_id = fds.asset_id
          AND fdd.book_type_code = fds.book_type_code
          AND fdd.period_counter = fds.period_counter
      ) AS fact_deprn_hist
        ON fact_asset_transaction.asset_id = fact_deprn_hist.asset_id
      INNER JOIN (
        SELECT
          *
        FROM silver_bec_ods.gl_code_combinations_kfv
        WHERE
          is_deleted_flg <> 'Y'
      ) AS gl_code
        ON fact_asset_transaction.code_combination_id = gl_code.code_combination_id
        AND fact_asset_transaction.source_type_code = 'ADDITION'
        AND fact_asset_transaction.adjustment_type = 'COST'
      GROUP BY
        fact_asset_transaction.asset_id,
        fact_asset_transaction.code_combination_id
    ) AS ods
    WHERE
      dw.dw_load_id = (
        SELECT
          system_id
        FROM bec_etl_ctrl.etlsourceappid
        WHERE
          source_system = 'EBS'
      ) || '-' || COALESCE(ods.asset_id, 0) || '-' || COALESCE(ods.code_combination_id, 0)
  );
UPDATE bec_etl_ctrl.batch_dw_info SET last_refresh_date = CURRENT_TIMESTAMP()
WHERE
  dw_table_name = 'dim_fa_asset_additional_detail' AND batch_name = 'fa';