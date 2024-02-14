/* Delete Records */
DELETE FROM gold_bec_dwh.FACT_FA_RETIREMENTS
WHERE
  (COALESCE(retirement_id, 0), COALESCE(adjustment_line_id, 0)) IN (
    SELECT
      COALESCE(ods.retirement_id, 0) AS retirement_id,
      COALESCE(ods.adjustment_line_id, 0) AS adjustment_line_id
    FROM gold_bec_dwh.FACT_FA_RETIREMENTS AS dw, (
      SELECT
        retirement_id,
        adjustment_line_id
      FROM (
        SELECT
          fr.retirement_id,
          faj.adjustment_line_id
        FROM silver_bec_ods.fa_transaction_headers AS fth, silver_bec_ods.fa_books AS books, silver_bec_ods.fa_retirements AS fr, silver_bec_ods.fa_adjustments AS faj, silver_bec_ods.fa_distribution_history AS fdh, silver_bec_ods.fa_asset_history AS ah, silver_bec_ods.fa_category_books AS cb
        WHERE
          fth.transaction_key = 'R'
          AND fth.book_type_code = fr.book_type_code
          AND fth.asset_id = fr.asset_id
          AND fr.asset_id = books.asset_id
          AND CASE
            WHEN fth.transaction_type_code = 'REINSTATEMENT'
            THEN fr.transaction_header_id_out
            ELSE fr.transaction_header_id_in
          END = fth.transaction_header_id
          AND faj.asset_id = fr.asset_id
          AND faj.book_type_code = fr.book_type_code
          AND faj.transaction_header_id = fth.transaction_header_id
          AND ah.asset_id = fth.asset_id
          AND ah.date_effective <= fth.date_effective
          AND COALESCE(ah.date_ineffective, fth.date_effective + 1) > fth.date_effective
          AND books.transaction_header_id_out = fth.transaction_header_id
          AND books.book_type_code = fth.book_type_code
          AND books.asset_id = fth.asset_id
          AND cb.category_id = ah.category_id
          AND cb.book_type_code = fr.book_type_code
          AND fdh.distribution_id = faj.distribution_id
          AND fth.asset_id = fdh.asset_id
          AND (
            fr.kca_seq_date > (
              SELECT
                (
                  executebegints - prune_days
                )
              FROM bec_etl_ctrl.batch_dw_info
              WHERE
                dw_table_name = 'fact_fa_retirements' AND batch_name = 'fa'
            )
            OR faj.kca_seq_date > (
              SELECT
                (
                  executebegints - prune_days
                )
              FROM bec_etl_ctrl.batch_dw_info
              WHERE
                dw_table_name = 'fact_fa_retirements' AND batch_name = 'fa'
            )
            OR fth.is_deleted_flg = 'Y'
            OR books.is_deleted_flg = 'Y'
            OR fr.is_deleted_flg = 'Y'
            OR faj.is_deleted_flg = 'Y'
            OR fdh.is_deleted_flg = 'Y'
            OR ah.is_deleted_flg = 'Y'
            OR cb.is_deleted_flg = 'Y'
          )
        UNION
        SELECT
          fr.retirement_id,
          NULL AS adjustment_line_id
        FROM silver_bec_ods.fa_transaction_headers AS fth, silver_bec_ods.fa_books AS books, silver_bec_ods.fa_retirements AS fr, (
          SELECT
            DH.*,
            TH2.DATE_EFFECTIVE AS TH2_DATE,
            TH1.DATE_EFFECTIVE AS TH1_DATE,
            DH.is_deleted_flg AS is_deleted_flg1,
            TH1.is_deleted_flg AS is_deleted_flg2,
            BC.is_deleted_flg AS is_deleted_flg3,
            TH2.is_deleted_flg AS is_deleted_flg4
          FROM silver_bec_ods.FA_TRANSACTION_HEADERS AS TH1, silver_bec_ods.FA_DISTRIBUTION_HISTORY AS DH, silver_bec_ods.FA_BOOK_CONTROLS AS BC, silver_bec_ods.FA_TRANSACTION_HEADERS AS TH2
          WHERE
            TH1.BOOK_TYPE_CODE = DH.BOOK_TYPE_CODE
            AND TH1.TRANSACTION_TYPE_CODE IN ('FULL RETIREMENT', 'PARTIAL RETIREMENT')
            AND TH1.ASSET_ID = DH.ASSET_ID
            AND BC.BOOK_TYPE_CODE = th1.BOOK_TYPE_CODE
            AND bC.DISTRIBUTION_SOURCE_BOOK = DH.BOOK_TYPE_CODE
            AND TH1.DATE_EFFECTIVE <= COALESCE(DH.DATE_INEFFECTIVE, TH1.DATE_EFFECTIVE)
            AND TH1.ASSET_ID = TH2.ASSET_ID
            AND TH2.BOOK_TYPE_CODE = DH.BOOK_TYPE_CODE
            AND th2.TRANSACTION_TYPE_CODE = 'REINSTATEMENT'
            AND th2.date_effective >= dh.DATE_EFFECTIVE
        ) AS fdh, silver_bec_ods.fa_asset_history AS ah, silver_bec_ods.fa_category_books AS cb
        WHERE
          fth.transaction_key = 'R'
          AND fth.book_type_code = fr.book_type_code
          AND fth.asset_id = fr.asset_id
          AND fr.asset_id = books.asset_id
          AND fr.transaction_header_id_out = fth.transaction_header_id
          AND ah.asset_id = fth.asset_id
          AND ah.date_effective <= fth.date_effective
          AND COALESCE(ah.date_ineffective, fth.date_effective + 1) > fth.date_effective
          AND books.transaction_header_id_out = fth.transaction_header_id
          AND books.book_type_code = fth.book_type_code
          AND books.asset_id = fth.asset_id
          AND cb.category_id = ah.category_id
          AND cb.book_type_code = fr.book_type_code /* AND  fdh.distribution_id  = faj.distribution_id */
          AND fth.asset_id = fdh.asset_id
          AND FDH.BOOK_TYPE_CODE = fth.BOOK_TYPE_CODE
          AND fdh.TH2_DATE >= fth.date_effective
          AND fdh.TH1_DATE <= fth.date_effective
          AND fth.TRANSACTION_TYPE_CODE = 'REINSTATEMENT'
          AND fr.COST_RETIRED = 0
          AND fr.cost_of_removal = 0
          AND fr.proceeds_of_sale = 0
          AND (
            fr.kca_seq_date > (
              SELECT
                (
                  executebegints - prune_days
                )
              FROM bec_etl_ctrl.batch_dw_info
              WHERE
                dw_table_name = 'fact_fa_retirements' AND batch_name = 'fa'
            )
            OR fr.is_deleted_flg = 'Y'
            OR fdh.is_deleted_flg1 = 'Y'
            OR fdh.is_deleted_flg2 = 'Y'
            OR fdh.is_deleted_flg3 = 'Y'
            OR fdh.is_deleted_flg4 = 'Y'
            OR fth.is_deleted_flg = 'Y'
            OR books.is_deleted_flg = 'Y'
            OR ah.is_deleted_flg = 'Y'
            OR cb.is_deleted_flg = 'Y'
          )
      )
    ) AS ods
    WHERE
      dw.dw_load_id = (
        SELECT
          system_id
        FROM bec_etl_ctrl.etlsourceappid
        WHERE
          source_system = 'EBS'
      ) || '-' || COALESCE(ods.retirement_id, 0) || '-' || COALESCE(ods.adjustment_line_id, 0)
  );
/* Insert records */
INSERT INTO gold_bec_dwh.FACT_FA_RETIREMENTS (
  retirement_id,
  book_type_code,
  cost_retired,
  retirement_status,
  cost_of_removal,
  nbv_retired,
  gain_loss_amount,
  proceeds_of_sale,
  retirement_type_code,
  created_by,
  creation_date,
  unrevalued_cost_retired,
  retirement_prorate_convention,
  reval_reserve_retired,
  date_retired,
  reserve_retired,
  asset_id,
  adjust_last_update_date,
  last_updated_by,
  book_type_code_faj,
  adjustment_type,
  source_type_code,
  adjustment_amount,
  transaction_header_id,
  adjustment_line_id,
  code_combination_id,
  date_ineffective,
  location_id,
  distribution_id,
  transaction_units,
  last_update_date,
  transaction_date_entered,
  transaction_type_code,
  date_effective,
  asset_type,
  account,
  date_placed_in_service,
  PROCEEDS_CLR,
  cost,
  nbv,
  proceeds,
  removal,
  reval_rsv_ret,
  code,
  retirement_id_key,
  asset_id_key,
  transaction_header_id_key,
  adjustment_line_id_key,
  code_combination_id_key,
  location_id_key,
  distribution_id_key,
  is_deleted_flg,
  source_app_id,
  dw_load_id,
  dw_insert_date,
  dw_update_date
)
(
  SELECT
    retirement_id,
    book_type_code,
    cost_retired,
    retirement_status,
    cost_of_removal,
    nbv_retired,
    gain_loss_amount,
    proceeds_of_sale,
    retirement_type_code,
    created_by,
    creation_date,
    unrevalued_cost_retired,
    retirement_prorate_convention,
    reval_reserve_retired,
    date_retired,
    reserve_retired,
    asset_id,
    adjust_last_update_date,
    last_updated_by,
    book_type_code_faj,
    adjustment_type,
    source_type_code,
    adjustment_amount,
    transaction_header_id,
    adjustment_line_id,
    code_combination_id,
    date_ineffective,
    location_id,
    distribution_id,
    transaction_units,
    last_update_date,
    transaction_date_entered,
    transaction_type_code,
    date_effective,
    asset_type,
    account,
    date_placed_in_service,
    PROCEEDS_CLR,
    cost,
    nbv,
    proceeds,
    removal,
    reval_rsv_ret,
    code,
    (
      SELECT
        system_id
      FROM bec_etl_ctrl.etlsourceappid
      WHERE
        source_system = 'EBS'
    ) || '-' || retirement_id AS retirement_id_key,
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
    ) || '-' || transaction_header_id AS transaction_header_id_key,
    (
      SELECT
        system_id
      FROM bec_etl_ctrl.etlsourceappid
      WHERE
        source_system = 'EBS'
    ) || '-' || adjustment_line_id AS adjustment_line_id_key,
    (
      SELECT
        system_id
      FROM bec_etl_ctrl.etlsourceappid
      WHERE
        source_system = 'EBS'
    ) || '-' || code_combination_id AS code_combination_id_key,
    (
      SELECT
        system_id
      FROM bec_etl_ctrl.etlsourceappid
      WHERE
        source_system = 'EBS'
    ) || '-' || location_id AS location_id_key,
    (
      SELECT
        system_id
      FROM bec_etl_ctrl.etlsourceappid
      WHERE
        source_system = 'EBS'
    ) || '-' || distribution_id AS distribution_id_key,
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
    ) || '-' || COALESCE(retirement_id, 0) || '-' || COALESCE(adjustment_line_id, 0) AS dw_load_id,
    CURRENT_TIMESTAMP() AS dw_insert_date,
    CURRENT_TIMESTAMP() AS dw_update_date
  FROM (
    SELECT
      fr.retirement_id,
      fr.book_type_code,
      fr.cost_retired,
      fr.status AS retirement_status,
      fr.cost_of_removal,
      fr.nbv_retired,
      fr.gain_loss_amount,
      fr.proceeds_of_sale,
      fr.retirement_type_code,
      fr.created_by,
      fr.creation_date,
      fr.unrevalued_cost_retired,
      fr.retirement_prorate_convention,
      fr.reval_reserve_retired,
      fr.date_retired,
      fr.reserve_retired,
      faj.asset_id,
      faj.last_update_date AS adjust_last_update_date,
      faj.last_updated_by,
      faj.book_type_code AS book_type_code_faj,
      faj.adjustment_type,
      faj.source_type_code,
      faj.adjustment_amount,
      faj.transaction_header_id,
      faj.adjustment_line_id,
      fdh.code_combination_id AS code_combination_id,
      COALESCE(fdh.date_ineffective, TO_DATE('01/01/9999 00:00:00', 'mm/dd/yyyy hh24:mi:ss')) AS date_ineffective,
      fdh.location_id,
      fdh.distribution_id,
      fdh.transaction_units,
      fdh.last_update_date,
      fth.transaction_date_entered,
      fth.transaction_type_code,
      fth.date_effective,
      ah.asset_type,
      CASE WHEN ah.asset_type = 'CIP' THEN cb.cip_cost_acct ELSE cb.asset_cost_acct END AS account,
      books.date_placed_in_service,
      (
        SELECT DISTINCT
          'PROCEEDS'
        FROM (
          SELECT
            *
          FROM silver_bec_ods.fa_adjustments
          WHERE
            is_deleted_flg <> 'Y'
        ) AS aj1
        WHERE
          aj1.book_type_code = faj.book_type_code
          AND aj1.asset_id = faj.asset_id
          AND aj1.transaction_header_id = faj.transaction_header_id
          AND aj1.adjustment_type = 'PROCEEDS CLR'
      ) AS PROCEEDS_CLR,
      CASE
        WHEN faj.adjustment_type = 'COST'
        THEN 1
        WHEN faj.adjustment_type = 'CIP COST'
        THEN 1
        ELSE 0
      END * CASE
        WHEN faj.debit_credit_flag = 'DR'
        THEN -1
        WHEN faj.debit_credit_flag = 'CR'
        THEN 1
        ELSE 0
      END * faj.adjustment_amount AS cost,
      CASE WHEN faj.adjustment_type = 'NBV RETIRED' THEN -1 ELSE 0 END * CASE
        WHEN faj.debit_credit_flag = 'DR'
        THEN -1
        WHEN faj.debit_credit_flag = 'CR'
        THEN 1
        ELSE 0
      END * faj.adjustment_amount AS nbv,
      CASE
        WHEN faj.adjustment_type = 'PROCEEDS CLR'
        THEN 1
        WHEN faj.adjustment_type = 'PROCEEDS'
        THEN 1
        ELSE 0
      END * CASE
        WHEN faj.debit_credit_flag = 'DR'
        THEN 1
        WHEN faj.debit_credit_flag = 'CR'
        THEN -1
        ELSE 0
      END * faj.adjustment_amount AS proceeds,
      CASE WHEN faj.adjustment_type = 'REMOVALCOST' THEN -1 ELSE 0 END * CASE
        WHEN faj.debit_credit_flag = 'DR'
        THEN -1
        WHEN faj.debit_credit_flag = 'CR'
        THEN 1
        ELSE 0
      END * faj.adjustment_amount AS removal,
      CASE WHEN faj.adjustment_type = 'REVAL RSV RET' THEN 1 ELSE 0 END * CASE
        WHEN faj.debit_credit_flag = 'DR'
        THEN -1
        WHEN faj.debit_credit_flag = 'CR'
        THEN 1
        ELSE 0
      END * faj.adjustment_amount AS reval_rsv_ret,
      CASE
        WHEN fth.transaction_type_code = 'REINSTATEMENT'
        THEN '*'
        WHEN fth.transaction_type_code = 'PARTIAL RETIREMENT'
        THEN 'P'
        ELSE CAST((
          NULL
        ) AS STRING)
      END AS code
    FROM (
      SELECT
        *
      FROM silver_bec_ods.fa_transaction_headers
      WHERE
        is_deleted_flg <> 'Y'
    ) AS fth, (
      SELECT
        *
      FROM silver_bec_ods.fa_books
      WHERE
        is_deleted_flg <> 'Y'
    ) AS books, (
      SELECT
        *
      FROM silver_bec_ods.fa_retirements
      WHERE
        is_deleted_flg <> 'Y'
    ) AS fr, (
      SELECT
        *
      FROM silver_bec_ods.fa_adjustments
      WHERE
        is_deleted_flg <> 'Y'
    ) AS faj, (
      SELECT
        *
      FROM silver_bec_ods.fa_distribution_history
      WHERE
        is_deleted_flg <> 'Y'
    ) AS fdh, (
      SELECT
        *
      FROM silver_bec_ods.fa_asset_history
      WHERE
        is_deleted_flg <> 'Y'
    ) AS ah, (
      SELECT
        *
      FROM silver_bec_ods.fa_category_books
      WHERE
        is_deleted_flg <> 'Y'
    ) AS cb
    WHERE
      fth.transaction_key = 'R'
      AND fth.book_type_code = fr.book_type_code
      AND fth.asset_id = fr.asset_id
      AND fr.asset_id = books.asset_id
      AND CASE
        WHEN fth.transaction_type_code = 'REINSTATEMENT'
        THEN fr.transaction_header_id_out
        ELSE fr.transaction_header_id_in
      END = fth.transaction_header_id
      AND faj.asset_id = fr.asset_id
      AND faj.book_type_code = fr.book_type_code
      AND faj.transaction_header_id = fth.transaction_header_id
      AND ah.asset_id = fth.asset_id
      AND ah.date_effective <= fth.date_effective
      AND COALESCE(ah.date_ineffective, fth.date_effective + 1) > fth.date_effective
      AND books.transaction_header_id_out = fth.transaction_header_id
      AND books.book_type_code = fth.book_type_code
      AND books.asset_id = fth.asset_id
      AND cb.category_id = ah.category_id
      AND cb.book_type_code = fr.book_type_code
      AND fdh.distribution_id = faj.distribution_id
      AND fth.asset_id = fdh.asset_id
      AND (
        fr.kca_seq_date > (
          SELECT
            (
              executebegints - prune_days
            )
          FROM bec_etl_ctrl.batch_dw_info
          WHERE
            dw_table_name = 'fact_fa_retirements' AND batch_name = 'fa'
        )
        OR faj.kca_seq_date > (
          SELECT
            (
              executebegints - prune_days
            )
          FROM bec_etl_ctrl.batch_dw_info
          WHERE
            dw_table_name = 'fact_fa_retirements' AND batch_name = 'fa'
        )
      ) /* AND faj.adjustment_type NOT IN  (select  'PROCEEDS' from fa_adjustments aj1
    				where aj1.book_type_code = faj.book_type_code
    				and aj1.asset_id = faj.asset_id
    				and aj1.transaction_header_id = faj.transaction_header_id
    			and aj1.adjustment_type = 'PROCEEDS CLR') */
    UNION
    SELECT
      fr.retirement_id,
      fr.book_type_code,
      fr.cost_retired,
      fr.status AS retirement_status,
      fr.cost_of_removal,
      fr.nbv_retired,
      fr.gain_loss_amount,
      fr.proceeds_of_sale,
      fr.retirement_type_code,
      fr.created_by,
      fr.creation_date,
      fr.unrevalued_cost_retired,
      fr.retirement_prorate_convention,
      fr.reval_reserve_retired,
      fr.date_retired,
      fr.reserve_retired,
      fr.asset_id,
      NULL AS adjust_last_update_date,
      NULL,
      NULL,
      NULL,
      NULL,
      NULL,
      NULL,
      NULL,
      fdh.code_combination_id AS code_combination_id,
      COALESCE(fdh.date_ineffective, TO_DATE('01/01/9999 00:00:00', 'mm/dd/yyyy hh24:mi:ss')) AS date_ineffective,
      fdh.location_id,
      fdh.distribution_id,
      fdh.transaction_units,
      fdh.last_update_date,
      fth.transaction_date_entered,
      fth.transaction_type_code,
      fth.date_effective,
      ah.asset_type,
      CASE WHEN ah.asset_type = 'CIP' THEN cb.cip_cost_acct ELSE cb.asset_cost_acct END AS account,
      books.date_placed_in_service,
      NULL AS PROCEEDS_CLR,
      0 AS cost, /* -sum colums */
      0 AS nbv,
      COALESCE(fr.proceeds_of_sale, 0) AS proceeds,
      COALESCE(fr.cost_of_removal, 0) AS removal,
      0 AS reval_rsv_ret,
      CASE WHEN fr.STATUS = 'DELETED' THEN '*' ELSE CAST((
        NULL
      ) AS STRING) END AS code
    FROM (
      SELECT
        *
      FROM silver_bec_ods.fa_transaction_headers
      WHERE
        is_deleted_flg <> 'Y'
    ) AS fth, (
      SELECT
        *
      FROM silver_bec_ods.fa_books
      WHERE
        is_deleted_flg <> 'Y'
    ) AS books, (
      SELECT
        *
      FROM silver_bec_ods.fa_retirements
      WHERE
        is_deleted_flg <> 'Y'
    ) AS fr, (
      SELECT
        DH.*,
        TH2.DATE_EFFECTIVE AS TH2_DATE,
        TH1.DATE_EFFECTIVE AS TH1_DATE
      FROM (
        SELECT
          *
        FROM silver_bec_ods.FA_TRANSACTION_HEADERS
        WHERE
          is_deleted_flg <> 'Y'
      ) AS TH1, (
        SELECT
          *
        FROM silver_bec_ods.FA_DISTRIBUTION_HISTORY
        WHERE
          is_deleted_flg <> 'Y'
      ) AS DH, (
        SELECT
          *
        FROM silver_bec_ods.FA_BOOK_CONTROLS
        WHERE
          is_deleted_flg <> 'Y'
      ) AS BC, (
        SELECT
          *
        FROM silver_bec_ods.FA_TRANSACTION_HEADERS
        WHERE
          is_deleted_flg <> 'Y'
      ) AS TH2
      WHERE
        TH1.BOOK_TYPE_CODE = DH.BOOK_TYPE_CODE
        AND TH1.TRANSACTION_TYPE_CODE IN ('FULL RETIREMENT', 'PARTIAL RETIREMENT')
        AND TH1.ASSET_ID = DH.ASSET_ID
        AND BC.BOOK_TYPE_CODE = th1.BOOK_TYPE_CODE
        AND bC.DISTRIBUTION_SOURCE_BOOK = DH.BOOK_TYPE_CODE
        AND TH1.DATE_EFFECTIVE <= COALESCE(DH.DATE_INEFFECTIVE, TH1.DATE_EFFECTIVE)
        AND TH1.ASSET_ID = TH2.ASSET_ID
        AND TH2.BOOK_TYPE_CODE = DH.BOOK_TYPE_CODE
        AND th2.TRANSACTION_TYPE_CODE = 'REINSTATEMENT'
        AND th2.date_effective >= dh.DATE_EFFECTIVE
    ) AS fdh, (
      SELECT
        *
      FROM silver_bec_ods.fa_asset_history
      WHERE
        is_deleted_flg <> 'Y'
    ) AS ah, (
      SELECT
        *
      FROM silver_bec_ods.fa_category_books
      WHERE
        is_deleted_flg <> 'Y'
    ) AS cb
    WHERE
      fth.transaction_key = 'R'
      AND fth.book_type_code = fr.book_type_code
      AND fth.asset_id = fr.asset_id
      AND fr.asset_id = books.asset_id
      AND fr.transaction_header_id_out = fth.transaction_header_id
      AND ah.asset_id = fth.asset_id
      AND ah.date_effective <= fth.date_effective
      AND COALESCE(ah.date_ineffective, fth.date_effective + 1) > fth.date_effective
      AND books.transaction_header_id_out = fth.transaction_header_id
      AND books.book_type_code = fth.book_type_code
      AND books.asset_id = fth.asset_id
      AND cb.category_id = ah.category_id
      AND cb.book_type_code = fr.book_type_code /* AND  fdh.distribution_id  = faj.distribution_id */
      AND fth.asset_id = fdh.asset_id
      AND FDH.BOOK_TYPE_CODE = fth.BOOK_TYPE_CODE
      AND fdh.TH2_DATE >= fth.date_effective
      AND fdh.TH1_DATE <= fth.date_effective
      AND fth.TRANSACTION_TYPE_CODE = 'REINSTATEMENT'
      AND fr.COST_RETIRED = 0
      AND fr.cost_of_removal = 0
      AND fr.proceeds_of_sale = 0
      AND (
        fr.kca_seq_date > (
          SELECT
            (
              executebegints - prune_days
            )
          FROM bec_etl_ctrl.batch_dw_info
          WHERE
            dw_table_name = 'fact_fa_retirements' AND batch_name = 'fa'
        )
      )
  )
);
UPDATE bec_etl_ctrl.batch_dw_info SET last_refresh_date = CURRENT_TIMESTAMP()
WHERE
  dw_table_name = 'fact_fa_retirements' AND batch_name = 'fa';