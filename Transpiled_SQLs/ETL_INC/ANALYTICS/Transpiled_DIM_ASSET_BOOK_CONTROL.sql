/* Delete Records */
DELETE FROM gold_bec_dwh.DIM_ASSET_BOOK_CONTROL
WHERE
  (COALESCE(ledger_id, 0), COALESCE(book_type_code, 'NA')) IN (
    SELECT
      COALESCE(ods.ledger_id, 0) AS ledger_id,
      COALESCE(ods.book_type_code, 'NA') AS book_type_code
    FROM gold_bec_dwh.DIM_ASSET_BOOK_CONTROL AS dw, (
      SELECT
        gl_ledgers.ledger_id,
        fa_book_controls.book_type_code
      FROM silver_bec_ods.gl_ledgers AS gl_ledgers, silver_bec_ods.fa_book_controls AS fa_book_controls
      WHERE
        1 = 1
        AND fa_book_controls.set_of_books_id = gl_ledgers.ledger_id
        AND (
          gl_ledgers.kca_seq_date > (
            SELECT
              (
                executebegints - prune_days
              )
            FROM bec_etl_ctrl.batch_dw_info
            WHERE
              dw_table_name = 'dim_asset_book_control' AND batch_name = 'fa'
          )
          OR fa_book_controls.kca_seq_date > (
            SELECT
              (
                executebegints - prune_days
              )
            FROM bec_etl_ctrl.batch_dw_info
            WHERE
              dw_table_name = 'dim_asset_book_control' AND batch_name = 'fa'
          )
        )
      UNION ALL
      SELECT
        gl_ledgers.ledger_id,
        fa_mc_book_controls.book_type_code
      FROM silver_bec_ods.fa_mc_book_controls AS fa_mc_book_controls, silver_bec_ods.fa_book_controls AS fa_book_controls, silver_bec_ods.gl_ledgers AS gl_ledgers
      WHERE
        1 = 1
        AND gl_ledgers.ledger_id = fa_mc_book_controls.set_of_books_id
        AND fa_book_controls.book_type_code = fa_mc_book_controls.book_type_code
        AND (
          fa_book_controls.kca_seq_date > (
            SELECT
              (
                executebegints - prune_days
              )
            FROM bec_etl_ctrl.batch_dw_info
            WHERE
              dw_table_name = 'dim_asset_book_control' AND batch_name = 'fa'
          )
          OR gl_ledgers.kca_seq_date > (
            SELECT
              (
                executebegints - prune_days
              )
            FROM bec_etl_ctrl.batch_dw_info
            WHERE
              dw_table_name = 'dim_asset_book_control' AND batch_name = 'fa'
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
      ) || '-' || COALESCE(ods.ledger_id, 0) || '-' || COALESCE(ods.book_type_code, 'NA')
  );
/* Insert Records */
INSERT INTO gold_bec_dwh.DIM_ASSET_BOOK_CONTROL (
  book_type_code,
  book_class,
  last_update_date,
  creation_date,
  name,
  currency_code,
  ledger_id,
  is_deleted_flg,
  source_app_id,
  dw_load_id,
  dw_insert_date,
  dw_update_date
)
(
  SELECT
    book_type_code,
    book_class,
    last_update_date,
    creation_date,
    name,
    currency_code,
    ledger_id,
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
    ) || '-' || COALESCE(ledger_id, 0) || '-' || COALESCE(book_type_code, 'NA') AS dw_load_id,
    CURRENT_TIMESTAMP() AS dw_insert_date,
    CURRENT_TIMESTAMP() AS dw_update_date
  FROM (
    SELECT
      fa_book_controls.book_type_code,
      fa_book_controls.book_class,
      fa_book_controls.last_update_date,
      fa_book_controls.creation_date,
      gl_ledgers.name,
      gl_ledgers.currency_code,
      gl_ledgers.ledger_id
    FROM silver_bec_ods.gl_ledgers AS gl_ledgers, silver_bec_ods.fa_book_controls AS fa_book_controls
    WHERE
      1 = 1
      AND fa_book_controls.set_of_books_id = gl_ledgers.ledger_id
      AND (
        fa_book_controls.kca_seq_date > (
          SELECT
            (
              executebegints - prune_days
            )
          FROM bec_etl_ctrl.batch_dw_info
          WHERE
            dw_table_name = 'dim_asset_book_control' AND batch_name = 'fa'
        )
        OR gl_ledgers.kca_seq_date > (
          SELECT
            (
              executebegints - prune_days
            )
          FROM bec_etl_ctrl.batch_dw_info
          WHERE
            dw_table_name = 'dim_asset_book_control' AND batch_name = 'fa'
        )
      )
    UNION ALL
    SELECT
      fa_mc_book_controls.book_type_code,
      fa_book_controls.book_class,
      fa_mc_book_controls.last_update_date,
      fa_book_controls.creation_date,
      gl_ledgers.name,
      gl_ledgers.currency_code,
      gl_ledgers.ledger_id
    FROM silver_bec_ods.fa_mc_book_controls AS fa_mc_book_controls, silver_bec_ods.fa_book_controls AS fa_book_controls, silver_bec_ods.gl_ledgers AS gl_ledgers
    WHERE
      1 = 1
      AND gl_ledgers.ledger_id = fa_mc_book_controls.set_of_books_id
      AND fa_book_controls.book_type_code = fa_mc_book_controls.book_type_code
      AND (
        fa_book_controls.kca_seq_date > (
          SELECT
            (
              executebegints - prune_days
            )
          FROM bec_etl_ctrl.batch_dw_info
          WHERE
            dw_table_name = 'dim_asset_book_control' AND batch_name = 'fa'
        )
        OR gl_ledgers.kca_seq_date > (
          SELECT
            (
              executebegints - prune_days
            )
          FROM bec_etl_ctrl.batch_dw_info
          WHERE
            dw_table_name = 'dim_asset_book_control' AND batch_name = 'fa'
        )
      )
  )
);
/* Soft Delete */
UPDATE gold_bec_dwh.DIM_ASSET_BOOK_CONTROL SET is_deleted_flg = 'Y'
WHERE
  NOT (COALESCE(ledger_id, 0), COALESCE(book_type_code, 'NA')) IN (
    SELECT
      COALESCE(ods.ledger_id, 0) AS ledger_id,
      COALESCE(ods.book_type_code, 'NA') AS book_type_code
    FROM gold_bec_dwh.DIM_ASSET_BOOK_CONTROL AS dw, (
      SELECT
        gl_ledgers.ledger_id,
        fa_book_controls.book_type_code
      FROM (
        SELECT
          *
        FROM silver_bec_ods.gl_ledgers
        WHERE
          is_deleted_flg <> 'Y'
      ) AS gl_ledgers, (
        SELECT
          *
        FROM silver_bec_ods.fa_book_controls
        WHERE
          is_deleted_flg <> 'Y'
      ) AS fa_book_controls
      WHERE
        1 = 1 AND fa_book_controls.set_of_books_id = gl_ledgers.ledger_id
      UNION ALL
      SELECT
        gl_ledgers.ledger_id,
        fa_mc_book_controls.book_type_code
      FROM (
        SELECT
          *
        FROM silver_bec_ods.fa_mc_book_controls
        WHERE
          is_deleted_flg <> 'Y'
      ) AS fa_mc_book_controls, (
        SELECT
          *
        FROM silver_bec_ods.fa_book_controls
        WHERE
          is_deleted_flg <> 'Y'
      ) AS fa_book_controls, (
        SELECT
          *
        FROM silver_bec_ods.gl_ledgers
        WHERE
          is_deleted_flg <> 'Y'
      ) AS gl_ledgers
      WHERE
        1 = 1
        AND gl_ledgers.ledger_id = fa_mc_book_controls.set_of_books_id
        AND fa_book_controls.book_type_code = fa_mc_book_controls.book_type_code
    ) AS ods
    WHERE
      dw.dw_load_id = (
        SELECT
          system_id
        FROM bec_etl_ctrl.etlsourceappid
        WHERE
          source_system = 'EBS'
      ) || '-' || COALESCE(ods.ledger_id, 0) || '-' || COALESCE(ods.book_type_code, 'NA')
  );
UPDATE bec_etl_ctrl.batch_dw_info SET load_type = 'I', last_refresh_date = CURRENT_TIMESTAMP()
WHERE
  dw_table_name = 'dim_asset_book_control' AND batch_name = 'fa';