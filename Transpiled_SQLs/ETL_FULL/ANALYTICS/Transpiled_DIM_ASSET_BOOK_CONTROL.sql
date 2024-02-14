DROP table IF EXISTS gold_bec_dwh.DIM_ASSET_BOOK_CONTROL;
CREATE TABLE gold_bec_dwh.DIM_ASSET_BOOK_CONTROL AS
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
      1 = 1 AND fa_book_controls.set_of_books_id = gl_ledgers.ledger_id
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
  )
);
UPDATE bec_etl_ctrl.batch_dw_info SET load_type = 'I', last_refresh_date = CURRENT_TIMESTAMP()
WHERE
  dw_table_name = 'dim_asset_book_control' AND batch_name = 'fa';