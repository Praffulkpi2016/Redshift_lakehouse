DROP table IF EXISTS gold_bec_dwh.dim_fa_book_type;
CREATE TABLE gold_bec_dwh.dim_fa_book_type AS
(
  SELECT
    fbc.book_type_code,
    fbc.book_type_name,
    fbc.set_of_books_id,
    fbc.last_deprn_run_date,
    fbc.book_class,
    fbc.last_update_date,
    fbc.last_updated_by,
    fbc.created_by,
    fbc.creation_date,
    fbc.date_ineffective,
    COALESCE(fbc.gl_posting_allowed_flag, 'NO') AS gl_posting_allowed_flag,
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
    ) || '-' || COALESCE(fbc.book_type_code, 'NA') || '-' || COALESCE(fbc.set_of_books_id, 0) AS dw_load_id,
    CURRENT_TIMESTAMP() AS dw_insert_date,
    CURRENT_TIMESTAMP() AS dw_update_date
  FROM silver_bec_ods.fa_book_controls AS fbc
  WHERE
    1 = 1 AND fbc.date_ineffective IS NULL
);
UPDATE bec_etl_ctrl.batch_dw_info SET load_type = 'I', last_refresh_date = CURRENT_TIMESTAMP()
WHERE
  dw_table_name = 'dim_fa_book_type' AND batch_name = 'fa';