/* Delete Records */
DELETE FROM gold_bec_dwh.dim_fa_book_type
WHERE
  (COALESCE(book_type_code, 'NA'), COALESCE(set_of_books_id, 0)) IN (
    SELECT
      COALESCE(ods.book_type_code, 'NA') AS book_type_code,
      COALESCE(ods.set_of_books_id, 0) AS set_of_books_id
    FROM gold_bec_dwh.dim_fa_book_type AS dw, (
      SELECT
        fbc.book_type_code,
        fbc.set_of_books_id
      FROM silver_bec_ods.fa_book_controls AS fbc
      WHERE
        1 = 1
        AND fbc.date_ineffective IS NULL
        AND (
          fbc.kca_seq_date > (
            SELECT
              (
                executebegints - prune_days
              )
            FROM bec_etl_ctrl.batch_dw_info
            WHERE
              dw_table_name = 'dim_fa_book_type' AND batch_name = 'fa'
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
      ) || '-' || COALESCE(ods.book_type_code, 'NA') || '-' || COALESCE(ods.set_of_books_id, 0)
  );
/* Insert records */
INSERT INTO gold_bec_dwh.dim_fa_book_type (
  book_type_code,
  book_type_name,
  set_of_books_id,
  last_deprn_run_date,
  book_class,
  last_update_date,
  last_updated_by,
  created_by,
  creation_date,
  date_ineffective,
  gl_posting_allowed_flag,
  is_deleted_flg,
  source_app_id,
  dw_load_id,
  dw_insert_date,
  dw_update_date
)
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
    1 = 1
    AND fbc.date_ineffective IS NULL
    AND (
      fbc.kca_seq_date > (
        SELECT
          (
            executebegints - prune_days
          )
        FROM bec_etl_ctrl.batch_dw_info
        WHERE
          dw_table_name = 'dim_fa_book_type' AND batch_name = 'fa'
      )
    )
);
/* Soft delete */
UPDATE gold_bec_dwh.dim_fa_book_type SET is_deleted_flg = 'Y'
WHERE
  NOT (COALESCE(book_type_code, 'NA'), COALESCE(set_of_books_id, 0)) IN (
    SELECT
      COALESCE(ods.book_type_code, 'NA') AS book_type_code,
      COALESCE(ods.set_of_books_id, 0) AS set_of_books_id
    FROM gold_bec_dwh.dim_fa_book_type AS dw, (
      SELECT
        fbc.book_type_code,
        fbc.set_of_books_id
      FROM silver_bec_ods.fa_book_controls AS fbc
      WHERE
        is_deleted_flg <> 'Y' AND 1 = 1 AND fbc.date_ineffective IS NULL
    ) AS ods
    WHERE
      dw.dw_load_id = (
        SELECT
          system_id
        FROM bec_etl_ctrl.etlsourceappid
        WHERE
          source_system = 'EBS'
      ) || '-' || COALESCE(ods.book_type_code, 'NA') || '-' || COALESCE(ods.set_of_books_id, 0)
  );
UPDATE bec_etl_ctrl.batch_dw_info SET last_refresh_date = CURRENT_TIMESTAMP()
WHERE
  dw_table_name = 'dim_fa_book_type' AND batch_name = 'fa';