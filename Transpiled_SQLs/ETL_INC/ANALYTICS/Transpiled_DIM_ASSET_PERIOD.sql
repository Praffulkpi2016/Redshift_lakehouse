/* Delete Records */
DELETE FROM gold_bec_dwh.DIM_ASSET_PERIOD
WHERE
  (COALESCE(SET_OF_BOOKS_ID, 0), COALESCE(BOOK_TYPE_CODE, 'NA'), COALESCE(PERIOD_COUNTER, 0)) IN (
    SELECT
      COALESCE(ods.SET_OF_BOOKS_ID, 0) AS SET_OF_BOOKS_ID,
      COALESCE(ods.BOOK_TYPE_CODE, 'NA') AS BOOK_TYPE_CODE,
      COALESCE(ods.PERIOD_COUNTER, 0) AS PERIOD_COUNTER
    FROM gold_bec_dwh.DIM_ASSET_PERIOD AS dw, (
      SELECT
        FA_BOOK_CONTROLS.SET_OF_BOOKS_ID AS SET_OF_BOOKS_ID,
        FA_DEPRN_PERIODS.BOOK_TYPE_CODE AS BOOK_TYPE_CODE,
        FA_DEPRN_PERIODS.PERIOD_COUNTER AS PERIOD_COUNTER
      FROM silver_bec_ods.FA_DEPRN_PERIODS AS FA_DEPRN_PERIODS
      INNER JOIN silver_bec_ods.FA_BOOK_CONTROLS AS FA_BOOK_CONTROLS
        ON FA_DEPRN_PERIODS.BOOK_TYPE_CODE = FA_BOOK_CONTROLS.BOOK_TYPE_CODE
        AND (
          FA_BOOK_CONTROLS.kca_seq_date > (
            SELECT
              (
                executebegints - prune_days
              )
            FROM bec_etl_ctrl.batch_dw_info
            WHERE
              dw_table_name = 'dim_asset_period' AND batch_name = 'fa'
          )
        )
      UNION ALL
      SELECT
        FA_MC_DEPRN_PERIODS.set_of_books_id AS SET_OF_BOOKS_ID,
        FA_MC_DEPRN_PERIODS.BOOK_TYPE_CODE AS BOOK_TYPE_CODE,
        FA_MC_DEPRN_PERIODS.PERIOD_COUNTER AS PERIOD_COUNTER
      FROM silver_bec_ods.FA_MC_BOOK_CONTROLS AS FA_MC_BOOK_CONTROLS
      INNER JOIN silver_bec_ods.FA_MC_DEPRN_PERIODS AS FA_MC_DEPRN_PERIODS
        ON FA_MC_BOOK_CONTROLS.BOOK_TYPE_CODE = FA_MC_DEPRN_PERIODS.BOOK_TYPE_CODE
        AND (
          FA_MC_BOOK_CONTROLS.kca_seq_date > (
            SELECT
              (
                executebegints - prune_days
              )
            FROM bec_etl_ctrl.batch_dw_info
            WHERE
              dw_table_name = 'dim_asset_period' AND batch_name = 'fa'
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
      ) || '-' || COALESCE(ods.SET_OF_BOOKS_ID, 0) || '-' || COALESCE(ods.BOOK_TYPE_CODE, 'NA') || '-' || COALESCE(ods.PERIOD_COUNTER, 0)
  );
/* Insert Records */
INSERT INTO gold_bec_dwh.DIM_ASSET_PERIOD (
  book_type_code,
  period_name,
  period_counter,
  fiscal_year,
  period_num,
  period_open_date,
  period_close_date,
  set_of_books_id,
  is_deleted_flg,
  source_app_id,
  dw_load_id,
  dw_insert_date,
  dw_update_date
)
(
  SELECT
    book_type_code,
    period_name,
    period_counter,
    fiscal_year,
    period_num,
    period_open_date,
    period_close_date,
    set_of_books_id,
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
    ) || '-' || COALESCE(SET_OF_BOOKS_ID, 0) || '-' || COALESCE(BOOK_TYPE_CODE, 'NA') || '-' || COALESCE(PERIOD_COUNTER, 0) AS dw_load_id,
    CURRENT_TIMESTAMP() AS dw_insert_date,
    CURRENT_TIMESTAMP() AS dw_update_date
  FROM (
    SELECT
      FA_DEPRN_PERIODS.BOOK_TYPE_CODE AS BOOK_TYPE_CODE,
      FA_DEPRN_PERIODS.PERIOD_NAME AS PERIOD_NAME,
      FA_DEPRN_PERIODS.PERIOD_COUNTER AS PERIOD_COUNTER,
      FA_DEPRN_PERIODS.FISCAL_YEAR AS FISCAL_YEAR,
      FA_DEPRN_PERIODS.PERIOD_NUM AS PERIOD_NUM,
      FA_DEPRN_PERIODS.PERIOD_OPEN_DATE AS PERIOD_OPEN_DATE,
      FA_DEPRN_PERIODS.PERIOD_CLOSE_DATE AS PERIOD_CLOSE_DATE,
      FA_BOOK_CONTROLS.SET_OF_BOOKS_ID AS SET_OF_BOOKS_ID
    FROM silver_bec_ods.FA_DEPRN_PERIODS AS FA_DEPRN_PERIODS
    INNER JOIN silver_bec_ods.FA_BOOK_CONTROLS AS FA_BOOK_CONTROLS
      ON FA_DEPRN_PERIODS.BOOK_TYPE_CODE = FA_BOOK_CONTROLS.BOOK_TYPE_CODE
      AND (
        FA_BOOK_CONTROLS.kca_seq_date > (
          SELECT
            (
              executebegints - prune_days
            )
          FROM bec_etl_ctrl.batch_dw_info
          WHERE
            dw_table_name = 'dim_asset_period' AND batch_name = 'fa'
        )
      )
    UNION ALL
    SELECT
      FA_MC_DEPRN_PERIODS.BOOK_TYPE_CODE AS BOOK_TYPE_CODE,
      FA_MC_DEPRN_PERIODS.PERIOD_NAME AS PERIOD_NAME,
      FA_MC_DEPRN_PERIODS.PERIOD_COUNTER AS PERIOD_COUNTER,
      FA_MC_DEPRN_PERIODS.FISCAL_YEAR AS FISCAL_YEAR,
      FA_MC_DEPRN_PERIODS.PERIOD_NUM AS PERIOD_NUM,
      FA_MC_DEPRN_PERIODS.PERIOD_OPEN_DATE AS PERIOD_OPEN_DATE,
      FA_MC_DEPRN_PERIODS.PERIOD_CLOSE_DATE AS PERIOD_CLOSE_DATE,
      FA_MC_DEPRN_PERIODS.set_of_books_id AS SET_OF_BOOKS_ID
    FROM silver_bec_ods.FA_MC_BOOK_CONTROLS AS FA_MC_BOOK_CONTROLS
    INNER JOIN silver_bec_ods.FA_MC_DEPRN_PERIODS AS FA_MC_DEPRN_PERIODS
      ON FA_MC_BOOK_CONTROLS.BOOK_TYPE_CODE = FA_MC_DEPRN_PERIODS.BOOK_TYPE_CODE
      AND (
        FA_MC_BOOK_CONTROLS.kca_seq_date > (
          SELECT
            (
              executebegints - prune_days
            )
          FROM bec_etl_ctrl.batch_dw_info
          WHERE
            dw_table_name = 'dim_asset_period' AND batch_name = 'fa'
        )
      )
  )
);
/* Soft Delete */
UPDATE gold_bec_dwh.DIM_ASSET_PERIOD SET is_deleted_flg = 'Y'
WHERE
  NOT (COALESCE(SET_OF_BOOKS_ID, 0), COALESCE(BOOK_TYPE_CODE, 'NA'), COALESCE(PERIOD_COUNTER, 0)) IN (
    SELECT
      COALESCE(ods.SET_OF_BOOKS_ID, 0) AS SET_OF_BOOKS_ID,
      COALESCE(ods.BOOK_TYPE_CODE, 'NA') AS BOOK_TYPE_CODE,
      COALESCE(ods.PERIOD_COUNTER, 0) AS PERIOD_COUNTER
    FROM gold_bec_dwh.DIM_ASSET_PERIOD AS dw, (
      SELECT
        FA_BOOK_CONTROLS.SET_OF_BOOKS_ID AS SET_OF_BOOKS_ID,
        FA_DEPRN_PERIODS.BOOK_TYPE_CODE AS BOOK_TYPE_CODE,
        FA_DEPRN_PERIODS.PERIOD_COUNTER AS PERIOD_COUNTER
      FROM (
        SELECT
          *
        FROM silver_bec_ods.FA_DEPRN_PERIODS
        WHERE
          is_deleted_flg <> 'Y'
      ) AS FA_DEPRN_PERIODS
      INNER JOIN (
        SELECT
          *
        FROM silver_bec_ods.FA_BOOK_CONTROLS
        WHERE
          is_deleted_flg <> 'Y'
      ) AS FA_BOOK_CONTROLS
        ON FA_DEPRN_PERIODS.BOOK_TYPE_CODE = FA_BOOK_CONTROLS.BOOK_TYPE_CODE
      UNION ALL
      SELECT
        FA_MC_DEPRN_PERIODS.set_of_books_id AS SET_OF_BOOKS_ID,
        FA_MC_DEPRN_PERIODS.BOOK_TYPE_CODE AS BOOK_TYPE_CODE,
        FA_MC_DEPRN_PERIODS.PERIOD_COUNTER AS PERIOD_COUNTER
      FROM (
        SELECT
          *
        FROM silver_bec_ods.FA_MC_BOOK_CONTROLS
        WHERE
          is_deleted_flg <> 'Y'
      ) AS FA_MC_BOOK_CONTROLS
      INNER JOIN (
        SELECT
          *
        FROM silver_bec_ods.FA_MC_DEPRN_PERIODS
        WHERE
          is_deleted_flg <> 'Y'
      ) AS FA_MC_DEPRN_PERIODS
        ON FA_MC_BOOK_CONTROLS.BOOK_TYPE_CODE = FA_MC_DEPRN_PERIODS.BOOK_TYPE_CODE
    ) AS ods
    WHERE
      dw.dw_load_id = (
        SELECT
          system_id
        FROM bec_etl_ctrl.etlsourceappid
        WHERE
          source_system = 'EBS'
      ) || '-' || COALESCE(ods.SET_OF_BOOKS_ID, 0) || '-' || COALESCE(ods.BOOK_TYPE_CODE, 'NA') || '-' || COALESCE(ods.PERIOD_COUNTER, 0)
  );
UPDATE bec_etl_ctrl.batch_dw_info SET load_type = 'I', last_refresh_date = CURRENT_TIMESTAMP()
WHERE
  dw_table_name = 'dim_asset_period' AND batch_name = 'fa';