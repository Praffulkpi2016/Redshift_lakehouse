/* Delete Records */
DELETE FROM gold_bec_dwh.FACT_FA_ASSET_RESERVES
WHERE
  (COALESCE(TRANSACTION_HEADER_ID, 0), COALESCE(ADJUSTMENT_LINE_ID, 0), COALESCE(DISTRIBUTION_ID, 0)) IN (
    SELECT
      COALESCE(ods.TRANSACTION_HEADER_ID, 0) AS TRANSACTION_HEADER_ID,
      COALESCE(ods.ADJUSTMENT_LINE_ID, 0) AS ADJUSTMENT_LINE_ID,
      COALESCE(ods.DISTRIBUTION_ID, 0) AS DISTRIBUTION_ID
    FROM gold_bec_dwh.FACT_FA_ASSET_RESERVES AS dw, (
      SELECT
        DH.DISTRIBUTION_ID,
        AJ.ADJUSTMENT_LINE_ID,
        TH.TRANSACTION_HEADER_ID
      FROM BEC_ODS.FA_ADJUSTMENTS AS AJ, BEC_ODS.FA_LOOKUPS_TL AS RT, BEC_ODS.FA_DISTRIBUTION_HISTORY AS DH, BEC_ODS.FA_ASSET_HISTORY AS AH, BEC_ODS.FA_TRANSACTION_HEADERS AS TH, BEC_ODS.XLA_AE_HEADERS AS HEADERS, BEC_ODS.XLA_AE_LINES AS LINES, BEC_ODS.XLA_DISTRIBUTION_LINKS AS LINKS
      WHERE
        1 = 1
        AND AJ.ADJUSTMENT_TYPE = RT.LOOKUP_CODE
        AND RT.LOOKUP_TYPE = 'REPORT TYPE'
        AND RT.LOOKUP_CODE = 'RESERVE'
        AND AJ.ASSET_ID = DH.ASSET_ID
        AND AJ.DISTRIBUTION_ID = DH.DISTRIBUTION_ID
        AND AJ.ADJUSTMENT_TYPE IN ('RESERVE', CASE WHEN 'RESERVE' = 'REVAL RESERVE' THEN 'REVAL AMORT' END)
        AND AH.ASSET_ID = DH.ASSET_ID
        AND (
          (
            AH.ASSET_TYPE <> 'EXPENSED' AND 'RESERVE' IN ('COST', 'CIP COST')
          )
          OR (
            AH.ASSET_TYPE IN ('CAPITALIZED', 'CIP') AND 'RESERVE' IN ('RESERVE', 'REVAL RESERVE')
          )
        )
        AND AJ.TRANSACTION_HEADER_ID = TH.TRANSACTION_HEADER_ID
        AND TH.TRANSACTION_HEADER_ID BETWEEN AH.TRANSACTION_HEADER_ID_IN AND COALESCE(AH.TRANSACTION_HEADER_ID_OUT - 1, TH.TRANSACTION_HEADER_ID)
        AND (
          CASE
            WHEN RT.LOOKUP_CODE = AJ.ADJUSTMENT_TYPE
            OR (
              RT.LOOKUP_CODE IS NULL AND AJ.ADJUSTMENT_TYPE IS NULL
            )
            THEN 1
            ELSE 0
          END * AJ.ADJUSTMENT_AMOUNT
        ) <> 0
        AND LINKS.SOURCE_DISTRIBUTION_ID_NUM_1 = AJ.TRANSACTION_HEADER_ID
        AND LINKS.SOURCE_DISTRIBUTION_ID_NUM_2 = AJ.ADJUSTMENT_LINE_ID
        AND LINKS.APPLICATION_ID = 140
        AND LINKS.SOURCE_DISTRIBUTION_TYPE = 'TRX'
        AND HEADERS.APPLICATION_ID = 140
        AND HEADERS.AE_HEADER_ID = LINKS.AE_HEADER_ID
        AND LINES.AE_HEADER_ID = LINKS.AE_HEADER_ID
        AND LINES.AE_LINE_NUM = LINKS.AE_LINE_NUM
        AND LINES.APPLICATION_ID = 140
        AND (
          AJ.kca_seq_date >= (
            SELECT
              (
                executebegints - prune_days
              )
            FROM bec_etl_ctrl.batch_dw_info
            WHERE
              dw_table_name = 'fact_fa_asset_reserves' AND batch_name = 'fa'
          )
          OR dh.kca_seq_date >= (
            SELECT
              (
                executebegints - prune_days
              )
            FROM bec_etl_ctrl.batch_dw_info
            WHERE
              dw_table_name = 'fact_fa_asset_reserves' AND batch_name = 'fa'
          )
          OR AH.kca_seq_date >= (
            SELECT
              (
                executebegints - prune_days
              )
            FROM bec_etl_ctrl.batch_dw_info
            WHERE
              dw_table_name = 'fact_fa_asset_reserves' AND batch_name = 'fa'
          )
          OR TH.kca_seq_date >= (
            SELECT
              (
                executebegints - prune_days
              )
            FROM bec_etl_ctrl.batch_dw_info
            WHERE
              dw_table_name = 'fact_fa_asset_reserves' AND batch_name = 'fa'
          )
          OR HEADERS.kca_seq_date >= (
            SELECT
              (
                executebegints - prune_days
              )
            FROM bec_etl_ctrl.batch_dw_info
            WHERE
              dw_table_name = 'fact_fa_asset_reserves' AND batch_name = 'fa'
          )
          OR LINES.kca_seq_date >= (
            SELECT
              (
                executebegints - prune_days
              )
            FROM bec_etl_ctrl.batch_dw_info
            WHERE
              dw_table_name = 'fact_fa_asset_reserves' AND batch_name = 'fa'
          )
          OR AJ.is_deleted_flg = 'Y'
          OR RT.is_deleted_flg = 'Y'
          OR dh.is_deleted_flg = 'Y'
          OR AH.is_deleted_flg = 'Y'
          OR TH.is_deleted_flg = 'Y'
          OR HEADERS.is_deleted_flg = 'Y'
          OR LINES.is_deleted_flg = 'Y'
          OR LINKS.is_deleted_flg = 'Y'
        )
    ) AS ods
    WHERE
      1 = 1
      AND dw.dw_load_id = (
        SELECT
          system_id
        FROM bec_etl_ctrl.etlsourceappid
        WHERE
          source_system = 'EBS'
      ) || '-' || COALESCE(ods.TRANSACTION_HEADER_ID, 0) || '-' || COALESCE(ods.ADJUSTMENT_LINE_ID, 0) || '-' || COALESCE(ods.DISTRIBUTION_ID, 0)
  );
/* Insert Records */
INSERT INTO gold_bec_dwh.FACT_FA_ASSET_RESERVES (
  ASSET_ID,
  DISTRIBUTION_ID,
  ADJUSTMENT_LINE_ID,
  TRANSACTION_HEADER_ID,
  LEDGER_ID,
  DISTRIBUTION_CCID,
  ADJUSTMENT_CCID,
  CATEGORY_BOOKS_ACCOUNT,
  SOURCE_TYPE_CODE,
  BOOK_TYPE_CODE,
  REPORT_TYPE,
  PERIOD_COUNTER_CREATED,
  BEGING,
  ADDITION,
  DEPRECIATION,
  RECLASS,
  RETIREMENT,
  REVALUATION,
  TAX,
  TRANSFER,
  ENDING,
  is_deleted_flg,
  SOURCE_APP_ID,
  DW_LOAD_ID,
  DW_INSERT_DATE,
  DW_UPDATE_DATE
)
(
  SELECT
    ASSET_ID,
    DISTRIBUTION_ID,
    ADJUSTMENT_LINE_ID,
    TRANSACTION_HEADER_ID,
    LEDGER_ID,
    DISTRIBUTION_CCID,
    ADJUSTMENT_CCID,
    CATEGORY_BOOKS_ACCOUNT,
    SOURCE_TYPE_CODE,
    BOOK_TYPE_CODE,
    REPORT_TYPE,
    PERIOD_COUNTER_CREATED,
    ROUND(
      (
        CASE WHEN SOURCE_TYPE_CODE = 'BEGIN' THEN COALESCE(AMOUNT, 0) ELSE NULL END
      ),
      2
    ) AS BEGING,
    ROUND(
      (
        CASE WHEN SOURCE_TYPE_CODE = 'ADDITION' THEN COALESCE(AMOUNT, 0) ELSE NULL END
      ),
      2
    ) AS ADDITION,
    ROUND(
      (
        CASE WHEN SOURCE_TYPE_CODE = 'DEPRECIATION' THEN COALESCE(AMOUNT, 0) ELSE NULL END
      ),
      2
    ) AS DEPRECIATION,
    ROUND(
      (
        CASE WHEN SOURCE_TYPE_CODE = 'RECLASS' THEN COALESCE(AMOUNT, 0) ELSE NULL END
      ),
      2
    ) AS RECLASS,
    ROUND(
      (
        CASE WHEN SOURCE_TYPE_CODE = 'RETIREMENT' THEN -COALESCE(AMOUNT, 0) ELSE NULL END
      ),
      2
    ) AS RETIREMENT,
    ROUND(
      (
        CASE WHEN SOURCE_TYPE_CODE = 'REVALUATION' THEN COALESCE(AMOUNT, 0) ELSE NULL END
      ),
      2
    ) AS REVALUATION,
    ROUND((
      CASE WHEN SOURCE_TYPE_CODE = 'TAX' THEN COALESCE(AMOUNT, 0) ELSE NULL END
    ), 2) AS TAX,
    ROUND(
      (
        CASE WHEN SOURCE_TYPE_CODE = 'TRANSFER' THEN COALESCE(AMOUNT, 0) ELSE NULL END
      ),
      2
    ) AS TRANSFER,
    ROUND((
      CASE WHEN SOURCE_TYPE_CODE = 'END' THEN COALESCE(AMOUNT, 0) ELSE NULL END
    ), 2) AS ENDING,
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
    ) || '-' || COALESCE(TRANSACTION_HEADER_ID, 0) || '-' || COALESCE(ADJUSTMENT_LINE_ID, 0) || '-' || COALESCE(DISTRIBUTION_ID, 0) AS dw_load_id,
    CURRENT_TIMESTAMP() AS dw_insert_date,
    CURRENT_TIMESTAMP() AS dw_update_date
  FROM (
    SELECT
      DH.ASSET_ID,
      DH.DISTRIBUTION_ID,
      AJ.ADJUSTMENT_LINE_ID,
      TH.TRANSACTION_HEADER_ID,
      HEADERS.LEDGER_ID,
      DH.CODE_COMBINATION_ID AS DISTRIBUTION_CCID,
      LINES.CODE_COMBINATION_ID AS ADJUSTMENT_CCID,
      NULL AS CATEGORY_BOOKS_ACCOUNT,
      AJ.SOURCE_TYPE_CODE,
      DH.BOOK_TYPE_CODE,
      RT.LOOKUP_CODE AS REPORT_TYPE,
      AJ.PERIOD_COUNTER_CREATED,
      (
        CASE WHEN AJ.DEBIT_CREDIT_FLAG = 'DR' THEN 1 ELSE -1 END * AJ.ADJUSTMENT_AMOUNT
      ) AS AMOUNT
    FROM (
      SELECT
        *
      FROM BEC_ODS.FA_ADJUSTMENTS
      WHERE
        is_deleted_flg <> 'Y'
    ) AS AJ, (
      SELECT
        *
      FROM BEC_ODS.FA_LOOKUPS_TL
      WHERE
        is_deleted_flg <> 'Y'
    ) AS RT, (
      SELECT
        *
      FROM BEC_ODS.FA_DISTRIBUTION_HISTORY
      WHERE
        is_deleted_flg <> 'Y'
    ) AS DH, (
      SELECT
        *
      FROM BEC_ODS.FA_ASSET_HISTORY
      WHERE
        is_deleted_flg <> 'Y'
    ) AS AH, (
      SELECT
        *
      FROM BEC_ODS.FA_TRANSACTION_HEADERS
      WHERE
        is_deleted_flg <> 'Y'
    ) AS TH, (
      SELECT
        *
      FROM BEC_ODS.XLA_AE_HEADERS
      WHERE
        is_deleted_flg <> 'Y'
    ) AS HEADERS, (
      SELECT
        *
      FROM BEC_ODS.XLA_AE_LINES
      WHERE
        is_deleted_flg <> 'Y'
    ) AS LINES, (
      SELECT
        *
      FROM BEC_ODS.XLA_DISTRIBUTION_LINKS
      WHERE
        is_deleted_flg <> 'Y'
    ) AS LINKS
    WHERE
      1 = 1
      AND AJ.ADJUSTMENT_TYPE = RT.LOOKUP_CODE
      AND RT.LOOKUP_TYPE = 'REPORT TYPE'
      AND RT.LOOKUP_CODE = 'RESERVE'
      AND AJ.ASSET_ID = DH.ASSET_ID
      AND AJ.DISTRIBUTION_ID = DH.DISTRIBUTION_ID
      AND AJ.ADJUSTMENT_TYPE IN ('RESERVE', CASE WHEN 'RESERVE' = 'REVAL RESERVE' THEN 'REVAL AMORT' END)
      AND AH.ASSET_ID = DH.ASSET_ID
      AND (
        (
          AH.ASSET_TYPE <> 'EXPENSED' AND 'RESERVE' IN ('COST', 'CIP COST')
        )
        OR (
          AH.ASSET_TYPE IN ('CAPITALIZED', 'CIP') AND 'RESERVE' IN ('RESERVE', 'REVAL RESERVE')
        )
      )
      AND AJ.TRANSACTION_HEADER_ID = TH.TRANSACTION_HEADER_ID
      AND TH.TRANSACTION_HEADER_ID BETWEEN AH.TRANSACTION_HEADER_ID_IN AND COALESCE(AH.TRANSACTION_HEADER_ID_OUT - 1, TH.TRANSACTION_HEADER_ID)
      AND (
        CASE
          WHEN RT.LOOKUP_CODE = AJ.ADJUSTMENT_TYPE
          OR (
            RT.LOOKUP_CODE IS NULL AND AJ.ADJUSTMENT_TYPE IS NULL
          )
          THEN 1
          ELSE 0
        END * AJ.ADJUSTMENT_AMOUNT
      ) <> 0
      AND LINKS.SOURCE_DISTRIBUTION_ID_NUM_1 = AJ.TRANSACTION_HEADER_ID
      AND LINKS.SOURCE_DISTRIBUTION_ID_NUM_2 = AJ.ADJUSTMENT_LINE_ID
      AND LINKS.APPLICATION_ID = 140
      AND LINKS.SOURCE_DISTRIBUTION_TYPE = 'TRX'
      AND HEADERS.APPLICATION_ID = 140
      AND HEADERS.AE_HEADER_ID = LINKS.AE_HEADER_ID
      AND LINES.AE_HEADER_ID = LINKS.AE_HEADER_ID
      AND LINES.AE_LINE_NUM = LINKS.AE_LINE_NUM
      AND LINES.APPLICATION_ID = 140
      AND (
        AJ.kca_seq_date >= (
          SELECT
            (
              executebegints - prune_days
            )
          FROM bec_etl_ctrl.batch_dw_info
          WHERE
            dw_table_name = 'fact_fa_asset_reserves' AND batch_name = 'fa'
        )
        OR dh.kca_seq_date >= (
          SELECT
            (
              executebegints - prune_days
            )
          FROM bec_etl_ctrl.batch_dw_info
          WHERE
            dw_table_name = 'fact_fa_asset_reserves' AND batch_name = 'fa'
        )
        OR AH.kca_seq_date >= (
          SELECT
            (
              executebegints - prune_days
            )
          FROM bec_etl_ctrl.batch_dw_info
          WHERE
            dw_table_name = 'fact_fa_asset_reserves' AND batch_name = 'fa'
        )
        OR TH.kca_seq_date >= (
          SELECT
            (
              executebegints - prune_days
            )
          FROM bec_etl_ctrl.batch_dw_info
          WHERE
            dw_table_name = 'fact_fa_asset_reserves' AND batch_name = 'fa'
        )
        OR HEADERS.kca_seq_date >= (
          SELECT
            (
              executebegints - prune_days
            )
          FROM bec_etl_ctrl.batch_dw_info
          WHERE
            dw_table_name = 'fact_fa_asset_reserves' AND batch_name = 'fa'
        )
        OR LINES.kca_seq_date >= (
          SELECT
            (
              executebegints - prune_days
            )
          FROM bec_etl_ctrl.batch_dw_info
          WHERE
            dw_table_name = 'fact_fa_asset_reserves' AND batch_name = 'fa'
        )
      )
  )
);
UPDATE bec_etl_ctrl.batch_dw_info SET load_type = 'I', last_refresh_date = CURRENT_TIMESTAMP()
WHERE
  dw_table_name = 'fact_fa_asset_reserves' AND batch_name = 'fa';