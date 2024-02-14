/* Delete Records */
DELETE FROM gold_bec_dwh.dim_gl_accounts
WHERE
  EXISTS(
    SELECT
      1
    FROM silver_bec_ods.GL_CODE_COMBINATIONS_KFV AS ods
    WHERE
      dim_gl_accounts.CODE_COMBINATION_ID = COALESCE(ods.CODE_COMBINATION_ID, 0)
      AND ods.kca_seq_date > (
        SELECT
          executebegints - prune_days
        FROM bec_etl_ctrl.batch_dw_info
        WHERE
          dw_table_name = 'dim_gl_accounts' AND batch_name = 'ap'
      )
  );
/* Insert records */
INSERT INTO gold_bec_dwh.DIM_GL_ACCOUNTS (
  CODE_COMBINATION_ID,
  CHART_OF_ACCOUNTS_ID,
  CONCATENATED_SEGMENTS,
  GL_ACCOUNT_TYPE, /* ,ACCOUNT_TYPE	*/
  SEGMENT1,
  SEGMENT1_DESC,
  SEGMENT2,
  SEGMENT2_DESC,
  SEGMENT3,
  SEGMENT3_DESC,
  SEGMENT4,
  SEGMENT4_DESC,
  SEGMENT5,
  SEGMENT5_DESC,
  SEGMENT6,
  SEGMENT6_DESC,
  SEGMENT7,
  SEGMENT7_DESC,
  SEGMENT8,
  SEGMENT8_DESC,
  SEGMENT9,
  SEGMENT9_DESC,
  SEGMENT10,
  SEGMENT10_DESC,
  ENABLED_FLAG,
  SUMMARY_FLAG,
  is_deleted_flg,
  source_app_id,
  dw_load_id,
  dw_insert_date,
  dw_update_date
)
SELECT
  CODE_COMBINATION_ID,
  CHART_OF_ACCOUNTS_ID,
  CONCATENATED_SEGMENTS,
  GL_ACCOUNT_TYPE,
  GCC.SEGMENT1,
  (
    SELECT
      F2A.DESCRIPTION
    FROM silver_bec_ods.FND_FLEX_VALUES AS F1A, silver_bec_ods.FND_FLEX_VALUES_TL AS F2A, silver_bec_ods.FND_ID_FLEX_SEGMENTS AS F3A
    WHERE
      F1A.FLEX_VALUE_ID = F2A.FLEX_VALUE_ID
      AND F2A.LANGUAGE = 'US'
      AND F1A.FLEX_VALUE_SET_ID = F3A.FLEX_VALUE_SET_ID
      AND F3A.ID_FLEX_NUM = GCC.CHART_OF_ACCOUNTS_ID
      AND F3A.ID_FLEX_CODE = 'GL#'
      AND F3A.APPLICATION_COLUMN_NAME = 'SEGMENT1'
      AND F1A.FLEX_VALUE = GCC.SEGMENT1
      AND F3A.APPLICATION_ID = 101
  ) AS SEGMENT1_DESC,
  GCC.SEGMENT2,
  (
    SELECT
      F2B.DESCRIPTION
    FROM silver_bec_ods.FND_FLEX_VALUES AS F1B, silver_bec_ods.FND_FLEX_VALUES_TL AS F2B, silver_bec_ods.FND_ID_FLEX_SEGMENTS AS F3B
    WHERE
      F1B.FLEX_VALUE_ID = F2B.FLEX_VALUE_ID
      AND F2B.LANGUAGE = 'US'
      AND F1B.FLEX_VALUE_SET_ID = F3B.FLEX_VALUE_SET_ID
      AND F3B.ID_FLEX_NUM = GCC.CHART_OF_ACCOUNTS_ID
      AND F3B.ID_FLEX_CODE = 'GL#'
      AND F3B.APPLICATION_COLUMN_NAME = 'SEGMENT2'
      AND F1B.FLEX_VALUE = GCC.SEGMENT2
      AND F3B.APPLICATION_ID = 101
  ) AS SEGMENT2_DESC,
  GCC.SEGMENT3,
  (
    SELECT
      F2C.DESCRIPTION
    FROM silver_bec_ods.FND_FLEX_VALUES AS F1C, silver_bec_ods.FND_FLEX_VALUES_TL AS F2C, silver_bec_ods.FND_ID_FLEX_SEGMENTS AS F3C
    WHERE
      F1C.FLEX_VALUE_ID = F2C.FLEX_VALUE_ID
      AND F2C.LANGUAGE = 'US'
      AND F1C.FLEX_VALUE_SET_ID = F3C.FLEX_VALUE_SET_ID
      AND F3C.ID_FLEX_NUM = GCC.CHART_OF_ACCOUNTS_ID
      AND F3C.ID_FLEX_CODE = 'GL#'
      AND F3C.APPLICATION_COLUMN_NAME = 'SEGMENT3'
      AND F1C.FLEX_VALUE = GCC.SEGMENT3
      AND F3C.APPLICATION_ID = 101
  ) AS SEGMENT3_DESC,
  GCC.SEGMENT4,
  (
    SELECT
      F2D.DESCRIPTION
    FROM silver_bec_ods.FND_FLEX_VALUES AS F1D, silver_bec_ods.FND_FLEX_VALUES_TL AS F2D, silver_bec_ods.FND_ID_FLEX_SEGMENTS AS F3D
    WHERE
      F1D.FLEX_VALUE_ID = F2D.FLEX_VALUE_ID
      AND F2D.LANGUAGE = 'US'
      AND F1D.FLEX_VALUE_SET_ID = F3D.FLEX_VALUE_SET_ID
      AND F3D.ID_FLEX_NUM = GCC.CHART_OF_ACCOUNTS_ID
      AND F3D.ID_FLEX_CODE = 'GL#'
      AND F3D.APPLICATION_COLUMN_NAME = 'SEGMENT4'
      AND F1D.FLEX_VALUE = GCC.SEGMENT4
      AND F3D.APPLICATION_ID = 101
  ) AS SEGMENT4_DESC,
  GCC.SEGMENT5,
  (
    SELECT
      F2E.DESCRIPTION
    FROM silver_bec_ods.FND_FLEX_VALUES AS F1E, silver_bec_ods.FND_FLEX_VALUES_TL AS F2E, silver_bec_ods.FND_ID_FLEX_SEGMENTS AS F3E
    WHERE
      F1E.FLEX_VALUE_ID = F2E.FLEX_VALUE_ID
      AND F2E.LANGUAGE = 'US'
      AND F1E.FLEX_VALUE_SET_ID = F3E.FLEX_VALUE_SET_ID
      AND F3E.ID_FLEX_NUM = GCC.CHART_OF_ACCOUNTS_ID
      AND F3E.ID_FLEX_CODE = 'GL#'
      AND F3E.APPLICATION_COLUMN_NAME = 'SEGMENT5'
      AND F1E.FLEX_VALUE = GCC.SEGMENT5
      AND F3E.APPLICATION_ID = 101
  ) AS SEGMENT5_DESC,
  GCC.SEGMENT6,
  (
    SELECT
      F2F.DESCRIPTION
    FROM silver_bec_ods.FND_FLEX_VALUES AS F1F, silver_bec_ods.FND_FLEX_VALUES_TL AS F2F, silver_bec_ods.FND_ID_FLEX_SEGMENTS AS F3F
    WHERE
      F1F.FLEX_VALUE_ID = F2F.FLEX_VALUE_ID
      AND F2F.LANGUAGE = 'US'
      AND F1F.FLEX_VALUE_SET_ID = F3F.FLEX_VALUE_SET_ID
      AND F3F.ID_FLEX_NUM = GCC.CHART_OF_ACCOUNTS_ID
      AND F3F.ID_FLEX_CODE = 'GL#'
      AND F3F.APPLICATION_COLUMN_NAME = 'SEGMENT6'
      AND F1F.FLEX_VALUE = GCC.SEGMENT6
      AND F3F.APPLICATION_ID = 101
  ) AS SEGMENT6_DESC,
  GCC.SEGMENT7,
  (
    SELECT
      F2G.DESCRIPTION
    FROM silver_bec_ods.FND_FLEX_VALUES AS F1G, silver_bec_ods.FND_FLEX_VALUES_TL AS F2G, silver_bec_ods.FND_ID_FLEX_SEGMENTS AS F3G
    WHERE
      F1G.FLEX_VALUE_ID = F2G.FLEX_VALUE_ID
      AND F2G.LANGUAGE = 'US'
      AND F1G.FLEX_VALUE_SET_ID = F3G.FLEX_VALUE_SET_ID
      AND F3G.ID_FLEX_NUM = GCC.CHART_OF_ACCOUNTS_ID
      AND F3G.ID_FLEX_CODE = 'GL#'
      AND F3G.APPLICATION_COLUMN_NAME = 'SEGMENT7'
      AND F1G.FLEX_VALUE = GCC.SEGMENT7
      AND F3G.APPLICATION_ID = 101
  ) AS SEGMENT7_DESC,
  GCC.SEGMENT8,
  (
    SELECT
      F2H.DESCRIPTION
    FROM silver_bec_ods.FND_FLEX_VALUES AS F1H, silver_bec_ods.FND_FLEX_VALUES_TL AS F2H, silver_bec_ods.FND_ID_FLEX_SEGMENTS AS F3H
    WHERE
      F1H.FLEX_VALUE_ID = F2H.FLEX_VALUE_ID
      AND F2H.LANGUAGE = 'US'
      AND F1H.FLEX_VALUE_SET_ID = F3H.FLEX_VALUE_SET_ID
      AND F3H.ID_FLEX_NUM = GCC.CHART_OF_ACCOUNTS_ID
      AND F3H.ID_FLEX_CODE = 'GL#'
      AND F3H.APPLICATION_COLUMN_NAME = 'SEGMENT8'
      AND F1H.FLEX_VALUE = GCC.SEGMENT8
      AND F3H.APPLICATION_ID = 101
  ) AS SEGMENT8_DESC,
  GCC.SEGMENT9,
  (
    SELECT
      F2I.DESCRIPTION
    FROM silver_bec_ods.FND_FLEX_VALUES AS F1I, silver_bec_ods.FND_FLEX_VALUES_TL AS F2I, silver_bec_ods.FND_ID_FLEX_SEGMENTS AS F3I
    WHERE
      F1I.FLEX_VALUE_ID = F2I.FLEX_VALUE_ID
      AND F2I.LANGUAGE = 'US'
      AND F1I.FLEX_VALUE_SET_ID = F3I.FLEX_VALUE_SET_ID
      AND F3I.ID_FLEX_NUM = GCC.CHART_OF_ACCOUNTS_ID
      AND F3I.ID_FLEX_CODE = 'GL#'
      AND F3I.APPLICATION_COLUMN_NAME = 'SEGMENT9'
      AND F1I.FLEX_VALUE = GCC.SEGMENT9
      AND F3I.APPLICATION_ID = 101
  ) AS SEGMENT9_DESC,
  GCC.SEGMENT10,
  (
    SELECT
      F2J.DESCRIPTION
    FROM silver_bec_ods.FND_FLEX_VALUES AS F1J, silver_bec_ods.FND_FLEX_VALUES_TL AS F2J, silver_bec_ods.FND_ID_FLEX_SEGMENTS AS F3J
    WHERE
      F1J.FLEX_VALUE_ID = F2J.FLEX_VALUE_ID
      AND F2J.LANGUAGE = 'US'
      AND F1J.FLEX_VALUE_SET_ID = F3J.FLEX_VALUE_SET_ID
      AND F3J.ID_FLEX_NUM = GCC.CHART_OF_ACCOUNTS_ID
      AND F3J.ID_FLEX_CODE = 'GL#'
      AND F3J.APPLICATION_COLUMN_NAME = 'SEGMENT10'
      AND F1J.FLEX_VALUE = GCC.SEGMENT10
      AND F3J.APPLICATION_ID = 101
  ) AS SEGMENT10_DESC,
  ENABLED_FLAG,
  SUMMARY_FLAG,
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
  ) || '-' || COALESCE(CODE_COMBINATION_ID, 0) AS dw_load_id,
  CURRENT_TIMESTAMP() AS dw_insert_date,
  CURRENT_TIMESTAMP() AS dw_update_date
FROM silver_bec_ods.GL_CODE_COMBINATIONS_KFV AS GCC
WHERE
  1 = 1
  AND (
    GCC.kca_seq_date > (
      SELECT
        (
          executebegints - prune_days
        )
      FROM bec_etl_ctrl.batch_dw_info
      WHERE
        dw_table_name = 'dim_gl_accounts' AND batch_name = 'ap'
    )
  );
/* Soft delete */
UPDATE gold_bec_dwh.dim_gl_accounts SET is_deleted_flg = 'Y'
WHERE
  NOT EXISTS(
    SELECT
      1
    FROM silver_bec_ods.GL_CODE_COMBINATIONS_KFV AS ods
    WHERE
      dim_gl_accounts.CODE_COMBINATION_ID = COALESCE(ods.CODE_COMBINATION_ID, 0)
      AND ods.is_deleted_flg <> 'Y'
  );
UPDATE bec_etl_ctrl.batch_dw_info SET last_refresh_date = CURRENT_TIMESTAMP()
WHERE
  dw_table_name = 'dim_gl_accounts' AND batch_name = 'ap';