DROP TABLE IF EXISTS BEC_DWH.DIM_SEGMENT4;
CREATE TABLE BEC_DWH.DIM_SEGMENT4 AS
(
  SELECT DISTINCT
    S1.CHART_OF_ACCOUNTS_ID AS CHART_OF_ACCOUNTS_ID,
    S1.FLEX_VALUE_SET_ID AS FLEX_VALUE_SET_ID,
    S1.LEDGER_ID AS LEDGER_ID,
    S1.LEDGER_NAME AS LEDGER_NAME,
    CONCAT(
      COALESCE(S1.CHART_OF_ACCOUNTS_ID, ''),
      COALESCE(CONCAT(COALESCE('-', ''), COALESCE(F1.FLEX_VALUE, '')), '')
    ) AS COA_SEGVAL,
    F1.FLEX_VALUE AS LEVEL0_VALUE,
    F1.DESCRIPTION AS LEVEL0_DESC,
    CONCAT(
      COALESCE(F1.FLEX_VALUE, ''),
      COALESCE(CONCAT(COALESCE('-', ''), COALESCE(F1.DESCRIPTION, '')), '')
    ) AS SEG4_VALUE_DESC,
    S1.SEGMENT_NAME AS SEGMENT_NAME,
    F1.CREATION_DATE AS CREATION_DATE,
    F1.LAST_UPDATE_DATE AS LAST_UPDATE,
    'N' AS IS_DELETED_FLG, /* AUDIT COLUMN */
    (
      SELECT
        SYSTEM_ID
      FROM BEC_ETL_CTRL.ETLSOURCEAPPID
      WHERE
        SOURCE_SYSTEM = 'EBS'
    ) AS SOURCE_APP_ID,
    (
      SELECT
        SYSTEM_ID
      FROM BEC_ETL_CTRL.ETLSOURCEAPPID
      WHERE
        SOURCE_SYSTEM = 'EBS'
    ) || '-' || COALESCE(S1.LEDGER_ID, 0) || '-' || COALESCE(S1.CHART_OF_ACCOUNTS_ID, 0) || '-' || COALESCE(S1.FLEX_VALUE_SET_ID, 0) || '-' || COALESCE(F1.FLEX_VALUE, '0') || '-' || COALESCE(S1.SEGMENT_NAME, '0') AS DW_LOAD_ID,
    CURRENT_TIMESTAMP() AS DW_INSERT_DATE,
    CURRENT_TIMESTAMP() AS DW_UPDATE_DATE
  FROM (
    SELECT
      F1.FLEX_VALUE_SET_ID,
      F1.FLEX_VALUE_ID,
      F1.FLEX_VALUE,
      F1.CREATION_DATE,
      F1.LAST_UPDATE_DATE,
      F2.DESCRIPTION
    FROM BEC_ODS.FND_FLEX_VALUES AS F1
    INNER JOIN BEC_ODS.FND_FLEX_VALUES_TL AS F2
      ON F1.FLEX_VALUE_ID = F2.FLEX_VALUE_ID AND F2.`LANGUAGE` = 'US'
    WHERE
      F1.IS_DELETED_FLG <> 'Y'
  ) AS F1
  INNER JOIN (
    SELECT
      F1.APPLICATION_ID,
      G1.LEDGER_ID,
      F1.ID_FLEX_NUM,
      F1.ID_FLEX_STRUCTURE_CODE,
      F1.ID_FLEX_STRUCTURE_NAME,
      F1.DESCRIPTION,
      F2.APPLICATION_COLUMN_NAME,
      F2.SEGMENT_NAME,
      F2.SEGMENT_NUM,
      F2.FLEX_VALUE_SET_ID,
      G1.CHART_OF_ACCOUNTS_ID,
      G1.NAME AS LEDGER_NAME
    FROM BEC_ODS.FND_ID_FLEX_STRUCTURES_VL AS F1
    INNER JOIN BEC_ODS.FND_ID_FLEX_SEGMENTS AS F2
      ON F1.ID_FLEX_NUM = F2.ID_FLEX_NUM
      AND F1.ID_FLEX_CODE = F2.ID_FLEX_CODE
      AND F2.ID_FLEX_CODE = 'GL#'
    INNER JOIN BEC_ODS.GL_LEDGERS AS G1
      ON F1.ID_FLEX_NUM = G1.CHART_OF_ACCOUNTS_ID
    WHERE
      F1.IS_DELETED_FLG <> 'Y'
  ) AS S1
    ON F1.FLEX_VALUE_SET_ID = S1.FLEX_VALUE_SET_ID
    AND S1.APPLICATION_COLUMN_NAME = 'SEGMENT4'
);
UPDATE BEC_ETL_CTRL.BATCH_DW_INFO SET LOAD_TYPE = 'I', LAST_REFRESH_DATE = CURRENT_TIMESTAMP()
WHERE
  DW_TABLE_NAME = 'dim_segment4' AND BATCH_NAME = 'gl';