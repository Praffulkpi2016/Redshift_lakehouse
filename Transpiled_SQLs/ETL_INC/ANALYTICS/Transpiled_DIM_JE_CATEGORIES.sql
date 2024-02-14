/* Delete Records */
DELETE FROM gold_bec_dwh.DIM_JE_CATEGORIES
WHERE
  (COALESCE(JE_CATEGORY_NAME, 'NA'), COALESCE(`LANGUAGE`, 'NA')) IN (
    SELECT
      ods.JE_CATEGORY_NAME,
      ods.`LANGUAGE`
    FROM gold_bec_dwh.DIM_JE_CATEGORIES AS dw, (
      SELECT
        COALESCE(JE_CATEGORY_NAME, 'NA') AS JE_CATEGORY_NAME,
        COALESCE(`LANGUAGE`, 'NA') AS `LANGUAGE`
      FROM BEC_ODS.GL_JE_CATEGORIES_TL
      WHERE
        1 = 1
        AND (
          kca_seq_date > (
            SELECT
              (
                executebegints - prune_days
              )
            FROM bec_etl_ctrl.batch_dw_info
            WHERE
              dw_table_name = 'dim_je_categories' AND batch_name = 'gl'
          )
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
      ) || '-' || COALESCE(ods.JE_CATEGORY_NAME, 'NA') || '-' || COALESCE(ods.`LANGUAGE`, 'NA')
  );
/* Insert records */
INSERT INTO gold_bec_dwh.DIM_JE_CATEGORIES (
  JE_CATEGORY_NAME,
  `LANGUAGE`,
  SOURCE_LANG,
  USER_JE_CATEGORY_NAME,
  LAST_UPDATE_DATE,
  LAST_UPDATED_BY,
  CREATION_DATE,
  CREATED_BY,
  LAST_UPDATE_LOGIN,
  DESCRIPTION,
  ATTRIBUTE1,
  ATTRIBUTE2,
  ATTRIBUTE3,
  ATTRIBUTE4,
  ATTRIBUTE5,
  CONTEXT,
  CONSOLIDATION_FLAG,
  JE_CATEGORY_KEY,
  ZD_EDITION_NAME,
  ZD_SYNC,
  is_deleted_flg,
  SOURCE_APP_ID,
  DW_LOAD_ID,
  DW_INSERT_DATE,
  DW_UPDATE_DATE
)
(
  SELECT
    JE_CATEGORY_NAME,
    `LANGUAGE`,
    SOURCE_LANG,
    USER_JE_CATEGORY_NAME,
    LAST_UPDATE_DATE,
    LAST_UPDATED_BY,
    CREATION_DATE,
    CREATED_BY,
    LAST_UPDATE_LOGIN,
    DESCRIPTION,
    ATTRIBUTE1,
    ATTRIBUTE2,
    ATTRIBUTE3,
    ATTRIBUTE4,
    ATTRIBUTE5,
    CONTEXT,
    CONSOLIDATION_FLAG,
    JE_CATEGORY_KEY,
    ZD_EDITION_NAME,
    ZD_SYNC,
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
    ) || '-' || COALESCE(JE_CATEGORY_NAME, 'NA') || '-' || COALESCE(`LANGUAGE`, 'NA') AS dw_load_id,
    CURRENT_TIMESTAMP() AS dw_insert_date,
    CURRENT_TIMESTAMP() AS dw_update_date
  FROM BEC_ODS.GL_JE_CATEGORIES_TL
  WHERE
    1 = 1
    AND (
      kca_seq_date > (
        SELECT
          (
            executebegints - prune_days
          )
        FROM bec_etl_ctrl.batch_dw_info
        WHERE
          dw_table_name = 'dim_je_categories' AND batch_name = 'gl'
      )
    )
);
/* Soft delete */
UPDATE gold_bec_dwh.dim_je_categories SET is_deleted_flg = 'Y'
WHERE
  NOT (JE_CATEGORY_NAME, `LANGUAGE`) IN (
    SELECT
      COALESCE(ods.JE_CATEGORY_NAME, 'NA') AS JE_CATEGORY_NAME,
      COALESCE(ods.`LANGUAGE`, 'NA') AS `LANGUAGE`
    FROM gold_bec_dwh.DIM_JE_CATEGORIES AS dw, silver_bec_ods.GL_JE_CATEGORIES_TL AS ods
    WHERE
      1 = 1
      AND dw.dw_load_id = (
        SELECT
          system_id
        FROM bec_etl_ctrl.etlsourceappid
        WHERE
          source_system = 'EBS'
      ) || '-' || COALESCE(ods.JE_CATEGORY_NAME, 'NA') || '-' || COALESCE(ods.`LANGUAGE`, 'NA')
      AND ods.is_deleted_flg <> 'Y'
  );
UPDATE bec_etl_ctrl.batch_dw_info SET load_type = 'I', last_refresh_date = CURRENT_TIMESTAMP()
WHERE
  dw_table_name = 'dim_je_categories' AND batch_name = 'gl';