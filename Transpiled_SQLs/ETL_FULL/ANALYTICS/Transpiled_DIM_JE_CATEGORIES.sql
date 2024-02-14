DROP table IF EXISTS gold_bec_dwh.DIM_JE_CATEGORIES;
CREATE TABLE gold_bec_dwh.DIM_JE_CATEGORIES AS
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
    'N' AS IS_DELETED_FLG,
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
);
UPDATE bec_etl_ctrl.batch_dw_info SET load_type = 'I', last_refresh_date = CURRENT_TIMESTAMP()
WHERE
  dw_table_name = 'dim_je_categories' AND batch_name = 'gl';