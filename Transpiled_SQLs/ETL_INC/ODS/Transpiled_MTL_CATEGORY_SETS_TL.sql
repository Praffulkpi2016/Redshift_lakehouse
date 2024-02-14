/* Delete Records */
DELETE FROM silver_bec_ods.MTL_CATEGORY_SETS_TL
WHERE
  (CATEGORY_SET_ID, language) IN (
    SELECT
      stg.CATEGORY_SET_ID,
      stg.language
    FROM silver_bec_ods.MTL_CATEGORY_SETS_TL AS ods, bronze_bec_ods_stg.MTL_CATEGORY_SETS_TL AS stg
    WHERE
      ods.CATEGORY_SET_ID = stg.CATEGORY_SET_ID
      AND ods.language = stg.language
      AND stg.kca_operation IN ('INSERT', 'UPDATE')
  );
/* Insert records */
INSERT INTO silver_bec_ods.MTL_CATEGORY_SETS_TL (
  CATEGORY_SET_ID,
  LANGUAGE,
  SOURCE_LANG,
  CATEGORY_SET_NAME,
  DESCRIPTION,
  LAST_UPDATE_DATE,
  LAST_UPDATED_BY,
  CREATION_DATE,
  CREATED_BY,
  LAST_UPDATE_LOGIN,
  ZD_EDITION_NAME,
  ZD_SYNC,
  KCA_OPERATION,
  IS_DELETED_FLG,
  kca_seq_id,
  kca_seq_date
)
(
  SELECT
    CATEGORY_SET_ID,
    LANGUAGE,
    SOURCE_LANG,
    CATEGORY_SET_NAME,
    DESCRIPTION,
    LAST_UPDATE_DATE,
    LAST_UPDATED_BY,
    CREATION_DATE,
    CREATED_BY,
    LAST_UPDATE_LOGIN,
    ZD_EDITION_NAME,
    ZD_SYNC,
    KCA_OPERATION,
    'N' AS IS_DELETED_FLG,
    CAST(NULLIF(KCA_SEQ_ID, '') AS DECIMAL(36, 0)) AS KCA_SEQ_ID,
    kca_seq_date
  FROM bronze_bec_ods_stg.MTL_CATEGORY_SETS_TL
  WHERE
    kca_operation IN ('INSERT', 'UPDATE')
    AND (CATEGORY_SET_ID, language, kca_seq_id) IN (
      SELECT
        CATEGORY_SET_ID,
        language,
        MAX(kca_seq_id)
      FROM bronze_bec_ods_stg.MTL_CATEGORY_SETS_TL
      WHERE
        kca_operation IN ('INSERT', 'UPDATE')
      GROUP BY
        CATEGORY_SET_ID,
        language
    )
);
/* Soft delete */
UPDATE silver_bec_ods.MTL_CATEGORY_SETS_TL SET IS_DELETED_FLG = 'N';
UPDATE silver_bec_ods.MTL_CATEGORY_SETS_TL SET IS_DELETED_FLG = 'Y'
WHERE
  (CATEGORY_SET_ID, language) IN (
    SELECT
      CATEGORY_SET_ID,
      language
    FROM bec_raw_dl_ext.MTL_CATEGORY_SETS_TL
    WHERE
      (CATEGORY_SET_ID, language, KCA_SEQ_ID) IN (
        SELECT
          CATEGORY_SET_ID,
          language,
          MAX(KCA_SEQ_ID) AS KCA_SEQ_ID
        FROM bec_raw_dl_ext.MTL_CATEGORY_SETS_TL
        GROUP BY
          CATEGORY_SET_ID,
          language
      )
      AND kca_operation = 'DELETE'
  );
UPDATE bec_etl_ctrl.batch_ods_info SET load_type = 'I', last_refresh_date = CURRENT_TIMESTAMP()
WHERE
  ods_table_name = 'mtl_category_sets_tl';