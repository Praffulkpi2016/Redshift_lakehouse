/* Delete Records */
DELETE FROM silver_bec_ods.fa_categories_tl
WHERE
  (COALESCE(CATEGORY_ID, 0), COALESCE(LANGUAGE, 'NA')) IN (
    SELECT
      COALESCE(stg.CATEGORY_ID, 0) AS CATEGORY_ID,
      COALESCE(stg.LANGUAGE, 'NA') AS LANGUAGE
    FROM silver_bec_ods.fa_categories_tl AS ods, bronze_bec_ods_stg.fa_categories_tl AS stg
    WHERE
      COALESCE(ods.CATEGORY_ID, 0) = COALESCE(stg.CATEGORY_ID, 0)
      AND COALESCE(ods.LANGUAGE, 'NA') = COALESCE(stg.LANGUAGE, 'NA')
      AND stg.kca_operation IN ('INSERT', 'UPDATE')
  );
/* Insert records */
INSERT INTO silver_bec_ods.fa_categories_tl (
  CATEGORY_ID,
  LANGUAGE,
  SOURCE_LANG,
  DESCRIPTION,
  LAST_UPDATE_DATE,
  LAST_UPDATED_BY,
  CREATED_BY,
  CREATION_DATE,
  LAST_UPDATE_LOGIN,
  KCA_OPERATION,
  IS_DELETED_FLG,
  kca_seq_id,
  kca_seq_date
)
(
  SELECT
    CATEGORY_ID,
    LANGUAGE,
    SOURCE_LANG,
    DESCRIPTION,
    LAST_UPDATE_DATE,
    LAST_UPDATED_BY,
    CREATED_BY,
    CREATION_DATE,
    LAST_UPDATE_LOGIN,
    KCA_OPERATION,
    'N' AS IS_DELETED_FLG,
    CAST(NULLIF(KCA_SEQ_ID, '') AS DECIMAL(36, 0)) AS KCA_SEQ_ID,
    kca_seq_date
  FROM bronze_bec_ods_stg.fa_categories_tl
  WHERE
    kca_operation IN ('INSERT', 'UPDATE')
    AND (COALESCE(CATEGORY_ID, 0), COALESCE(LANGUAGE, 'NA'), kca_seq_id) IN (
      SELECT
        COALESCE(CATEGORY_ID, 0) AS CATEGORY_ID,
        COALESCE(LANGUAGE, 'NA') AS LANGUAGE,
        MAX(kca_seq_id)
      FROM bronze_bec_ods_stg.fa_categories_tl
      WHERE
        kca_operation IN ('INSERT', 'UPDATE')
      GROUP BY
        COALESCE(CATEGORY_ID, 0),
        COALESCE(LANGUAGE, 'NA')
    )
);
/* Soft delete */
UPDATE silver_bec_ods.fa_categories_tl SET IS_DELETED_FLG = 'N';
UPDATE silver_bec_ods.fa_categories_tl SET IS_DELETED_FLG = 'Y'
WHERE
  (COALESCE(CATEGORY_ID, 0), COALESCE(LANGUAGE, 'NA')) IN (
    SELECT
      COALESCE(CATEGORY_ID, 0),
      COALESCE(LANGUAGE, 'NA')
    FROM bec_raw_dl_ext.fa_categories_tl
    WHERE
      (COALESCE(CATEGORY_ID, 0), COALESCE(LANGUAGE, 'NA'), KCA_SEQ_ID) IN (
        SELECT
          COALESCE(CATEGORY_ID, 0),
          COALESCE(LANGUAGE, 'NA'),
          MAX(KCA_SEQ_ID) AS KCA_SEQ_ID
        FROM bec_raw_dl_ext.fa_categories_tl
        GROUP BY
          COALESCE(CATEGORY_ID, 0),
          COALESCE(LANGUAGE, 'NA')
      )
      AND kca_operation = 'DELETE'
  );
UPDATE bec_etl_ctrl.batch_ods_info SET last_refresh_date = CURRENT_TIMESTAMP()
WHERE
  ods_table_name = 'fa_categories_tl';