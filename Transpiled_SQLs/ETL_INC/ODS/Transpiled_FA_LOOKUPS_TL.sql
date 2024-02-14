/* Delete Records */
DELETE FROM silver_bec_ods.FA_LOOKUPS_TL
WHERE
  (COALESCE(LOOKUP_TYPE, 'NA'), COALESCE(LOOKUP_CODE, 'NA'), COALESCE(language, 'NA')) IN (
    SELECT
      COALESCE(stg.LOOKUP_TYPE, 'NA') AS LOOKUP_TYPE,
      COALESCE(stg.LOOKUP_CODE, 'NA') AS LOOKUP_CODE,
      COALESCE(stg.language, 'NA') AS language
    FROM silver_bec_ods.FA_LOOKUPS_TL AS ods, bronze_bec_ods_stg.FA_LOOKUPS_TL AS stg
    WHERE
      COALESCE(ods.LOOKUP_TYPE, 'NA') = COALESCE(stg.LOOKUP_TYPE, 'NA')
      AND COALESCE(ods.LOOKUP_CODE, 'NA') = COALESCE(stg.LOOKUP_CODE, 'NA')
      AND COALESCE(ods.language, 'NA') = COALESCE(stg.language, 'NA')
      AND stg.kca_operation IN ('INSERT', 'UPDATE')
  );
/* Insert records */
INSERT INTO silver_bec_ods.FA_LOOKUPS_TL (
  LOOKUP_TYPE,
  LOOKUP_CODE,
  LANGUAGE,
  SOURCE_LANG,
  MEANING,
  DESCRIPTION,
  LAST_UPDATE_DATE,
  LAST_UPDATED_BY,
  CREATED_BY,
  CREATION_DATE,
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
    LOOKUP_TYPE,
    LOOKUP_CODE,
    LANGUAGE,
    SOURCE_LANG,
    MEANING,
    DESCRIPTION,
    LAST_UPDATE_DATE,
    LAST_UPDATED_BY,
    CREATED_BY,
    CREATION_DATE,
    LAST_UPDATE_LOGIN,
    ZD_EDITION_NAME,
    ZD_SYNC,
    KCA_OPERATION,
    'N' AS IS_DELETED_FLG,
    CAST(NULLIF(KCA_SEQ_ID, '') AS DECIMAL(36, 0)) AS KCA_SEQ_ID,
    kca_seq_date
  FROM bronze_bec_ods_stg.FA_LOOKUPS_TL
  WHERE
    kca_operation IN ('INSERT', 'UPDATE')
    AND (COALESCE(LOOKUP_TYPE, 'NA'), COALESCE(LOOKUP_CODE, 'NA'), COALESCE(language, 'NA'), kca_seq_id) IN (
      SELECT
        COALESCE(LOOKUP_TYPE, 'NA') AS LOOKUP_TYPE,
        COALESCE(LOOKUP_CODE, 'NA') AS LOOKUP_CODE,
        COALESCE(language, 'NA') AS language,
        MAX(kca_seq_id) AS kca_seq_id
      FROM bronze_bec_ods_stg.FA_LOOKUPS_TL
      WHERE
        kca_operation IN ('INSERT', 'UPDATE')
      GROUP BY
        COALESCE(LOOKUP_TYPE, 'NA'),
        COALESCE(LOOKUP_CODE, 'NA'),
        COALESCE(language, 'NA')
    )
);
/* Soft delete */
UPDATE silver_bec_ods.FA_LOOKUPS_TL SET IS_DELETED_FLG = 'N';
UPDATE silver_bec_ods.FA_LOOKUPS_TL SET IS_DELETED_FLG = 'Y'
WHERE
  (COALESCE(LOOKUP_TYPE, 'NA'), COALESCE(LOOKUP_CODE, 'NA'), COALESCE(language, 'NA')) IN (
    SELECT
      COALESCE(LOOKUP_TYPE, 'NA'),
      COALESCE(LOOKUP_CODE, 'NA'),
      COALESCE(language, 'NA')
    FROM bec_raw_dl_ext.FA_LOOKUPS_TL
    WHERE
      (COALESCE(LOOKUP_TYPE, 'NA'), COALESCE(LOOKUP_CODE, 'NA'), COALESCE(language, 'NA'), KCA_SEQ_ID) IN (
        SELECT
          COALESCE(LOOKUP_TYPE, 'NA'),
          COALESCE(LOOKUP_CODE, 'NA'),
          COALESCE(language, 'NA'),
          MAX(KCA_SEQ_ID) AS KCA_SEQ_ID
        FROM bec_raw_dl_ext.FA_LOOKUPS_TL
        GROUP BY
          COALESCE(LOOKUP_TYPE, 'NA'),
          COALESCE(LOOKUP_CODE, 'NA'),
          COALESCE(language, 'NA')
      )
      AND kca_operation = 'DELETE'
  );
UPDATE bec_etl_ctrl.batch_ods_info SET last_refresh_date = CURRENT_TIMESTAMP()
WHERE
  ods_table_name = 'fa_lookups_tl';