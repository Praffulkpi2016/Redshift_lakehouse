/* Delete Records */
DELETE FROM silver_bec_ods.FA_ADDITIONS_TL
WHERE
  (COALESCE(LANGUAGE, 'NA'), COALESCE(ASSET_ID, 0)) IN (
    SELECT
      COALESCE(stg.LANGUAGE, 'NA') AS LANGUAGE,
      COALESCE(stg.ASSET_ID, 0) AS ASSET_ID
    FROM silver_bec_ods.FA_ADDITIONS_TL AS ods, bronze_bec_ods_stg.FA_ADDITIONS_TL AS stg
    WHERE
      COALESCE(ods.LANGUAGE, 'NA') = COALESCE(stg.LANGUAGE, 'NA')
      AND COALESCE(ods.ASSET_ID, 0) = COALESCE(stg.ASSET_ID, 0)
      AND stg.kca_operation IN ('INSERT', 'UPDATE')
  );
/* Insert records */
INSERT INTO silver_bec_ods.FA_ADDITIONS_TL (
  ASSET_ID,
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
    ASSET_ID,
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
  FROM bronze_bec_ods_stg.FA_ADDITIONS_TL
  WHERE
    kca_operation IN ('INSERT', 'UPDATE')
    AND (COALESCE(LANGUAGE, 'NA'), COALESCE(ASSET_ID, 0), kca_seq_id) IN (
      SELECT
        COALESCE(LANGUAGE, 'NA') AS LANGUAGE,
        COALESCE(ASSET_ID, 0) AS ASSET_ID,
        MAX(kca_seq_id)
      FROM bronze_bec_ods_stg.FA_ADDITIONS_TL
      WHERE
        kca_operation IN ('INSERT', 'UPDATE')
      GROUP BY
        COALESCE(LANGUAGE, 'NA'),
        COALESCE(ASSET_ID, 0)
    )
);
/* Soft delete */
UPDATE silver_bec_ods.FA_ADDITIONS_TL SET IS_DELETED_FLG = 'N';
UPDATE silver_bec_ods.FA_ADDITIONS_TL SET IS_DELETED_FLG = 'Y'
WHERE
  (COALESCE(LANGUAGE, 'NA'), COALESCE(ASSET_ID, 0)) IN (
    SELECT
      COALESCE(LANGUAGE, 'NA'),
      COALESCE(ASSET_ID, 0)
    FROM bec_raw_dl_ext.FA_ADDITIONS_TL
    WHERE
      (COALESCE(LANGUAGE, 'NA'), COALESCE(ASSET_ID, 0), KCA_SEQ_ID) IN (
        SELECT
          COALESCE(LANGUAGE, 'NA'),
          COALESCE(ASSET_ID, 0),
          MAX(KCA_SEQ_ID) AS KCA_SEQ_ID
        FROM bec_raw_dl_ext.FA_ADDITIONS_TL
        GROUP BY
          COALESCE(LANGUAGE, 'NA'),
          COALESCE(ASSET_ID, 0)
      )
      AND kca_operation = 'DELETE'
  );
UPDATE bec_etl_ctrl.batch_ods_info SET last_refresh_date = CURRENT_TIMESTAMP()
WHERE
  ods_table_name = 'fa_additions_tl';