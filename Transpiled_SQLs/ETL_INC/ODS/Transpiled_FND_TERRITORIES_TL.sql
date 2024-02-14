/* Delete Records */
DELETE FROM silver_bec_ods.FND_TERRITORIES_TL
WHERE
  (COALESCE(TERRITORY_CODE, 'NA'), COALESCE(LANGUAGE, 'NA')) IN (
    SELECT
      COALESCE(stg.TERRITORY_CODE, 'NA') AS TERRITORY_CODE,
      COALESCE(stg.LANGUAGE, 'NA') AS LANGUAGE
    FROM silver_bec_ods.FND_TERRITORIES_TL AS ods, bronze_bec_ods_stg.FND_TERRITORIES_TL AS stg
    WHERE
      COALESCE(ods.TERRITORY_CODE, 'NA') = COALESCE(stg.TERRITORY_CODE, 'NA')
      AND COALESCE(ods.LANGUAGE, 'NA') = COALESCE(stg.LANGUAGE, 'NA')
      AND stg.kca_operation IN ('INSERT', 'UPDATE')
  );
/* Insert records */
INSERT INTO silver_bec_ods.FND_TERRITORIES_TL (
  territory_code,
  `language`,
  territory_short_name,
  created_by,
  creation_date,
  last_updated_by,
  last_update_date,
  last_update_login,
  description,
  source_lang,
  zd_edition_name,
  zd_sync,
  KCA_OPERATION,
  IS_DELETED_FLG,
  kca_seq_id,
  kca_seq_date
)
(
  SELECT
    territory_code,
    `language`,
    territory_short_name,
    created_by,
    creation_date,
    last_updated_by,
    last_update_date,
    last_update_login,
    description,
    source_lang,
    zd_edition_name,
    zd_sync,
    KCA_OPERATION,
    'N' AS IS_DELETED_FLG,
    CAST(NULLIF(KCA_SEQ_ID, '') AS DECIMAL(36, 0)) AS KCA_SEQ_ID,
    kca_seq_date
  FROM bronze_bec_ods_stg.FND_TERRITORIES_TL
  WHERE
    kca_operation IN ('INSERT', 'UPDATE')
    AND (COALESCE(TERRITORY_CODE, 'NA'), COALESCE(LANGUAGE, 'NA'), kca_seq_id) IN (
      SELECT
        COALESCE(TERRITORY_CODE, 'NA') AS TERRITORY_CODE,
        COALESCE(LANGUAGE, 'NA') AS LANGUAGE,
        MAX(kca_seq_id)
      FROM bronze_bec_ods_stg.FND_TERRITORIES_TL
      WHERE
        kca_operation IN ('INSERT', 'UPDATE')
      GROUP BY
        COALESCE(TERRITORY_CODE, 'NA'),
        COALESCE(LANGUAGE, 'NA')
    )
);
/* Soft delete */
UPDATE silver_bec_ods.FND_TERRITORIES_TL SET IS_DELETED_FLG = 'N';
UPDATE silver_bec_ods.FND_TERRITORIES_TL SET IS_DELETED_FLG = 'Y'
WHERE
  (COALESCE(TERRITORY_CODE, 'NA'), COALESCE(LANGUAGE, 'NA')) IN (
    SELECT
      COALESCE(TERRITORY_CODE, 'NA'),
      COALESCE(LANGUAGE, 'NA')
    FROM bec_raw_dl_ext.FND_TERRITORIES_TL
    WHERE
      (COALESCE(TERRITORY_CODE, 'NA'), COALESCE(LANGUAGE, 'NA'), KCA_SEQ_ID) IN (
        SELECT
          COALESCE(TERRITORY_CODE, 'NA'),
          COALESCE(LANGUAGE, 'NA'),
          MAX(KCA_SEQ_ID) AS KCA_SEQ_ID
        FROM bec_raw_dl_ext.FND_TERRITORIES_TL
        GROUP BY
          COALESCE(TERRITORY_CODE, 'NA'),
          COALESCE(LANGUAGE, 'NA')
      )
      AND kca_operation = 'DELETE'
  );
UPDATE bec_etl_ctrl.batch_ods_info SET last_refresh_date = CURRENT_TIMESTAMP()
WHERE
  ods_table_name = 'fnd_territories_tl';