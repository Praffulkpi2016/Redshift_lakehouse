/* Delete Records */
DELETE FROM silver_bec_ods.fnd_flex_values_tl
WHERE
  (FLEX_VALUE_ID, language) IN (
    SELECT
      stg.FLEX_VALUE_ID,
      stg.LANGUAGE
    FROM silver_bec_ods.fnd_flex_values_tl AS ods, bronze_bec_ods_stg.fnd_flex_values_tl AS stg
    WHERE
      ods.FLEX_VALUE_ID = stg.FLEX_VALUE_ID
      AND ods.LANGUAGE = stg.LANGUAGE
      AND stg.kca_operation IN ('INSERT', 'UPDATE')
  );
/* Insert records */
INSERT INTO silver_bec_ods.fnd_flex_values_tl (
  FLEX_VALUE_ID,
  LANGUAGE,
  LAST_UPDATE_DATE,
  LAST_UPDATED_BY,
  CREATION_DATE,
  CREATED_BY,
  LAST_UPDATE_LOGIN,
  DESCRIPTION,
  SOURCE_LANG,
  FLEX_VALUE_MEANING,
  ZD_EDITION_NAME,
  ZD_SYNC,
  KCA_OPERATION,
  IS_DELETED_FLG,
  KCA_SEQ_ID,
  kca_seq_date
)
(
  SELECT
    FLEX_VALUE_ID,
    LANGUAGE,
    LAST_UPDATE_DATE,
    LAST_UPDATED_BY,
    CREATION_DATE,
    CREATED_BY,
    LAST_UPDATE_LOGIN,
    DESCRIPTION,
    SOURCE_LANG,
    FLEX_VALUE_MEANING,
    ZD_EDITION_NAME,
    ZD_SYNC,
    KCA_OPERATION,
    'N' AS IS_DELETED_FLG,
    CAST(NULLIF(KCA_SEQ_ID, '') AS DECIMAL(36, 0)) AS KCA_SEQ_ID,
    kca_seq_date
  FROM bronze_bec_ods_stg.fnd_flex_values_tl
  WHERE
    kca_operation IN ('INSERT', 'UPDATE')
    AND (FLEX_VALUE_ID, LANGUAGE, kca_seq_id) IN (
      SELECT
        FLEX_VALUE_ID,
        LANGUAGE,
        MAX(kca_seq_id)
      FROM bronze_bec_ods_stg.fnd_flex_values_tl
      WHERE
        kca_operation IN ('INSERT', 'UPDATE')
      GROUP BY
        FLEX_VALUE_ID,
        LANGUAGE
    )
);
/* Soft Delete */
UPDATE silver_bec_ods.fnd_flex_values_tl SET IS_DELETED_FLG = 'N';
UPDATE silver_bec_ods.fnd_flex_values_tl SET IS_DELETED_FLG = 'Y'
WHERE
  (FLEX_VALUE_ID, language) IN (
    SELECT
      FLEX_VALUE_ID,
      language
    FROM bec_raw_dl_ext.fnd_flex_values_tl
    WHERE
      (FLEX_VALUE_ID, language, KCA_SEQ_ID) IN (
        SELECT
          FLEX_VALUE_ID,
          language,
          MAX(KCA_SEQ_ID) AS KCA_SEQ_ID
        FROM bec_raw_dl_ext.fnd_flex_values_tl
        GROUP BY
          FLEX_VALUE_ID,
          language
      )
      AND kca_operation = 'DELETE'
  );
UPDATE bec_etl_ctrl.batch_ods_info SET last_refresh_date = CURRENT_TIMESTAMP()
WHERE
  ods_table_name = 'fnd_flex_values_tl';