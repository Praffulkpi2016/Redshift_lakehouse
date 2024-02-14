/* Delete Records */
DELETE FROM silver_bec_ods.fnd_flex_value_sets
WHERE
  FLEX_VALUE_SET_ID IN (
    SELECT
      stg.FLEX_VALUE_SET_ID
    FROM silver_bec_ods.fnd_flex_value_sets AS ods, bronze_bec_ods_stg.fnd_flex_value_sets AS stg
    WHERE
      ods.FLEX_VALUE_SET_ID = stg.FLEX_VALUE_SET_ID
      AND stg.kca_operation IN ('INSERT', 'UPDATE')
  );
/* Insert records */
INSERT INTO silver_bec_ods.fnd_flex_value_sets (
  FLEX_VALUE_SET_ID,
  FLEX_VALUE_SET_NAME,
  LAST_UPDATE_DATE,
  LAST_UPDATED_BY,
  CREATION_DATE,
  CREATED_BY,
  LAST_UPDATE_LOGIN,
  VALIDATION_TYPE,
  PROTECTED_FLAG,
  SECURITY_ENABLED_FLAG,
  LONGLIST_FLAG,
  FORMAT_TYPE,
  MAXIMUM_SIZE,
  ALPHANUMERIC_ALLOWED_FLAG,
  UPPERCASE_ONLY_FLAG,
  NUMERIC_MODE_ENABLED_FLAG,
  DESCRIPTION,
  DEPENDANT_DEFAULT_VALUE,
  DEPENDANT_DEFAULT_MEANING,
  PARENT_FLEX_VALUE_SET_ID,
  MINIMUM_VALUE,
  MAXIMUM_VALUE,
  NUMBER_PRECISION,
  ZD_EDITION_NAME,
  ZD_SYNC,
  KCA_OPERATION,
  IS_DELETED_FLG,
  KCA_SEQ_ID,
  kca_seq_date
)
(
  SELECT
    FLEX_VALUE_SET_ID,
    FLEX_VALUE_SET_NAME,
    LAST_UPDATE_DATE,
    LAST_UPDATED_BY,
    CREATION_DATE,
    CREATED_BY,
    LAST_UPDATE_LOGIN,
    VALIDATION_TYPE,
    PROTECTED_FLAG,
    SECURITY_ENABLED_FLAG,
    LONGLIST_FLAG,
    FORMAT_TYPE,
    MAXIMUM_SIZE,
    ALPHANUMERIC_ALLOWED_FLAG,
    UPPERCASE_ONLY_FLAG,
    NUMERIC_MODE_ENABLED_FLAG,
    DESCRIPTION,
    DEPENDANT_DEFAULT_VALUE,
    DEPENDANT_DEFAULT_MEANING,
    PARENT_FLEX_VALUE_SET_ID,
    MINIMUM_VALUE,
    MAXIMUM_VALUE,
    NUMBER_PRECISION,
    ZD_EDITION_NAME,
    ZD_SYNC,
    KCA_OPERATION,
    'N' AS IS_DELETED_FLG,
    CAST(NULLIF(KCA_SEQ_ID, '') AS DECIMAL(36, 0)) AS KCA_SEQ_ID,
    kca_seq_date
  FROM bronze_bec_ods_stg.fnd_flex_value_sets
  WHERE
    kca_operation IN ('INSERT', 'UPDATE')
    AND (FLEX_VALUE_SET_ID, kca_seq_id) IN (
      SELECT
        FLEX_VALUE_SET_ID,
        MAX(kca_seq_id)
      FROM bronze_bec_ods_stg.fnd_flex_value_sets
      WHERE
        kca_operation IN ('INSERT', 'UPDATE')
      GROUP BY
        FLEX_VALUE_SET_ID
    )
);
/* Soft delete */
UPDATE silver_bec_ods.fnd_flex_value_sets SET IS_DELETED_FLG = 'N';
UPDATE silver_bec_ods.fnd_flex_value_sets SET IS_DELETED_FLG = 'Y'
WHERE
  (
    FLEX_VALUE_SET_ID
  ) IN (
    SELECT
      FLEX_VALUE_SET_ID
    FROM bec_raw_dl_ext.fnd_flex_value_sets
    WHERE
      (FLEX_VALUE_SET_ID, KCA_SEQ_ID) IN (
        SELECT
          FLEX_VALUE_SET_ID,
          MAX(KCA_SEQ_ID) AS KCA_SEQ_ID
        FROM bec_raw_dl_ext.fnd_flex_value_sets
        GROUP BY
          FLEX_VALUE_SET_ID
      )
      AND kca_operation = 'DELETE'
  );
UPDATE bec_etl_ctrl.batch_ods_info SET last_refresh_date = CURRENT_TIMESTAMP()
WHERE
  ods_table_name = 'fnd_flex_value_sets';