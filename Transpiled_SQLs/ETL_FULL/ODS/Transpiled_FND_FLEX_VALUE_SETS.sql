DROP table IF EXISTS silver_bec_ods.FND_FLEX_VALUE_SETS;
CREATE TABLE IF NOT EXISTS silver_bec_ods.FND_FLEX_VALUE_SETS (
  FLEX_VALUE_SET_ID DECIMAL(10, 0),
  FLEX_VALUE_SET_NAME STRING,
  LAST_UPDATE_DATE TIMESTAMP,
  LAST_UPDATED_BY DECIMAL(15, 0),
  CREATION_DATE TIMESTAMP,
  CREATED_BY DECIMAL(15, 0),
  LAST_UPDATE_LOGIN DECIMAL(15, 0),
  VALIDATION_TYPE STRING,
  PROTECTED_FLAG STRING,
  SECURITY_ENABLED_FLAG STRING,
  LONGLIST_FLAG STRING,
  FORMAT_TYPE STRING,
  MAXIMUM_SIZE DECIMAL(3, 0),
  ALPHANUMERIC_ALLOWED_FLAG STRING,
  UPPERCASE_ONLY_FLAG STRING,
  NUMERIC_MODE_ENABLED_FLAG STRING,
  DESCRIPTION STRING,
  DEPENDANT_DEFAULT_VALUE STRING,
  DEPENDANT_DEFAULT_MEANING STRING,
  PARENT_FLEX_VALUE_SET_ID DECIMAL(10, 0),
  MINIMUM_VALUE STRING,
  MAXIMUM_VALUE STRING,
  NUMBER_PRECISION DECIMAL(2, 0), /* ,SECURITY_GROUP_ID	NUMERIC(28,10)   */
  ZD_EDITION_NAME STRING,
  ZD_SYNC STRING,
  KCA_OPERATION STRING,
  IS_DELETED_FLG STRING,
  kca_seq_id DECIMAL(36, 0),
  kca_seq_date TIMESTAMP
);
INSERT INTO silver_bec_ods.FND_FLEX_VALUE_SETS (
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
  kca_seq_id,
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
);
UPDATE bec_etl_ctrl.batch_ods_info SET load_type = 'I', last_refresh_date = CURRENT_TIMESTAMP()
WHERE
  ods_table_name = 'fnd_flex_value_sets';