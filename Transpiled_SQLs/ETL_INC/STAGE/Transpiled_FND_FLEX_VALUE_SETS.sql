TRUNCATE table
	table bronze_bec_ods_stg.fnd_flex_value_sets;
INSERT INTO bronze_bec_ods_stg.fnd_flex_value_sets (
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
    kca_seq_id,
    kca_seq_date
  FROM bec_raw_dl_ext.fnd_flex_value_sets
  WHERE
    kca_operation <> 'DELETE'
    AND COALESCE(kca_seq_id, '') <> ''
    AND (FLEX_VALUE_SET_ID, kca_seq_id) IN (
      SELECT
        FLEX_VALUE_SET_ID,
        MAX(kca_seq_id)
      FROM bec_raw_dl_ext.fnd_flex_value_sets
      WHERE
        kca_operation <> 'DELETE' AND COALESCE(kca_seq_id, '') <> ''
      GROUP BY
        FLEX_VALUE_SET_ID
    )
    AND kca_seq_date > (
      SELECT
        (
          executebegints - prune_days
        )
      FROM bec_etl_ctrl.batch_ods_info
      WHERE
        ods_table_name = 'fnd_flex_value_sets'
    )
);