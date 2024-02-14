TRUNCATE table
	table bronze_bec_ods_stg.fnd_flex_values_tl;
INSERT INTO bronze_bec_ods_stg.FND_FLEX_VALUES_TL (
  FLEX_VALUE_ID,
  LANGUAGE,
  LAST_UPDATE_DATE,
  LAST_UPDATED_BY,
  CREATION_DATE,
  CREATED_BY,
  LAST_UPDATE_LOGIN,
  DESCRIPTION,
  SOURCE_LANG,
  FLEX_VALUE_MEANING, /* -,SECURITY_GROUP_ID */
  ZD_EDITION_NAME,
  ZD_SYNC,
  KCA_OPERATION,
  kca_seq_id,
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
    FLEX_VALUE_MEANING, /* -,SECURITY_GROUP_ID */
    ZD_EDITION_NAME,
    ZD_SYNC,
    KCA_OPERATION,
    kca_seq_id,
    kca_seq_date
  FROM bec_raw_dl_ext.FND_FLEX_VALUES_TL
  WHERE
    kca_operation <> 'DELETE'
    AND COALESCE(kca_seq_id, '') <> ''
    AND (FLEX_VALUE_ID, language, kca_seq_id) IN (
      SELECT
        FLEX_VALUE_ID,
        language,
        MAX(kca_seq_id)
      FROM bec_raw_dl_ext.FND_FLEX_VALUES_TL
      WHERE
        kca_operation <> 'DELETE' AND COALESCE(kca_seq_id, '') <> ''
      GROUP BY
        FLEX_VALUE_ID,
        language
    )
    AND kca_seq_date > (
      SELECT
        (
          executebegints - prune_days
        )
      FROM bec_etl_ctrl.batch_ods_info
      WHERE
        ods_table_name = 'fnd_flex_values_tl'
    )
);