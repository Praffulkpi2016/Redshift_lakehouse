TRUNCATE table
	table bronze_bec_ods_stg.fnd_id_flex_segments;
INSERT INTO bronze_bec_ods_stg.FND_ID_FLEX_SEGMENTS (
  APPLICATION_ID,
  ID_FLEX_CODE,
  ID_FLEX_NUM,
  APPLICATION_COLUMN_NAME,
  SEGMENT_NAME,
  LAST_UPDATE_DATE,
  LAST_UPDATED_BY,
  CREATION_DATE,
  CREATED_BY,
  LAST_UPDATE_LOGIN,
  SEGMENT_NUM,
  APPLICATION_COLUMN_INDEX_FLAG,
  ENABLED_FLAG,
  REQUIRED_FLAG,
  DISPLAY_FLAG,
  DISPLAY_SIZE,
  SECURITY_ENABLED_FLAG,
  MAXIMUM_DESCRIPTION_LEN,
  CONCATENATION_DESCRIPTION_LEN,
  FLEX_VALUE_SET_ID,
  RANGE_CODE,
  DEFAULT_TYPE,
  DEFAULT_VALUE,
  RUNTIME_PROPERTY_FUNCTION,
  ADDITIONAL_WHERE_CLAUSE,
  SEGMENT_INSERT_FLAG,
  SEGMENT_UPDATE_FLAG,
  ZD_EDITION_NAME,
  ZD_SYNC,
  KCA_OPERATION, /* ,IS_DELETED_FLG */
  kca_seq_id,
  kca_seq_date
)
(
  SELECT
    APPLICATION_ID,
    ID_FLEX_CODE,
    ID_FLEX_NUM,
    APPLICATION_COLUMN_NAME,
    SEGMENT_NAME,
    LAST_UPDATE_DATE,
    LAST_UPDATED_BY,
    CREATION_DATE,
    CREATED_BY,
    LAST_UPDATE_LOGIN,
    SEGMENT_NUM,
    APPLICATION_COLUMN_INDEX_FLAG,
    ENABLED_FLAG,
    REQUIRED_FLAG,
    DISPLAY_FLAG,
    DISPLAY_SIZE,
    SECURITY_ENABLED_FLAG,
    MAXIMUM_DESCRIPTION_LEN,
    CONCATENATION_DESCRIPTION_LEN,
    FLEX_VALUE_SET_ID,
    RANGE_CODE,
    DEFAULT_TYPE,
    DEFAULT_VALUE,
    RUNTIME_PROPERTY_FUNCTION,
    ADDITIONAL_WHERE_CLAUSE,
    SEGMENT_INSERT_FLAG,
    SEGMENT_UPDATE_FLAG,
    ZD_EDITION_NAME,
    ZD_SYNC,
    KCA_OPERATION, /* ,'N' as IS_DELETED_FLG */
    kca_seq_id,
    kca_seq_date
  FROM bec_raw_dl_ext.FND_ID_FLEX_SEGMENTS
  WHERE
    kca_operation <> 'DELETE'
    AND COALESCE(kca_seq_id, '') <> ''
    AND (APPLICATION_ID, ID_FLEX_CODE, ID_FLEX_NUM, APPLICATION_COLUMN_NAME, kca_seq_id) IN (
      SELECT
        APPLICATION_ID,
        ID_FLEX_CODE,
        ID_FLEX_NUM,
        APPLICATION_COLUMN_NAME,
        MAX(kca_seq_id)
      FROM bec_raw_dl_ext.FND_ID_FLEX_SEGMENTS
      WHERE
        kca_operation <> 'DELETE' AND COALESCE(kca_seq_id, '') <> ''
      GROUP BY
        APPLICATION_ID,
        ID_FLEX_CODE,
        ID_FLEX_NUM,
        APPLICATION_COLUMN_NAME
    )
    AND kca_seq_date > (
      SELECT
        (
          executebegints - prune_days
        )
      FROM bec_etl_ctrl.batch_ods_info
      WHERE
        ods_table_name = 'fnd_id_flex_segments'
    )
);