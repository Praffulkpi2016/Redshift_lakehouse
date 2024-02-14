TRUNCATE table bronze_bec_ods_stg.xla_event_types_tl;
INSERT INTO bronze_bec_ods_stg.xla_event_types_tl (
  APPLICATION_ID,
  ENTITY_CODE,
  EVENT_CLASS_CODE,
  EVENT_TYPE_CODE,
  LANGUAGE,
  NAME,
  DESCRIPTION,
  SOURCE_LANG,
  CREATION_DATE,
  CREATED_BY,
  LAST_UPDATE_DATE,
  LAST_UPDATED_BY,
  LAST_UPDATE_LOGIN,
  ZD_EDITION_NAME,
  ZD_SYNC,
  KCA_OPERATION,
  kca_seq_id,
  KCA_SEQ_DATE
)
(
  SELECT
    APPLICATION_ID,
    ENTITY_CODE,
    EVENT_CLASS_CODE,
    EVENT_TYPE_CODE,
    LANGUAGE,
    NAME,
    DESCRIPTION,
    SOURCE_LANG,
    CREATION_DATE,
    CREATED_BY,
    LAST_UPDATE_DATE,
    LAST_UPDATED_BY,
    LAST_UPDATE_LOGIN,
    ZD_EDITION_NAME,
    ZD_SYNC,
    KCA_OPERATION,
    kca_seq_id,
    KCA_SEQ_DATE
  FROM bec_raw_dl_ext.xla_event_types_tl
  WHERE
    kca_operation <> 'DELETE'
    AND COALESCE(kca_seq_id, '') <> ''
    AND (APPLICATION_ID, ENTITY_CODE, EVENT_CLASS_CODE, EVENT_TYPE_CODE, LANGUAGE, kca_seq_id) IN (
      SELECT
        APPLICATION_ID,
        ENTITY_CODE,
        EVENT_CLASS_CODE,
        EVENT_TYPE_CODE,
        LANGUAGE,
        MAX(kca_seq_id)
      FROM bec_raw_dl_ext.xla_event_types_tl
      WHERE
        kca_operation <> 'DELETE' AND COALESCE(kca_seq_id, '') <> ''
      GROUP BY
        APPLICATION_ID,
        ENTITY_CODE,
        EVENT_CLASS_CODE,
        EVENT_TYPE_CODE,
        LANGUAGE
    )
    AND (
      KCA_SEQ_DATE > (
        SELECT
          (
            executebegints - prune_days
          )
        FROM bec_etl_ctrl.batch_ods_info
        WHERE
          ods_table_name = 'xla_event_types_tl'
      )
    )
);