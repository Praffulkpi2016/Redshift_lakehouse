TRUNCATE table bronze_bec_ods_stg.XLA_EVENT_CLASSES_TL;
INSERT INTO bronze_bec_ods_stg.XLA_EVENT_CLASSES_TL (
  APPLICATION_ID,
  ENTITY_CODE,
  EVENT_CLASS_CODE,
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
  FROM bec_raw_dl_ext.XLA_EVENT_CLASSES_TL
  WHERE
    kca_operation <> 'DELETE'
    AND COALESCE(kca_seq_id, '') <> ''
    AND (COALESCE(APPLICATION_ID, 0), COALESCE(ENTITY_CODE, 'NA'), COALESCE(EVENT_CLASS_CODE, 'NA'), COALESCE(LANGUAGE, 'NA'), kca_seq_id) IN (
      SELECT
        COALESCE(APPLICATION_ID, 0),
        COALESCE(ENTITY_CODE, 'NA'),
        COALESCE(EVENT_CLASS_CODE, 'NA'),
        COALESCE(LANGUAGE, 'NA'),
        MAX(kca_seq_id)
      FROM bec_raw_dl_ext.XLA_EVENT_CLASSES_TL
      WHERE
        kca_operation <> 'DELETE' AND COALESCE(kca_seq_id, '') <> ''
      GROUP BY
        COALESCE(APPLICATION_ID, 0),
        COALESCE(ENTITY_CODE, 'NA'),
        COALESCE(EVENT_CLASS_CODE, 'NA'),
        COALESCE(LANGUAGE, 'NA')
    )
    AND (
      KCA_SEQ_DATE > (
        SELECT
          (
            executebegints - prune_days
          )
        FROM bec_etl_ctrl.batch_ods_info
        WHERE
          ods_table_name = 'xla_event_classes_tl'
      )
    )
);