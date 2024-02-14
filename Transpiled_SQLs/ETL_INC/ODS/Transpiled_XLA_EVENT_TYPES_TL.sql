/* Delete Records */
DELETE FROM silver_bec_ods.xla_event_types_tl
WHERE
  (APPLICATION_ID, ENTITY_CODE, EVENT_CLASS_CODE, EVENT_TYPE_CODE, LANGUAGE) IN (
    SELECT
      stg.APPLICATION_ID,
      stg.ENTITY_CODE,
      stg.EVENT_CLASS_CODE,
      stg.EVENT_TYPE_CODE,
      stg.LANGUAGE
    FROM silver_bec_ods.xla_event_types_tl AS ods, bronze_bec_ods_stg.xla_event_types_tl AS stg
    WHERE
      ods.APPLICATION_ID = stg.APPLICATION_ID
      AND ods.ENTITY_CODE = stg.ENTITY_CODE
      AND ods.EVENT_CLASS_CODE = stg.EVENT_CLASS_CODE
      AND ods.EVENT_TYPE_CODE = stg.EVENT_TYPE_CODE
      AND ods.LANGUAGE = stg.LANGUAGE
      AND stg.kca_operation IN ('INSERT', 'UPDATE')
  );
/* Insert records */
INSERT INTO silver_bec_ods.xla_event_types_tl (
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
  IS_DELETED_FLG,
  kca_seq_id,
  kca_seq_date
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
    'N' AS IS_DELETED_FLG,
    CAST(NULLIF(KCA_SEQ_ID, '') AS DECIMAL(36, 0)) AS KCA_SEQ_ID,
    KCA_SEQ_DATE
  FROM bronze_bec_ods_stg.xla_event_types_tl
  WHERE
    kca_operation IN ('INSERT', 'UPDATE')
    AND (APPLICATION_ID, ENTITY_CODE, EVENT_CLASS_CODE, EVENT_TYPE_CODE, LANGUAGE, kca_seq_id) IN (
      SELECT
        APPLICATION_ID,
        ENTITY_CODE,
        EVENT_CLASS_CODE,
        EVENT_TYPE_CODE,
        LANGUAGE,
        MAX(kca_seq_id)
      FROM bronze_bec_ods_stg.xla_event_types_tl
      WHERE
        kca_operation IN ('INSERT', 'UPDATE')
      GROUP BY
        APPLICATION_ID,
        ENTITY_CODE,
        EVENT_CLASS_CODE,
        EVENT_TYPE_CODE,
        LANGUAGE
    )
);
/* Soft delete */
UPDATE silver_bec_ods.xla_event_types_tl SET IS_DELETED_FLG = 'N';
UPDATE silver_bec_ods.xla_event_types_tl SET IS_DELETED_FLG = 'Y'
WHERE
  (APPLICATION_ID, ENTITY_CODE, EVENT_CLASS_CODE, EVENT_TYPE_CODE, LANGUAGE) IN (
    SELECT
      APPLICATION_ID,
      ENTITY_CODE,
      EVENT_CLASS_CODE,
      EVENT_TYPE_CODE,
      LANGUAGE
    FROM bec_raw_dl_ext.xla_event_types_tl
    WHERE
      (APPLICATION_ID, ENTITY_CODE, EVENT_CLASS_CODE, EVENT_TYPE_CODE, LANGUAGE, KCA_SEQ_ID) IN (
        SELECT
          APPLICATION_ID,
          ENTITY_CODE,
          EVENT_CLASS_CODE,
          EVENT_TYPE_CODE,
          LANGUAGE,
          MAX(KCA_SEQ_ID) AS KCA_SEQ_ID
        FROM bec_raw_dl_ext.xla_event_types_tl
        GROUP BY
          APPLICATION_ID,
          ENTITY_CODE,
          EVENT_CLASS_CODE,
          EVENT_TYPE_CODE,
          LANGUAGE
      )
      AND kca_operation = 'DELETE'
  );
UPDATE bec_etl_ctrl.batch_ods_info SET last_refresh_date = CURRENT_TIMESTAMP()
WHERE
  ods_table_name = 'xla_event_types_tl';