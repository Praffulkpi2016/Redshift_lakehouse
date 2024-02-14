DROP TABLE IF EXISTS bronze_bec_ods_stg.xla_event_types_tl;
CREATE TABLE bronze_bec_ods_stg.xla_event_types_tl AS
SELECT
  *
FROM bec_raw_dl_ext.xla_event_types_tl
WHERE
  kca_operation <> 'DELETE'
  AND (APPLICATION_ID, ENTITY_CODE, EVENT_CLASS_CODE, EVENT_TYPE_CODE, LANGUAGE, last_update_date) IN (
    SELECT
      APPLICATION_ID,
      ENTITY_CODE,
      EVENT_CLASS_CODE,
      EVENT_TYPE_CODE,
      LANGUAGE,
      MAX(last_update_date)
    FROM bec_raw_dl_ext.xla_event_types_tl
    WHERE
      kca_operation <> 'DELETE' AND COALESCE(kca_seq_id, '') = ''
    GROUP BY
      APPLICATION_ID,
      ENTITY_CODE,
      EVENT_CLASS_CODE,
      EVENT_TYPE_CODE,
      LANGUAGE
  );