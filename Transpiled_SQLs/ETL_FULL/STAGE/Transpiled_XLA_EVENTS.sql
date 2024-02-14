DROP TABLE IF EXISTS bronze_bec_ods_stg.XLA_EVENTS;
CREATE TABLE bronze_bec_ods_stg.XLA_EVENTS AS
SELECT
  *
FROM bec_raw_dl_ext.XLA_EVENTS
WHERE
  kca_operation <> 'DELETE'
  AND (EVENT_ID, APPLICATION_ID, last_update_date) IN (
    SELECT
      EVENT_ID,
      APPLICATION_ID,
      MAX(last_update_date)
    FROM bec_raw_dl_ext.XLA_EVENTS
    WHERE
      kca_operation <> 'DELETE' AND COALESCE(kca_seq_id, '') = ''
    GROUP BY
      EVENT_ID,
      APPLICATION_ID
  );