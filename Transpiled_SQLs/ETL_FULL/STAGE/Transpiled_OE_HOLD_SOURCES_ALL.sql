DROP TABLE IF EXISTS bronze_bec_ods_stg.OE_HOLD_SOURCES_ALL;
CREATE TABLE bronze_bec_ods_stg.OE_HOLD_SOURCES_ALL AS
SELECT
  *
FROM bec_raw_dl_ext.OE_HOLD_SOURCES_ALL
WHERE
  kca_operation <> 'DELETE'
  AND (COALESCE(HOLD_SOURCE_ID, 0), last_update_date) IN (
    SELECT
      COALESCE(HOLD_SOURCE_ID, 0) AS HOLD_SOURCE_ID,
      MAX(last_update_date)
    FROM bec_raw_dl_ext.OE_HOLD_SOURCES_ALL
    WHERE
      kca_operation <> 'DELETE' AND COALESCE(kca_seq_id, '') = ''
    GROUP BY
      COALESCE(HOLD_SOURCE_ID, 0)
  );