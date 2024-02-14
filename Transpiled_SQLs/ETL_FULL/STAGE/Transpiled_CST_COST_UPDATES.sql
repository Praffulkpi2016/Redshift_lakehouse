DROP TABLE IF EXISTS bronze_bec_ods_stg.CST_COST_UPDATES;
CREATE TABLE bronze_bec_ods_stg.CST_COST_UPDATES AS
SELECT
  *
FROM bec_raw_dl_ext.CST_COST_UPDATES
WHERE
  kca_operation <> 'DELETE'
  AND (COST_UPDATE_ID, last_update_date) IN (
    SELECT
      COST_UPDATE_ID,
      MAX(last_update_date)
    FROM bec_raw_dl_ext.CST_COST_UPDATES
    WHERE
      kca_operation <> 'DELETE' AND COALESCE(kca_seq_id, '') = ''
    GROUP BY
      COST_UPDATE_ID
  );