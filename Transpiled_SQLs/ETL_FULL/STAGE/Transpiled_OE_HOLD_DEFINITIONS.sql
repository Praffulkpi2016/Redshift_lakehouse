DROP TABLE IF EXISTS bronze_bec_ods_stg.oe_hold_definitions;
CREATE TABLE bronze_bec_ods_stg.oe_hold_definitions AS
SELECT
  *
FROM bec_raw_dl_ext.oe_hold_definitions
WHERE
  kca_operation <> 'DELETE'
  AND (COALESCE(HOLD_ID, 0), last_update_date) IN (
    SELECT
      COALESCE(HOLD_ID, 0) AS HOLD_ID,
      MAX(last_update_date)
    FROM bec_raw_dl_ext.oe_hold_definitions
    WHERE
      kca_operation <> 'DELETE' AND COALESCE(kca_seq_id, '') = ''
    GROUP BY
      COALESCE(HOLD_ID, 0)
  );