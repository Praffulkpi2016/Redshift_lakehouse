DROP TABLE IF EXISTS bronze_bec_ods_stg.MRP_ASSIGNMENT_SETS;
CREATE TABLE bronze_bec_ods_stg.MRP_ASSIGNMENT_SETS AS
SELECT
  *
FROM bec_raw_dl_ext.MRP_ASSIGNMENT_SETS
WHERE
  kca_operation <> 'DELETE'
  AND (ASSIGNMENT_SET_ID, last_update_date) IN (
    SELECT
      COALESCE(ASSIGNMENT_SET_ID, 0) AS ASSIGNMENT_SET_ID,
      MAX(last_update_date)
    FROM bec_raw_dl_ext.MRP_ASSIGNMENT_SETS
    WHERE
      kca_operation <> 'DELETE' AND COALESCE(kca_seq_id, '') = ''
    GROUP BY
      COALESCE(ASSIGNMENT_SET_ID, 0)
  );