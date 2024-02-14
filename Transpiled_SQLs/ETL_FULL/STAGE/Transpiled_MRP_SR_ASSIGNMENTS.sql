DROP table IF EXISTS bronze_bec_ods_stg.MRP_SR_ASSIGNMENTS;
CREATE TABLE bronze_bec_ods_stg.MRP_SR_ASSIGNMENTS AS
(
  SELECT
    *
  FROM bec_raw_dl_ext.MRP_SR_ASSIGNMENTS
  WHERE
    kca_operation <> 'DELETE'
    AND (COALESCE(ASSIGNMENT_ID, 0), last_update_date) IN (
      SELECT
        COALESCE(ASSIGNMENT_ID, 0) AS ASSIGNMENT_ID,
        MAX(last_update_date)
      FROM bec_raw_dl_ext.MRP_SR_ASSIGNMENTS
      WHERE
        kca_operation <> 'DELETE' AND COALESCE(kca_seq_id, '') = ''
      GROUP BY
        COALESCE(ASSIGNMENT_ID, 0)
    )
);