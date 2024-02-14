DROP table IF EXISTS bronze_bec_ods_stg.WSH_DELIVERY_ASSIGNMENTS;
CREATE TABLE bronze_bec_ods_stg.WSH_DELIVERY_ASSIGNMENTS AS
(
  SELECT
    *
  FROM bec_raw_dl_ext.WSH_DELIVERY_ASSIGNMENTS
  WHERE
    kca_operation <> 'DELETE'
    AND (COALESCE(DELIVERY_ASSIGNMENT_ID, 0), last_update_date) IN (
      SELECT
        COALESCE(DELIVERY_ASSIGNMENT_ID, 0) AS DELIVERY_ASSIGNMENT_ID,
        MAX(last_update_date)
      FROM bec_raw_dl_ext.WSH_DELIVERY_ASSIGNMENTS
      WHERE
        kca_operation <> 'DELETE' AND COALESCE(kca_seq_id, '') = ''
      GROUP BY
        DELIVERY_ASSIGNMENT_ID
    )
);