DROP table IF EXISTS bronze_bec_ods_stg.PO_ASL_STATUSES;
CREATE TABLE bronze_bec_ods_stg.PO_ASL_STATUSES AS
(
  SELECT
    *
  FROM bec_raw_dl_ext.PO_ASL_STATUSES
  WHERE
    kca_operation <> 'DELETE'
    AND (COALESCE(STATUS_ID, 0), last_update_date) IN (
      SELECT
        COALESCE(STATUS_ID, 0) AS STATUS_ID,
        MAX(last_update_date)
      FROM bec_raw_dl_ext.PO_ASL_STATUSES
      WHERE
        kca_operation <> 'DELETE' AND COALESCE(kca_seq_id, '') = ''
      GROUP BY
        COALESCE(STATUS_ID, 0)
    )
);