DROP table IF EXISTS bronze_bec_ods_stg.QP_LIST_LINES;
CREATE TABLE bronze_bec_ods_stg.QP_LIST_LINES AS
(
  SELECT
    *
  FROM bec_raw_dl_ext.QP_LIST_LINES
  WHERE
    kca_operation <> 'DELETE'
    AND (COALESCE(LIST_LINE_ID, 0), last_update_date) IN (
      SELECT
        COALESCE(LIST_LINE_ID, 0) AS LIST_LINE_ID,
        MAX(last_update_date)
      FROM bec_raw_dl_ext.QP_LIST_LINES
      WHERE
        kca_operation <> 'DELETE' AND COALESCE(kca_seq_id, '') = ''
      GROUP BY
        LIST_LINE_ID
    )
);