DROP TABLE IF EXISTS bronze_bec_ods_stg.OE_ORDER_LINES_ALL;
CREATE TABLE bronze_bec_ods_stg.OE_ORDER_LINES_ALL AS
(
  SELECT
    *
  FROM bec_raw_dl_ext.OE_ORDER_LINES_ALL
  WHERE
    kca_operation <> 'DELETE'
    AND (COALESCE(LINE_ID, 0), last_update_date) IN (
      SELECT
        COALESCE(LINE_ID, 0),
        MAX(last_update_date)
      FROM bec_raw_dl_ext.OE_ORDER_LINES_ALL
      WHERE
        kca_operation <> 'DELETE' AND COALESCE(kca_seq_id, '') = ''
      GROUP BY
        COALESCE(LINE_ID, 0)
    )
);