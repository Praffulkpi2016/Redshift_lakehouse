DROP table IF EXISTS bronze_bec_ods_stg.OE_ORDER_SOURCES;
CREATE TABLE bronze_bec_ods_stg.OE_ORDER_SOURCES AS
(
  SELECT
    *
  FROM bec_raw_dl_ext.OE_ORDER_SOURCES
  WHERE
    kca_operation <> 'DELETE'
    AND (COALESCE(ORDER_SOURCE_ID, 0), last_update_date) IN (
      SELECT
        COALESCE(ORDER_SOURCE_ID, 0) AS ORDER_SOURCE_ID,
        MAX(last_update_date)
      FROM bec_raw_dl_ext.OE_ORDER_SOURCES
      WHERE
        kca_operation <> 'DELETE' AND COALESCE(kca_seq_id, '') = ''
      GROUP BY
        ORDER_SOURCE_ID
    )
);