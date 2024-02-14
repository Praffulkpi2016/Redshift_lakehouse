DROP TABLE IF EXISTS bronze_bec_ods_stg.MTL_ONHAND_QUANTITIES_DETAIL;
CREATE TABLE bronze_bec_ods_stg.MTL_ONHAND_QUANTITIES_DETAIL AS
(
  SELECT
    *
  FROM bec_raw_dl_ext.MTL_ONHAND_QUANTITIES_DETAIL
  WHERE
    kca_operation <> 'DELETE'
    AND (COALESCE(ONHAND_QUANTITIES_ID, 0), last_update_date) IN (
      SELECT
        COALESCE(ONHAND_QUANTITIES_ID, 0) AS ONHAND_QUANTITIES_ID,
        MAX(last_update_date)
      FROM bec_raw_dl_ext.MTL_ONHAND_QUANTITIES_DETAIL
      WHERE
        kca_operation <> 'DELETE' AND COALESCE(kca_seq_id, '') = ''
      GROUP BY
        COALESCE(ONHAND_QUANTITIES_ID, 0)
    )
);