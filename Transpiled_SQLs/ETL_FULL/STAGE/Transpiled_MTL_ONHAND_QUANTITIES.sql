DROP table IF EXISTS bronze_bec_ods_stg.MTL_ONHAND_QUANTITIES;
CREATE TABLE bronze_bec_ods_stg.MTL_ONHAND_QUANTITIES AS
(
  SELECT
    *
  FROM bec_raw_dl_ext.MTL_ONHAND_QUANTITIES_DETAIL
  WHERE
    kca_operation <> 'DELETE'
    AND is_consigned = 2
    AND (COALESCE(ONHAND_QUANTITIES_ID, 0), last_update_date) IN (
      SELECT
        COALESCE(ONHAND_QUANTITIES_ID, 0) AS ONHAND_QUANTITIES_ID,
        MAX(last_update_date)
      FROM bec_raw_dl_ext.MTL_ONHAND_QUANTITIES_DETAIL
      WHERE
        kca_operation <> 'DELETE' AND is_consigned = 2 AND COALESCE(kca_seq_id, '') = ''
      GROUP BY
        COALESCE(ONHAND_QUANTITIES_ID, 0)
    )
);