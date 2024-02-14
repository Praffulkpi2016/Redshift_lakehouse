DROP TABLE IF EXISTS bronze_bec_ods_stg.MTL_PHYSICAL_ADJUSTMENTS;
CREATE TABLE bronze_bec_ods_stg.MTL_PHYSICAL_ADJUSTMENTS AS
(
  SELECT
    *
  FROM bec_raw_dl_ext.MTL_PHYSICAL_ADJUSTMENTS
  WHERE
    kca_operation <> 'DELETE'
    AND (COALESCE(ADJUSTMENT_ID, 0), last_update_date) IN (
      SELECT
        COALESCE(ADJUSTMENT_ID, 0),
        MAX(last_update_date)
      FROM bec_raw_dl_ext.MTL_PHYSICAL_ADJUSTMENTS
      WHERE
        kca_operation <> 'DELETE' AND COALESCE(kca_seq_id, '') = ''
      GROUP BY
        COALESCE(ADJUSTMENT_ID, 0)
    )
);