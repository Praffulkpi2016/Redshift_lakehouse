DROP TABLE IF EXISTS bronze_bec_ods_stg.CST_QUANTITY_LAYERS;
CREATE TABLE bronze_bec_ods_stg.CST_QUANTITY_LAYERS AS
(
  SELECT
    *
  FROM bec_raw_dl_ext.CST_QUANTITY_LAYERS
  WHERE
    kca_operation <> 'DELETE'
    AND (COALESCE(LAYER_ID, 0), last_update_date) IN (
      SELECT
        COALESCE(LAYER_ID, 0),
        MAX(last_update_date)
      FROM bec_raw_dl_ext.CST_QUANTITY_LAYERS
      WHERE
        kca_operation <> 'DELETE' AND COALESCE(kca_seq_id, '') = ''
      GROUP BY
        COALESCE(LAYER_ID, 0)
    )
);