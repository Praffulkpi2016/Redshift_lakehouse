DROP TABLE IF EXISTS bronze_bec_ods_stg.CST_COST_TYPES;
CREATE TABLE bronze_bec_ods_stg.CST_COST_TYPES AS
(
  SELECT
    *
  FROM bec_raw_dl_ext.CST_COST_TYPES
  WHERE
    kca_operation <> 'DELETE'
    AND (COALESCE(COST_TYPE_ID, 0), last_update_date) IN (
      SELECT
        COALESCE(COST_TYPE_ID, 0),
        MAX(last_update_date)
      FROM bec_raw_dl_ext.CST_COST_TYPES
      WHERE
        kca_operation <> 'DELETE' AND COALESCE(kca_seq_id, '') = ''
      GROUP BY
        COALESCE(COST_TYPE_ID, 0)
    )
);