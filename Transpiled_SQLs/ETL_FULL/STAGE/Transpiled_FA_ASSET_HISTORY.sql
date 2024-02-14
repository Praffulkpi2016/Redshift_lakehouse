DROP table IF EXISTS bronze_bec_ods_stg.fa_asset_history;
CREATE TABLE bronze_bec_ods_stg.fa_asset_history AS
(
  SELECT
    *
  FROM bec_raw_dl_ext.fa_asset_history
  WHERE
    kca_operation <> 'DELETE'
    AND (COALESCE(ASSET_ID, 0), COALESCE(DATE_EFFECTIVE, '1900-01-01 00:00:00'), last_update_date) IN (
      SELECT
        COALESCE(ASSET_ID, 0) AS ASSET_ID,
        COALESCE(DATE_EFFECTIVE, '1900-01-01 00:00:00') AS DATE_EFFECTIVE,
        MAX(last_update_date)
      FROM bec_raw_dl_ext.fa_asset_history
      WHERE
        kca_operation <> 'DELETE' AND COALESCE(kca_seq_id, '') = ''
      GROUP BY
        COALESCE(ASSET_ID, 0),
        COALESCE(DATE_EFFECTIVE, '1900-01-01 00:00:00')
    )
);