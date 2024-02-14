DROP table IF EXISTS bronze_bec_ods_stg.fa_asset_invoices;
CREATE TABLE bronze_bec_ods_stg.fa_asset_invoices AS
(
  SELECT
    *
  FROM bec_raw_dl_ext.fa_asset_invoices
  WHERE
    kca_operation <> 'DELETE'
    AND (COALESCE(SOURCE_LINE_ID, 0), last_update_date) IN (
      SELECT
        COALESCE(SOURCE_LINE_ID, 0) AS SOURCE_LINE_ID,
        MAX(last_update_date)
      FROM bec_raw_dl_ext.fa_asset_invoices
      WHERE
        kca_operation <> 'DELETE' AND COALESCE(kca_seq_id, '') = ''
      GROUP BY
        COALESCE(SOURCE_LINE_ID, 0)
    )
);