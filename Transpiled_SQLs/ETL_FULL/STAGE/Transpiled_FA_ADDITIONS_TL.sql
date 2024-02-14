DROP TABLE IF EXISTS bronze_bec_ods_stg.fa_additions_tl;
CREATE TABLE bronze_bec_ods_stg.fa_additions_tl AS
SELECT
  *
FROM bec_raw_dl_ext.fa_additions_tl
WHERE
  kca_operation <> 'DELETE'
  AND (COALESCE(ASSET_ID, 0), COALESCE(LANGUAGE, 'NA'), last_update_date) IN (
    SELECT
      COALESCE(ASSET_ID, 0) AS ASSET_ID,
      COALESCE(LANGUAGE, 'NA') AS LANGUAGE,
      MAX(last_update_date)
    FROM bec_raw_dl_ext.fa_additions_tl
    WHERE
      kca_operation <> 'DELETE' AND COALESCE(kca_seq_id, '') = ''
    GROUP BY
      COALESCE(ASSET_ID, 0),
      COALESCE(LANGUAGE, 'NA')
  );