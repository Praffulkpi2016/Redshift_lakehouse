DROP TABLE IF EXISTS bronze_bec_ods_stg.PO_RELEASES_ALL;
CREATE TABLE bronze_bec_ods_stg.PO_RELEASES_ALL AS
SELECT
  *
FROM bec_raw_dl_ext.PO_RELEASES_ALL
WHERE
  kca_operation <> 'DELETE'
  AND (PO_RELEASE_ID, last_update_date) IN (
    SELECT
      PO_RELEASE_ID,
      MAX(last_update_date)
    FROM bec_raw_dl_ext.PO_RELEASES_ALL
    WHERE
      kca_operation <> 'DELETE' AND COALESCE(kca_seq_id, '') = ''
    GROUP BY
      PO_RELEASE_ID
  );