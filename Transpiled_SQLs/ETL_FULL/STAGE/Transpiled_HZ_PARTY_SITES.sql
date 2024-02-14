DROP TABLE IF EXISTS bronze_bec_ods_stg.HZ_PARTY_SITES;
CREATE TABLE bronze_bec_ods_stg.HZ_PARTY_SITES AS
SELECT
  *
FROM bec_raw_dl_ext.HZ_PARTY_SITES
WHERE
  kca_operation <> 'DELETE'
  AND (PARTY_SITE_ID, last_update_date) IN (
    SELECT
      PARTY_SITE_ID,
      MAX(last_update_date)
    FROM bec_raw_dl_ext.HZ_PARTY_SITES
    WHERE
      kca_operation <> 'DELETE' AND COALESCE(kca_seq_id, '') = ''
    GROUP BY
      PARTY_SITE_ID
  );