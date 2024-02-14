DROP TABLE IF EXISTS bronze_bec_ods_stg.HZ_PARTIES;
CREATE TABLE bronze_bec_ods_stg.HZ_PARTIES AS
SELECT
  *
FROM bec_raw_dl_ext.HZ_PARTIES
WHERE
  kca_operation <> 'DELETE'
  AND (party_id, last_update_date) IN (
    SELECT
      party_id,
      MAX(last_update_date)
    FROM bec_raw_dl_ext.HZ_PARTIES
    WHERE
      kca_operation <> 'DELETE' AND COALESCE(kca_seq_id, '') = ''
    GROUP BY
      party_id
  );