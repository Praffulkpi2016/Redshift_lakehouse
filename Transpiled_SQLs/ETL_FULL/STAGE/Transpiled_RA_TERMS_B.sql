DROP TABLE IF EXISTS bronze_bec_ods_stg.RA_TERMS_B;
CREATE TABLE bronze_bec_ods_stg.RA_TERMS_B AS
SELECT
  *
FROM bec_raw_dl_ext.RA_TERMS_B
WHERE
  kca_operation <> 'DELETE'
  AND (TERM_ID, last_update_date) IN (
    SELECT
      TERM_ID,
      MAX(last_update_date)
    FROM bec_raw_dl_ext.RA_TERMS_B
    WHERE
      kca_operation <> 'DELETE' AND COALESCE(kca_seq_id, '') = ''
    GROUP BY
      TERM_ID
  );