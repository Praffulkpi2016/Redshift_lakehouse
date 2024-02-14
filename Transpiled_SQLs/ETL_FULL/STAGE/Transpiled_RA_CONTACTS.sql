DROP TABLE IF EXISTS bronze_bec_ods_stg.RA_CONTACTS;
CREATE TABLE bronze_bec_ods_stg.RA_CONTACTS AS
SELECT
  *
FROM bec_raw_dl_ext.RA_CONTACTS
WHERE
  kca_operation <> 'DELETE'
  AND (CONTACT_ID, last_update_date) IN (
    SELECT
      CONTACT_ID,
      MAX(last_update_date)
    FROM bec_raw_dl_ext.RA_CONTACTS
    WHERE
      kca_operation <> 'DELETE' AND COALESCE(kca_seq_id, '') = ''
    GROUP BY
      CONTACT_ID
  );