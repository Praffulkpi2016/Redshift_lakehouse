DROP TABLE IF EXISTS bronze_bec_ods_stg.FND_USER;
CREATE TABLE bronze_bec_ods_stg.FND_USER AS
SELECT
  *
FROM bec_raw_dl_ext.FND_USER
WHERE
  kca_operation <> 'DELETE'
  AND (user_id, last_update_date) IN (
    SELECT
      user_id,
      MAX(last_update_date)
    FROM bec_raw_dl_ext.FND_USER
    WHERE
      kca_operation <> 'DELETE' AND COALESCE(kca_seq_id, '') = ''
    GROUP BY
      user_id
  );