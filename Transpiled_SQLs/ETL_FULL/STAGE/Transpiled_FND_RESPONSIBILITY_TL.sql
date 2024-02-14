DROP TABLE IF EXISTS bronze_bec_ods_stg.fnd_responsibility_tl;
CREATE TABLE bronze_bec_ods_stg.fnd_responsibility_tl AS
SELECT
  *
FROM bec_raw_dl_ext.fnd_responsibility_tl
WHERE
  kca_operation <> 'DELETE'
  AND (COALESCE(APPLICATION_ID, 0), COALESCE(RESPONSIBILITY_ID, 0), COALESCE(LANGUAGE, 'NA'), last_update_date) IN (
    SELECT
      COALESCE(APPLICATION_ID, 0) AS APPLICATION_ID,
      COALESCE(RESPONSIBILITY_ID, 0) AS RESPONSIBILITY_ID,
      COALESCE(LANGUAGE, 'NA') AS LANGUAGE,
      MAX(last_update_date)
    FROM bec_raw_dl_ext.fnd_responsibility_tl
    WHERE
      kca_operation <> 'DELETE' AND COALESCE(kca_seq_id, '') = ''
    GROUP BY
      COALESCE(APPLICATION_ID, 0),
      COALESCE(RESPONSIBILITY_ID, 0),
      COALESCE(LANGUAGE, 'NA')
  );