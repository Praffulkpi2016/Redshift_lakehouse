DROP TABLE IF EXISTS bronze_bec_ods_stg.PER_ALL_PEOPLE_F;
CREATE TABLE bronze_bec_ods_stg.PER_ALL_PEOPLE_F AS
SELECT
  *
FROM bec_raw_dl_ext.PER_ALL_PEOPLE_F
WHERE
  kca_operation <> 'DELETE'
  AND (PERSON_ID, EFFECTIVE_START_DATE, EFFECTIVE_END_DATE, last_update_date) IN (
    SELECT
      PERSON_ID,
      EFFECTIVE_START_DATE,
      EFFECTIVE_END_DATE,
      MAX(last_update_date)
    FROM bec_raw_dl_ext.PER_ALL_PEOPLE_F
    WHERE
      kca_operation <> 'DELETE' AND COALESCE(kca_seq_id, '') = ''
    GROUP BY
      PERSON_ID,
      EFFECTIVE_START_DATE,
      EFFECTIVE_END_DATE
  );