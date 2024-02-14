DROP TABLE IF EXISTS bronze_bec_ods_stg.JTF_RS_SALESREPS;
CREATE TABLE bronze_bec_ods_stg.JTF_RS_SALESREPS AS
SELECT
  *
FROM bec_raw_dl_ext.JTF_RS_SALESREPS
WHERE
  kca_operation <> 'DELETE'
  AND (SALESREP_ID, COALESCE(ORG_ID, 0), last_update_date) IN (
    SELECT
      SALESREP_ID,
      COALESCE(ORG_ID, 0),
      MAX(last_update_date)
    FROM bec_raw_dl_ext.JTF_RS_SALESREPS
    WHERE
      kca_operation <> 'DELETE' AND COALESCE(kca_seq_id, '') = ''
    GROUP BY
      SALESREP_ID,
      COALESCE(ORG_ID, 0)
  );