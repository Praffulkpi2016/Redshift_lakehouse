DROP table IF EXISTS bronze_bec_ods_stg.PA_PROJECT_ACCUM_HEADERS;
CREATE TABLE bronze_bec_ods_stg.PA_PROJECT_ACCUM_HEADERS AS
(
  SELECT
    *
  FROM bec_raw_dl_ext.PA_PROJECT_ACCUM_HEADERS
  WHERE
    kca_operation <> 'DELETE'
    AND (COALESCE(PROJECT_ACCUM_ID, 0), last_update_date) IN (
      SELECT
        COALESCE(PROJECT_ACCUM_ID, 0) AS PROJECT_ACCUM_ID,
        MAX(last_update_date)
      FROM bec_raw_dl_ext.PA_PROJECT_ACCUM_HEADERS
      WHERE
        kca_operation <> 'DELETE' AND COALESCE(kca_seq_id, '') = ''
      GROUP BY
        PROJECT_ACCUM_ID
    )
);