DROP TABLE IF EXISTS bronze_bec_ods_stg.RA_CUST_TRX_LINE_GL_DIST_ALL;
CREATE TABLE bronze_bec_ods_stg.RA_CUST_TRX_LINE_GL_DIST_ALL AS
SELECT
  *
FROM bec_raw_dl_ext.RA_CUST_TRX_LINE_GL_DIST_ALL
WHERE
  kca_operation <> 'DELETE'
  AND (CUST_TRX_LINE_GL_DIST_ID, last_update_date) IN (
    SELECT
      CUST_TRX_LINE_GL_DIST_ID,
      MAX(last_update_date)
    FROM bec_raw_dl_ext.RA_CUST_TRX_LINE_GL_DIST_ALL
    WHERE
      kca_operation <> 'DELETE' AND COALESCE(kca_seq_id, '') = ''
    GROUP BY
      CUST_TRX_LINE_GL_DIST_ID
  );