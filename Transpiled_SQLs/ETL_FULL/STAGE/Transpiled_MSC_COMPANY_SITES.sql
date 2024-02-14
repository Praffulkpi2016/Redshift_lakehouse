DROP TABLE IF EXISTS bronze_bec_ods_stg.MSC_COMPANY_SITES;
CREATE TABLE bronze_bec_ods_stg.MSC_COMPANY_SITES AS
(
  SELECT
    *
  FROM bec_raw_dl_ext.MSC_COMPANY_SITES
  WHERE
    kca_operation <> 'DELETE'
    AND (COALESCE(COMPANY_ID, 0), COALESCE(COMPANY_SITE_ID, 0), last_update_date) IN (
      SELECT
        COALESCE(COMPANY_ID, 0) AS COMPANY_ID,
        COALESCE(COMPANY_SITE_ID, 0) AS COMPANY_SITE_ID,
        MAX(last_update_date)
      FROM bec_raw_dl_ext.MSC_COMPANY_SITES
      WHERE
        kca_operation <> 'DELETE' AND COALESCE(kca_seq_id, '') = ''
      GROUP BY
        COALESCE(COMPANY_ID, 0),
        COALESCE(COMPANY_SITE_ID, 0)
    )
);