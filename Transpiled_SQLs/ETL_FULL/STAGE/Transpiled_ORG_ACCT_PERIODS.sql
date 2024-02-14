DROP table IF EXISTS bronze_bec_ods_stg.ORG_ACCT_PERIODS;
CREATE TABLE bronze_bec_ods_stg.ORG_ACCT_PERIODS AS
(
  SELECT
    *
  FROM bec_raw_dl_ext.ORG_ACCT_PERIODS
  WHERE
    kca_operation <> 'DELETE'
    AND (COALESCE(ACCT_PERIOD_ID, 0), COALESCE(ORGANIZATION_ID, 0), last_update_date) IN (
      SELECT
        COALESCE(ACCT_PERIOD_ID, 0) AS ACCT_PERIOD_ID,
        COALESCE(ORGANIZATION_ID, 0) AS ORGANIZATION_ID,
        MAX(last_update_date)
      FROM bec_raw_dl_ext.ORG_ACCT_PERIODS
      WHERE
        kca_operation <> 'DELETE' AND COALESCE(kca_seq_id, '') = ''
      GROUP BY
        COALESCE(ACCT_PERIOD_ID, 0),
        COALESCE(ORGANIZATION_ID, 0)
    )
);