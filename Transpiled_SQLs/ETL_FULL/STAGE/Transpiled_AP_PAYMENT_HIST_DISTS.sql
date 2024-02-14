DROP TABLE IF EXISTS bronze_bec_ods_stg.AP_PAYMENT_HIST_DISTS;
CREATE TABLE bronze_bec_ods_stg.AP_PAYMENT_HIST_DISTS AS
SELECT
  *
FROM bec_raw_dl_ext.AP_PAYMENT_HIST_DISTS
WHERE
  kca_operation <> 'DELETE'
  AND (payment_hist_dist_id, last_update_date) IN (
    SELECT
      payment_hist_dist_id,
      MAX(last_update_date)
    FROM bec_raw_dl_ext.AP_PAYMENT_HIST_DISTS
    WHERE
      kca_operation <> 'DELETE' AND COALESCE(kca_seq_id, '') = ''
    GROUP BY
      payment_hist_dist_id
  );