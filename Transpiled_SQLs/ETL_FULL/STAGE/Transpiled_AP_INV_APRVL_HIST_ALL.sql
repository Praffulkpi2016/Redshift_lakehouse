DROP table IF EXISTS bronze_bec_ods_stg.AP_INV_APRVL_HIST_ALL;
CREATE TABLE bronze_bec_ods_stg.AP_INV_APRVL_HIST_ALL AS
(
  SELECT
    *
  FROM bec_raw_dl_ext.AP_INV_APRVL_HIST_ALL
  WHERE
    kca_operation <> 'DELETE'
    AND (COALESCE(APPROVAL_HISTORY_ID, '0'), last_update_date) IN (
      SELECT
        COALESCE(APPROVAL_HISTORY_ID, '0') AS APPROVAL_HISTORY_ID,
        MAX(last_update_date)
      FROM bec_raw_dl_ext.AP_INV_APRVL_HIST_ALL
      WHERE
        kca_operation <> 'DELETE' AND COALESCE(kca_seq_id, '') = ''
      GROUP BY
        COALESCE(APPROVAL_HISTORY_ID, '0')
    )
);