DROP table IF EXISTS bronze_bec_ods_stg.ALR_ACTION_HISTORY;
CREATE TABLE bronze_bec_ods_stg.alr_action_history AS
(
  SELECT
    *
  FROM bec_raw_dl_ext.alr_action_history
  WHERE
    kca_operation <> 'DELETE'
    AND (COALESCE(APPLICATION_ID, 0), COALESCE(ACTION_HISTORY_ID, 0), last_update_date) IN (
      SELECT
        COALESCE(APPLICATION_ID, 0) AS APPLICATION_ID,
        COALESCE(ACTION_HISTORY_ID, 0) AS ACTION_HISTORY_ID,
        MAX(last_update_date)
      FROM bec_raw_dl_ext.alr_action_history
      WHERE
        kca_operation <> 'DELETE' AND COALESCE(kca_seq_id, '') = ''
      GROUP BY
        COALESCE(APPLICATION_ID, 0),
        COALESCE(ACTION_HISTORY_ID, 0)
    )
);