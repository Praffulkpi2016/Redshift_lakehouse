TRUNCATE table
	table bronze_bec_ods_stg.ALR_ACTION_HISTORY;
INSERT INTO bronze_bec_ods_stg.alr_action_history (
  APPLICATION_ID,
  ACTION_HISTORY_ID,
  ALERT_ID,
  ACTION_SET_ID,
  CHECK_ID,
  ACTION_ID,
  ACTION_SET_MEMBER_ID,
  LAST_UPDATE_DATE,
  ACTION_LEVEL,
  COLUMN_WRAP_FLAG,
  MAXIMUM_SUMMARY_MESSAGE_WIDTH,
  ACTION_EXECUTED_FLAG,
  SUCCESS_FLAG,
  VERSION_NUMBER,
  MESSAGE_HANDLE,
  NODE_HANDLE,
  SECURITY_GROUP_ID,
  KCA_OPERATION,
  KCA_SEQ_ID,
  kca_seq_date
)
(
  SELECT
    APPLICATION_ID,
    ACTION_HISTORY_ID,
    ALERT_ID,
    ACTION_SET_ID,
    CHECK_ID,
    ACTION_ID,
    ACTION_SET_MEMBER_ID,
    LAST_UPDATE_DATE,
    ACTION_LEVEL,
    COLUMN_WRAP_FLAG,
    MAXIMUM_SUMMARY_MESSAGE_WIDTH,
    ACTION_EXECUTED_FLAG,
    SUCCESS_FLAG,
    VERSION_NUMBER,
    MESSAGE_HANDLE,
    NODE_HANDLE,
    SECURITY_GROUP_ID,
    KCA_OPERATION,
    KCA_SEQ_ID,
    kca_seq_date
  FROM bec_raw_dl_ext.alr_action_history
  WHERE
    kca_operation <> 'DELETE'
    AND (COALESCE(APPLICATION_ID, 0), COALESCE(ACTION_HISTORY_ID, 0), KCA_SEQ_ID) IN (
      SELECT
        COALESCE(APPLICATION_ID, 0) AS APPLICATION_ID,
        COALESCE(ACTION_HISTORY_ID, 0) AS ACTION_HISTORY_ID,
        MAX(KCA_SEQ_ID)
      FROM bec_raw_dl_ext.alr_action_history
      WHERE
        kca_operation <> 'DELETE' AND COALESCE(kca_seq_id, '') <> ''
      GROUP BY
        COALESCE(APPLICATION_ID, 0),
        COALESCE(ACTION_HISTORY_ID, 0)
    )
    AND kca_seq_date > (
      SELECT
        (
          executebegints - prune_days
        )
      FROM bec_etl_ctrl.batch_ods_info
      WHERE
        ods_table_name = 'alr_action_history'
    )
);