/* Delete Records */
DELETE FROM silver_bec_ods.ALR_ACTION_HISTORY
WHERE
  (COALESCE(APPLICATION_ID, 0), COALESCE(ACTION_HISTORY_ID, 0)) IN (
    SELECT
      COALESCE(stg.APPLICATION_ID, 0) AS APPLICATION_ID,
      COALESCE(stg.ACTION_HISTORY_ID, 0) AS ACTION_HISTORY_ID
    FROM silver_bec_ods.alr_action_history AS ods, bronze_bec_ods_stg.alr_action_history AS stg
    WHERE
      COALESCE(ods.APPLICATION_ID, 0) = COALESCE(stg.APPLICATION_ID, 0)
      AND COALESCE(ods.ACTION_HISTORY_ID, 0) = COALESCE(stg.ACTION_HISTORY_ID, 0)
      AND stg.kca_operation IN ('INSERT', 'UPDATE')
  );
/* Insert records */
INSERT INTO silver_bec_ods.alr_action_history (
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
  kca_operation,
  IS_DELETED_FLG,
  kca_seq_id,
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
    kca_operation,
    'N' AS IS_DELETED_FLG,
    CAST(NULLIF(KCA_SEQ_ID, '') AS DECIMAL(36, 0)) AS KCA_SEQ_ID,
    kca_seq_date
  FROM bronze_bec_ods_stg.alr_action_history
  WHERE
    kca_operation IN ('INSERT', 'UPDATE')
    AND (COALESCE(APPLICATION_ID, 0), COALESCE(ACTION_HISTORY_ID, 0), KCA_SEQ_ID) IN (
      SELECT
        COALESCE(APPLICATION_ID, 0) AS APPLICATION_ID,
        COALESCE(ACTION_HISTORY_ID, 0) AS ACTION_HISTORY_ID,
        MAX(KCA_SEQ_ID)
      FROM bronze_bec_ods_stg.alr_action_history
      WHERE
        kca_operation IN ('INSERT', 'UPDATE')
      GROUP BY
        APPLICATION_ID,
        ACTION_HISTORY_ID
    )
);
/* Soft delete */
UPDATE silver_bec_ods.alr_action_history SET IS_DELETED_FLG = 'N';
UPDATE silver_bec_ods.alr_action_history SET IS_DELETED_FLG = 'Y'
WHERE
  (COALESCE(APPLICATION_ID, 0), COALESCE(ACTION_HISTORY_ID, 0)) IN (
    SELECT
      APPLICATION_ID,
      ACTION_HISTORY_ID
    FROM bec_raw_dl_ext.alr_action_history
    WHERE
      (APPLICATION_ID, ACTION_HISTORY_ID, KCA_SEQ_ID) IN (
        SELECT
          COALESCE(APPLICATION_ID, 0) AS APPLICATION_ID,
          COALESCE(ACTION_HISTORY_ID, 0) AS ACTION_HISTORY_ID,
          MAX(KCA_SEQ_ID) AS KCA_SEQ_ID
        FROM bec_raw_dl_ext.alr_action_history
        GROUP BY
          COALESCE(APPLICATION_ID, 0),
          COALESCE(ACTION_HISTORY_ID, 0)
      )
      AND kca_operation = 'DELETE'
  );
UPDATE bec_etl_ctrl.batch_ods_info SET last_refresh_date = CURRENT_TIMESTAMP()
WHERE
  ods_table_name = 'alr_action_history';