TRUNCATE table bronze_bec_ods_stg.fnd_new_messages;
INSERT INTO bronze_bec_ods_stg.fnd_new_messages (
  APPLICATION_ID,
  LANGUAGE_CODE,
  MESSAGE_NUMBER,
  MESSAGE_NAME,
  MESSAGE_TEXT,
  CREATION_DATE,
  CREATED_BY,
  LAST_UPDATE_DATE,
  LAST_UPDATED_BY,
  LAST_UPDATE_LOGIN,
  DESCRIPTION,
  TYPE,
  MAX_LENGTH,
  CATEGORY,
  SEVERITY,
  FND_LOG_SEVERITY,
  ZD_EDITION_NAME,
  ZD_SYNC,
  KCA_OPERATION,
  kca_seq_id,
  kca_seq_date
)
(
  SELECT
    APPLICATION_ID,
    LANGUAGE_CODE,
    MESSAGE_NUMBER,
    MESSAGE_NAME,
    MESSAGE_TEXT,
    CREATION_DATE,
    CREATED_BY,
    LAST_UPDATE_DATE,
    LAST_UPDATED_BY,
    LAST_UPDATE_LOGIN,
    DESCRIPTION,
    TYPE,
    MAX_LENGTH,
    CATEGORY,
    SEVERITY,
    FND_LOG_SEVERITY,
    ZD_EDITION_NAME,
    ZD_SYNC,
    KCA_OPERATION,
    kca_seq_id,
    kca_seq_date
  FROM bec_raw_dl_ext.fnd_new_messages
  WHERE
    kca_operation <> 'DELETE'
    AND COALESCE(kca_seq_id, '') <> ''
    AND (COALESCE(APPLICATION_ID, 0), COALESCE(MESSAGE_NAME, 'NA'), COALESCE(LANGUAGE_CODE, 'NA'), COALESCE(MESSAGE_NUMBER, 0), COALESCE(ZD_EDITION_NAME, 'NA'), kca_seq_id, last_update_date) IN (
      SELECT
        COALESCE(APPLICATION_ID, 0) AS APPLICATION_ID,
        COALESCE(MESSAGE_NAME, 'NA') AS MESSAGE_NAME,
        COALESCE(LANGUAGE_CODE, 'NA') AS LANGUAGE_CODE,
        COALESCE(MESSAGE_NUMBER, 0) AS MESSAGE_NUMBER,
        COALESCE(ZD_EDITION_NAME, 'NA') AS ZD_EDITION_NAME,
        MAX(kca_seq_id),
        MAX(last_update_date)
      FROM bec_raw_dl_ext.fnd_new_messages
      WHERE
        kca_operation <> 'DELETE' AND COALESCE(kca_seq_id, '') <> ''
      GROUP BY
        COALESCE(APPLICATION_ID, 0),
        COALESCE(MESSAGE_NAME, 'NA'),
        COALESCE(LANGUAGE_CODE, 'NA'),
        COALESCE(MESSAGE_NUMBER, 0),
        COALESCE(ZD_EDITION_NAME, 'NA')
    )
    AND (
      kca_seq_date > (
        SELECT
          (
            executebegints - prune_days
          )
        FROM bec_etl_ctrl.batch_ods_info
        WHERE
          ods_table_name = 'fnd_new_messages'
      )
    )
);