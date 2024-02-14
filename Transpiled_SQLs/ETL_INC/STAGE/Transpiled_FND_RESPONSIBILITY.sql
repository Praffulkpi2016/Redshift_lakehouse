TRUNCATE table bronze_bec_ods_stg.FND_RESPONSIBILITY;
INSERT INTO bronze_bec_ods_stg.FND_RESPONSIBILITY (
  APPLICATION_ID,
  RESPONSIBILITY_ID,
  LAST_UPDATE_DATE,
  LAST_UPDATED_BY,
  CREATION_DATE,
  CREATED_BY,
  LAST_UPDATE_LOGIN,
  DATA_GROUP_APPLICATION_ID,
  DATA_GROUP_ID,
  MENU_ID,
  TERM_SECURITY_ENABLED_FLAG,
  START_DATE,
  END_DATE,
  GROUP_APPLICATION_ID,
  REQUEST_GROUP_ID,
  VERSION,
  WEB_HOST_NAME,
  WEB_AGENT_NAME,
  RESPONSIBILITY_KEY,
  ZD_EDITION_NAME,
  ZD_SYNC,
  KCA_OPERATION,
  kca_seq_id,
  kca_seq_date
)
(
  SELECT
    APPLICATION_ID,
    RESPONSIBILITY_ID,
    LAST_UPDATE_DATE,
    LAST_UPDATED_BY,
    CREATION_DATE,
    CREATED_BY,
    LAST_UPDATE_LOGIN,
    DATA_GROUP_APPLICATION_ID,
    DATA_GROUP_ID,
    MENU_ID,
    TERM_SECURITY_ENABLED_FLAG,
    START_DATE,
    END_DATE,
    GROUP_APPLICATION_ID,
    REQUEST_GROUP_ID,
    VERSION,
    WEB_HOST_NAME,
    WEB_AGENT_NAME,
    RESPONSIBILITY_KEY,
    ZD_EDITION_NAME,
    ZD_SYNC,
    KCA_OPERATION,
    kca_seq_id,
    kca_seq_date
  FROM bec_raw_dl_ext.FND_RESPONSIBILITY
  WHERE
    kca_operation <> 'DELETE'
    AND COALESCE(kca_seq_id, '') <> ''
    AND (COALESCE(APPLICATION_ID, 0), COALESCE(RESPONSIBILITY_ID, 0), kca_seq_id) IN (
      SELECT
        COALESCE(APPLICATION_ID, 0),
        COALESCE(RESPONSIBILITY_ID, 0),
        MAX(kca_seq_id)
      FROM bec_raw_dl_ext.FND_RESPONSIBILITY
      WHERE
        kca_operation <> 'DELETE' AND COALESCE(kca_seq_id, '') <> ''
      GROUP BY
        COALESCE(APPLICATION_ID, 0),
        COALESCE(RESPONSIBILITY_ID, 0)
    )
    AND (
      kca_seq_date > (
        SELECT
          (
            executebegints - prune_days
          )
        FROM bec_etl_ctrl.batch_ods_info
        WHERE
          ods_table_name = 'fnd_responsibility'
      )
    )
);