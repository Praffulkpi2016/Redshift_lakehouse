TRUNCATE table
	table bronze_bec_ods_stg.CS_INCIDENT_STATUSES_TL;
INSERT INTO bronze_bec_ods_stg.CS_INCIDENT_STATUSES_TL (
  INCIDENT_STATUS_ID,
  LANGUAGE,
  SOURCE_LANG,
  LAST_UPDATE_DATE,
  LAST_UPDATED_BY,
  CREATION_DATE,
  CREATED_BY,
  LAST_UPDATE_LOGIN,
  NAME,
  DESCRIPTION,
  SECURITY_GROUP_ID,
  ZD_EDITION_NAME,
  ZD_SYNC,
  KCA_OPERATION,
  kca_seq_id,
  kca_seq_date
)
(
  SELECT
    INCIDENT_STATUS_ID,
    LANGUAGE,
    SOURCE_LANG,
    LAST_UPDATE_DATE,
    LAST_UPDATED_BY,
    CREATION_DATE,
    CREATED_BY,
    LAST_UPDATE_LOGIN,
    NAME,
    DESCRIPTION,
    SECURITY_GROUP_ID,
    ZD_EDITION_NAME,
    ZD_SYNC,
    KCA_OPERATION,
    kca_seq_id,
    kca_seq_date
  FROM bec_raw_dl_ext.CS_INCIDENT_STATUSES_TL
  WHERE
    kca_operation <> 'DELETE'
    AND COALESCE(kca_seq_id, '') <> ''
    AND (COALESCE(INCIDENT_STATUS_ID, 0), COALESCE(LANGUAGE, 'NA'), kca_seq_id) IN (
      SELECT
        COALESCE(INCIDENT_STATUS_ID, 0) AS INCIDENT_STATUS_ID,
        COALESCE(LANGUAGE, 'NA') AS LANGUAGE,
        MAX(kca_seq_id) AS kca_seq_id
      FROM bec_raw_dl_ext.CS_INCIDENT_STATUSES_TL
      WHERE
        kca_operation <> 'DELETE' AND COALESCE(kca_seq_id, '') <> ''
      GROUP BY
        COALESCE(INCIDENT_STATUS_ID, 0),
        COALESCE(LANGUAGE, 'NA')
    )
    AND kca_seq_date > (
      SELECT
        (
          executebegints - prune_days
        )
      FROM bec_etl_ctrl.batch_ods_info
      WHERE
        ods_table_name = 'cs_incident_statuses_tl'
    )
);