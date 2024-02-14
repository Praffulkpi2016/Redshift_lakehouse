TRUNCATE table
	table bronze_bec_ods_stg.JTF_TASK_TYPES_TL;
INSERT INTO bronze_bec_ods_stg.JTF_TASK_TYPES_TL (
  TASK_TYPE_ID,
  LANGUAGE,
  SOURCE_LANG,
  NAME,
  DESCRIPTION,
  CREATED_BY,
  CREATION_DATE,
  LAST_UPDATED_BY,
  LAST_UPDATE_DATE,
  LAST_UPDATE_LOGIN,
  SECURITY_GROUP_ID,
  ZD_EDITION_NAME,
  ZD_SYNC,
  KCA_OPERATION,
  kca_seq_id,
  kca_seq_date
)
(
  SELECT
    TASK_TYPE_ID,
    LANGUAGE,
    SOURCE_LANG,
    NAME,
    DESCRIPTION,
    CREATED_BY,
    CREATION_DATE,
    LAST_UPDATED_BY,
    LAST_UPDATE_DATE,
    LAST_UPDATE_LOGIN,
    SECURITY_GROUP_ID,
    ZD_EDITION_NAME,
    ZD_SYNC,
    KCA_OPERATION,
    kca_seq_id,
    kca_seq_date
  FROM bec_raw_dl_ext.JTF_TASK_TYPES_TL
  WHERE
    kca_operation <> 'DELETE'
    AND COALESCE(kca_seq_id, '') <> ''
    AND (COALESCE(TASK_TYPE_ID, 0), COALESCE(LANGUAGE, 'NA'), kca_seq_id) IN (
      SELECT
        COALESCE(TASK_TYPE_ID, 0) AS TASK_TYPE_ID,
        COALESCE(LANGUAGE, 'NA') AS LANGUAGE,
        MAX(kca_seq_id) AS kca_seq_id
      FROM bec_raw_dl_ext.JTF_TASK_TYPES_TL
      WHERE
        kca_operation <> 'DELETE' AND COALESCE(kca_seq_id, '') <> ''
      GROUP BY
        COALESCE(TASK_TYPE_ID, 0),
        COALESCE(LANGUAGE, 'NA')
    )
    AND kca_seq_date > (
      SELECT
        (
          executebegints - prune_days
        )
      FROM bec_etl_ctrl.batch_ods_info
      WHERE
        ods_table_name = 'jtf_task_types_tl'
    )
);