TRUNCATE table
	table bronze_bec_ods_stg.JTF_RS_GROUPS_TL;
INSERT INTO bronze_bec_ods_stg.JTF_RS_GROUPS_TL (
  GROUP_ID,
  CREATED_BY,
  CREATION_DATE,
  LAST_UPDATED_BY,
  LAST_UPDATE_DATE,
  LAST_UPDATE_LOGIN,
  GROUP_NAME,
  GROUP_DESC,
  LANGUAGE,
  SOURCE_LANG,
  SECURITY_GROUP_ID,
  KCA_OPERATION,
  kca_seq_id,
  kca_seq_date
)
(
  SELECT
    GROUP_ID,
    CREATED_BY,
    CREATION_DATE,
    LAST_UPDATED_BY,
    LAST_UPDATE_DATE,
    LAST_UPDATE_LOGIN,
    GROUP_NAME,
    GROUP_DESC,
    LANGUAGE,
    SOURCE_LANG,
    SECURITY_GROUP_ID,
    KCA_OPERATION,
    kca_seq_id,
    kca_seq_date
  FROM bec_raw_dl_ext.JTF_RS_GROUPS_TL
  WHERE
    kca_operation <> 'DELETE'
    AND COALESCE(kca_seq_id, '') <> ''
    AND (COALESCE(GROUP_ID, 0), COALESCE(LANGUAGE, 'NA'), kca_seq_id) IN (
      SELECT
        COALESCE(GROUP_ID, 0) AS GROUP_ID,
        COALESCE(LANGUAGE, 'NA') AS LANGUAGE,
        MAX(kca_seq_id) AS kca_seq_id
      FROM bec_raw_dl_ext.JTF_RS_GROUPS_TL
      WHERE
        kca_operation <> 'DELETE' AND COALESCE(kca_seq_id, '') <> ''
      GROUP BY
        COALESCE(GROUP_ID, 0),
        COALESCE(LANGUAGE, 'NA')
    )
    AND kca_seq_date > (
      SELECT
        (
          executebegints - prune_days
        )
      FROM bec_etl_ctrl.batch_ods_info
      WHERE
        ods_table_name = 'jtf_rs_groups_tl'
    )
);