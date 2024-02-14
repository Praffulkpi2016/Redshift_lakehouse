TRUNCATE table bronze_bec_ods_stg.JTF_RS_RESOURCE_EXTNS_TL;
INSERT INTO bronze_bec_ods_stg.jtf_rs_resource_extns_tl (
  created_by,
  resource_id,
  category,
  creation_date,
  last_updated_by,
  last_update_date,
  last_update_login,
  `language`,
  resource_name,
  source_lang,
  security_group_id,
  kca_operation,
  kca_seq_id,
  kca_seq_date
)
SELECT
  created_by,
  resource_id,
  category,
  creation_date,
  last_updated_by,
  last_update_date,
  last_update_login,
  `language`,
  resource_name,
  source_lang,
  security_group_id,
  KCA_OPERATION,
  KCA_SEQ_ID, /* 'N' as IS_DELETED_FLG, */
  kca_seq_date
FROM bec_raw_dl_ext.jtf_rs_resource_extns_tl
WHERE
  kca_operation <> 'DELETE'
  AND COALESCE(kca_seq_id, '') <> ''
  AND (RESOURCE_ID, CATEGORY, LANGUAGE, kca_seq_id) IN (
    SELECT
      RESOURCE_ID,
      CATEGORY,
      LANGUAGE,
      MAX(kca_seq_id)
    FROM bec_raw_dl_ext.jtf_rs_resource_extns_tl
    WHERE
      kca_operation <> 'DELETE' AND COALESCE(kca_seq_id, '') <> ''
    GROUP BY
      RESOURCE_ID,
      CATEGORY,
      LANGUAGE
  )
  AND (
    kca_seq_date > (
      SELECT
        (
          executebegints - prune_days
        )
      FROM bec_etl_ctrl.batch_ods_info
      WHERE
        ods_table_name = 'jtf_rs_resource_extns_tl'
    )
  );