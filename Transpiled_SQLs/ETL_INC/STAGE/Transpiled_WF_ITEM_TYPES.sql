TRUNCATE table bronze_bec_ods_stg.wf_item_types;
INSERT INTO bronze_bec_ods_stg.wf_item_types (
  NAME,
  PROTECT_LEVEL,
  CUSTOM_LEVEL,
  WF_SELECTOR,
  READ_ROLE,
  WRITE_ROLE,
  EXECUTE_ROLE,
  PERSISTENCE_TYPE,
  PERSISTENCE_DAYS,
  SECURITY_GROUP_ID,
  NUM_ACTIVE,
  NUM_ERROR,
  NUM_DEFER,
  NUM_SUSPEND,
  NUM_COMPLETE,
  NUM_PURGEABLE,
  ZD_EDITION_NAME,
  ZD_SYNC,
  KCA_OPERATION,
  KCA_SEQ_ID,
  KCA_SEQ_DATE
)
(
  SELECT
    NAME,
    PROTECT_LEVEL,
    CUSTOM_LEVEL,
    WF_SELECTOR,
    READ_ROLE,
    WRITE_ROLE,
    EXECUTE_ROLE,
    PERSISTENCE_TYPE,
    PERSISTENCE_DAYS,
    SECURITY_GROUP_ID,
    NUM_ACTIVE,
    NUM_ERROR,
    NUM_DEFER,
    NUM_SUSPEND,
    NUM_COMPLETE,
    NUM_PURGEABLE,
    ZD_EDITION_NAME,
    ZD_SYNC,
    KCA_OPERATION,
    KCA_SEQ_ID,
    KCA_SEQ_DATE
  FROM bec_raw_dl_ext.wf_item_types
  WHERE
    kca_operation <> 'DELETE'
    AND COALESCE(kca_seq_id, '') <> ''
    AND (COALESCE(NAME, 'NA'), kca_seq_id) IN (
      SELECT
        COALESCE(NAME, 'NA') AS NAME,
        MAX(kca_seq_id)
      FROM bec_raw_dl_ext.wf_item_types
      WHERE
        kca_operation <> 'DELETE' AND COALESCE(kca_seq_id, '') <> ''
      GROUP BY
        COALESCE(NAME, 'NA')
    )
    AND KCA_SEQ_DATE > (
      SELECT
        (
          executebegints - prune_days
        )
      FROM bec_etl_ctrl.batch_ods_info
      WHERE
        ods_table_name = 'wf_item_types'
    )
);