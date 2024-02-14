TRUNCATE table bronze_bec_ods_stg.ap_hold_codes;
INSERT INTO bronze_bec_ods_stg.ap_hold_codes (
  hold_lookup_code,
  hold_type,
  description,
  last_update_date,
  last_updated_by,
  user_releaseable_flag,
  user_updateable_flag,
  inactive_date,
  postable_flag,
  last_update_login,
  creation_date,
  created_by,
  external_description,
  hold_instruction,
  wait_before_notify_days,
  reminder_days,
  initiate_workflow_flag,
  zd_edition_name,
  zd_sync,
  KCA_OPERATION,
  kca_seq_id,
  kca_seq_date
)
(
  SELECT
    hold_lookup_code,
    hold_type,
    description,
    last_update_date,
    last_updated_by,
    user_releaseable_flag,
    user_updateable_flag,
    inactive_date,
    postable_flag,
    last_update_login,
    creation_date,
    created_by,
    external_description,
    hold_instruction,
    wait_before_notify_days,
    reminder_days,
    initiate_workflow_flag,
    zd_edition_name,
    zd_sync,
    KCA_OPERATION,
    kca_seq_id,
    kca_seq_date
  FROM bec_raw_dl_ext.ap_hold_codes
  WHERE
    kca_operation <> 'DELETE'
    AND COALESCE(kca_seq_id, '') <> ''
    AND (hold_lookup_code, KCA_SEQ_ID) IN (
      SELECT
        hold_lookup_code,
        MAX(KCA_SEQ_ID)
      FROM bec_raw_dl_ext.ap_hold_codes
      WHERE
        kca_operation <> 'DELETE' AND COALESCE(kca_seq_id, '') <> ''
      GROUP BY
        hold_lookup_code
    )
    AND kca_seq_date > (
      SELECT
        (
          executebegints - prune_days
        )
      FROM bec_etl_ctrl.batch_ods_info
      WHERE
        ods_table_name = 'ap_hold_codes'
    )
);