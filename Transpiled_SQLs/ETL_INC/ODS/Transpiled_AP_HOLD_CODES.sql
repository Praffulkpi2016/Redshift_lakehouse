/* Delete Records */
DELETE FROM silver_bec_ods.ap_hold_codes
WHERE
  hold_lookup_code IN (
    SELECT
      stg.hold_lookup_code
    FROM silver_bec_ods.ap_hold_codes AS ods, bronze_bec_ods_stg.ap_hold_codes AS stg
    WHERE
      ods.hold_lookup_code = stg.hold_lookup_code
      AND stg.kca_operation IN ('INSERT', 'UPDATE')
  );
/* Insert records */
INSERT INTO silver_bec_ods.ap_hold_codes (
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
  IS_DELETED_FLG,
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
    'N' AS IS_DELETED_FLG,
    CAST(NULLIF(KCA_SEQ_ID, '') AS DECIMAL(36, 0)) AS KCA_SEQ_ID,
    kca_seq_date
  FROM bronze_bec_ods_stg.ap_hold_codes
  WHERE
    kca_operation IN ('INSERT', 'UPDATE')
    AND (hold_lookup_code, KCA_SEQ_ID) IN (
      SELECT
        hold_lookup_code,
        MAX(KCA_SEQ_ID)
      FROM bronze_bec_ods_stg.ap_hold_codes
      WHERE
        kca_operation IN ('INSERT', 'UPDATE')
      GROUP BY
        hold_lookup_code
    )
);
/* Soft delete */
UPDATE silver_bec_ods.ap_hold_codes SET IS_DELETED_FLG = 'N';
UPDATE silver_bec_ods.ap_hold_codes SET IS_DELETED_FLG = 'Y'
WHERE
  (
    hold_lookup_code
  ) IN (
    SELECT
      hold_lookup_code
    FROM bec_raw_dl_ext.ap_hold_codes
    WHERE
      (hold_lookup_code, KCA_SEQ_ID) IN (
        SELECT
          hold_lookup_code,
          MAX(KCA_SEQ_ID) AS KCA_SEQ_ID
        FROM bec_raw_dl_ext.ap_hold_codes
        GROUP BY
          hold_lookup_code
      )
      AND kca_operation = 'DELETE'
  );
UPDATE bec_etl_ctrl.batch_ods_info SET last_refresh_date = CURRENT_TIMESTAMP()
WHERE
  ods_table_name = 'ap_hold_codes';