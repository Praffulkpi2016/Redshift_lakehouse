/* Delete Records */
DELETE FROM silver_bec_ods.FUN_SEQ_VERSIONS
WHERE
  SEQ_VERSION_ID IN (
    SELECT
      stg.SEQ_VERSION_ID
    FROM silver_bec_ods.FUN_SEQ_VERSIONS AS ods, bronze_bec_ods_stg.FUN_SEQ_VERSIONS AS stg
    WHERE
      ods.SEQ_VERSION_ID = stg.SEQ_VERSION_ID AND stg.kca_operation IN ('INSERT', 'UPDATE')
  );
/* Insert records */
INSERT INTO silver_bec_ods.FUN_SEQ_VERSIONS (
  seq_version_id,
  seq_header_id,
  version_name,
  header_name,
  initial_value,
  start_date,
  end_date,
  current_value,
  use_status_code,
  db_sequence_name,
  object_version_number,
  last_update_date,
  last_updated_by,
  creation_date,
  created_by,
  last_update_login,
  zd_edition_name,
  zd_sync,
  kca_operation,
  is_deleted_flg,
  kca_seq_id,
  kca_seq_date
)
(
  SELECT
    seq_version_id,
    seq_header_id,
    version_name,
    header_name,
    initial_value,
    start_date,
    end_date,
    current_value,
    use_status_code,
    db_sequence_name,
    object_version_number,
    last_update_date,
    last_updated_by,
    creation_date,
    created_by,
    last_update_login,
    zd_edition_name,
    zd_sync,
    kca_operation,
    'N' AS IS_DELETED_FLG,
    CAST(NULLIF(KCA_SEQ_ID, '') AS DECIMAL(36, 0)) AS KCA_SEQ_ID,
    kca_seq_date
  FROM bronze_bec_ods_stg.FUN_SEQ_VERSIONS
  WHERE
    kca_operation IN ('INSERT', 'UPDATE')
    AND (SEQ_VERSION_ID, kca_seq_id) IN (
      SELECT
        SEQ_VERSION_ID,
        MAX(kca_seq_id)
      FROM bronze_bec_ods_stg.FUN_SEQ_VERSIONS
      WHERE
        kca_operation IN ('INSERT', 'UPDATE')
      GROUP BY
        SEQ_VERSION_ID
    )
);
/* Soft delete */
UPDATE silver_bec_ods.FUN_SEQ_VERSIONS SET IS_DELETED_FLG = 'N';
UPDATE silver_bec_ods.FUN_SEQ_VERSIONS SET IS_DELETED_FLG = 'Y'
WHERE
  (
    SEQ_VERSION_ID
  ) IN (
    SELECT
      SEQ_VERSION_ID
    FROM bec_raw_dl_ext.FUN_SEQ_VERSIONS
    WHERE
      (SEQ_VERSION_ID, KCA_SEQ_ID) IN (
        SELECT
          SEQ_VERSION_ID,
          MAX(KCA_SEQ_ID) AS KCA_SEQ_ID
        FROM bec_raw_dl_ext.FUN_SEQ_VERSIONS
        GROUP BY
          SEQ_VERSION_ID
      )
      AND kca_operation = 'DELETE'
  );
UPDATE bec_etl_ctrl.batch_ods_info SET last_refresh_date = CURRENT_TIMESTAMP()
WHERE
  ods_table_name = 'fun_seq_versions';