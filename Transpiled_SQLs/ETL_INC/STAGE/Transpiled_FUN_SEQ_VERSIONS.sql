TRUNCATE table
	table bronze_bec_ods_stg.FUN_SEQ_VERSIONS;
INSERT INTO bronze_bec_ods_stg.FUN_SEQ_VERSIONS (
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
  KCA_OPERATION,
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
    KCA_OPERATION,
    kca_seq_id,
    kca_seq_date
  FROM bec_raw_dl_ext.FUN_SEQ_VERSIONS
  WHERE
    kca_operation <> 'DELETE'
    AND COALESCE(kca_seq_id, '') <> ''
    AND (SEQ_VERSION_ID, kca_seq_id) IN (
      SELECT
        SEQ_VERSION_ID,
        MAX(kca_seq_id)
      FROM bec_raw_dl_ext.FUN_SEQ_VERSIONS
      WHERE
        kca_operation <> 'DELETE' AND COALESCE(kca_seq_id, '') <> ''
      GROUP BY
        SEQ_VERSION_ID
    )
    AND (
      kca_seq_date > (
        SELECT
          (
            executebegints - prune_days
          )
        FROM bec_etl_ctrl.batch_ods_info
        WHERE
          ods_table_name = 'fun_seq_versions'
      )
    )
);