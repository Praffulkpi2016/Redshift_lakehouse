TRUNCATE table bronze_bec_ods_stg.ORG_ACCESS;
INSERT INTO bronze_bec_ods_stg.ORG_ACCESS (
  organization_id,
  resp_application_id,
  responsibility_id,
  last_update_date,
  last_updated_by,
  creation_date,
  created_by,
  last_update_login,
  disable_date,
  comments,
  request_id,
  program_application_id,
  program_id,
  program_update_date,
  KCA_OPERATION,
  kca_seq_id,
  kca_seq_date
)
(
  SELECT
    organization_id,
    resp_application_id,
    responsibility_id,
    last_update_date,
    last_updated_by,
    creation_date,
    created_by,
    last_update_login,
    disable_date,
    comments,
    request_id,
    program_application_id,
    program_id,
    program_update_date,
    KCA_OPERATION,
    kca_seq_id,
    kca_seq_date
  FROM bec_raw_dl_ext.ORG_ACCESS
  WHERE
    kca_operation <> 'DELETE'
    AND COALESCE(kca_seq_id, '') <> ''
    AND (COALESCE(RESP_APPLICATION_ID, 0), COALESCE(RESPONSIBILITY_ID, 0), COALESCE(ORGANIZATION_ID, 0), kca_seq_id) IN (
      SELECT
        COALESCE(RESP_APPLICATION_ID, 0) AS RESP_APPLICATION_ID,
        COALESCE(RESPONSIBILITY_ID, 0) AS RESPONSIBILITY_ID,
        COALESCE(ORGANIZATION_ID, 0) AS ORGANIZATION_ID,
        MAX(kca_seq_id)
      FROM bec_raw_dl_ext.ORG_ACCESS
      WHERE
        kca_operation <> 'DELETE' AND COALESCE(kca_seq_id, '') <> ''
      GROUP BY
        COALESCE(RESP_APPLICATION_ID, 0),
        COALESCE(RESPONSIBILITY_ID, 0),
        COALESCE(ORGANIZATION_ID, 0)
    )
    AND kca_seq_date > (
      SELECT
        (
          executebegints - prune_days
        )
      FROM bec_etl_ctrl.batch_ods_info
      WHERE
        ods_table_name = 'org_access'
    )
);