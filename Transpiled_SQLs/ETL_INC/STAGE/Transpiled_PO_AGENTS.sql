TRUNCATE table bronze_bec_ods_stg.PO_AGENTS;
INSERT INTO bronze_bec_ods_stg.PO_AGENTS (
  agent_id,
  last_update_date,
  last_updated_by,
  last_update_login,
  creation_date,
  created_by,
  location_id,
  category_id,
  authorization_limit,
  start_date_active,
  end_date_active,
  attribute_category,
  attribute1,
  attribute2,
  attribute3,
  attribute4,
  attribute5,
  attribute6,
  attribute7,
  attribute8,
  attribute9,
  attribute10,
  attribute11,
  attribute12,
  attribute13,
  attribute14,
  attribute15,
  request_id,
  program_application_id,
  program_id,
  program_update_date,
  IS_CONTRACT_OFFICER,
  WARRANT_ID,
  kca_operation,
  kca_seq_id,
  KCA_SEQ_DATE
)
(
  SELECT
    agent_id,
    last_update_date,
    last_updated_by,
    last_update_login,
    creation_date,
    created_by,
    location_id,
    category_id,
    authorization_limit,
    start_date_active,
    end_date_active,
    attribute_category,
    attribute1,
    attribute2,
    attribute3,
    attribute4,
    attribute5,
    attribute6,
    attribute7,
    attribute8,
    attribute9,
    attribute10,
    attribute11,
    attribute12,
    attribute13,
    attribute14,
    attribute15,
    request_id,
    program_application_id,
    program_id,
    program_update_date,
    IS_CONTRACT_OFFICER,
    WARRANT_ID,
    kca_operation,
    kca_seq_id,
    KCA_SEQ_DATE
  FROM bec_raw_dl_ext.PO_AGENTS
  WHERE
    kca_operation <> 'DELETE'
    AND COALESCE(kca_seq_id, '') <> ''
    AND (AGENT_ID, kca_seq_id) IN (
      SELECT
        AGENT_ID,
        MAX(kca_seq_id)
      FROM bec_raw_dl_ext.PO_AGENTS
      WHERE
        kca_operation <> 'DELETE' AND COALESCE(kca_seq_id, '') <> ''
      GROUP BY
        AGENT_ID
    )
    AND (
      KCA_SEQ_DATE > (
        SELECT
          (
            executebegints - prune_days
          )
        FROM bec_etl_ctrl.batch_ods_info
        WHERE
          ods_table_name = 'po_agents'
      )
    )
);