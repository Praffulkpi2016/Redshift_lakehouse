/* Delete Records */
DELETE FROM silver_bec_ods.PO_AGENTS
WHERE
  (
    AGENT_ID
  ) IN (
    SELECT
      stg.AGENT_ID
    FROM silver_bec_ods.PO_AGENTS AS ods, bronze_bec_ods_stg.PO_AGENTS AS stg
    WHERE
      ods.AGENT_ID = stg.AGENT_ID AND stg.kca_operation IN ('INSERT', 'UPDATE')
  );
/* Insert records */
INSERT INTO silver_bec_ods.PO_AGENTS (
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
  is_deleted_flg,
  kca_seq_id,
  kca_seq_date
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
    KCA_OPERATION,
    'N' AS IS_DELETED_FLG,
    CAST(NULLIF(KCA_SEQ_ID, '') AS DECIMAL(36, 0)) AS KCA_SEQ_ID,
    KCA_SEQ_DATE
  FROM bronze_bec_ods_stg.PO_AGENTS
  WHERE
    kca_operation IN ('INSERT', 'UPDATE')
    AND (AGENT_ID, kca_seq_id) IN (
      SELECT
        AGENT_ID,
        MAX(kca_seq_id)
      FROM bronze_bec_ods_stg.PO_AGENTS
      WHERE
        kca_operation IN ('INSERT', 'UPDATE')
      GROUP BY
        AGENT_ID
    )
);
/* Soft delete */
UPDATE silver_bec_ods.PO_AGENTS SET IS_DELETED_FLG = 'N';
UPDATE silver_bec_ods.PO_AGENTS SET IS_DELETED_FLG = 'Y'
WHERE
  (
    AGENT_ID
  ) IN (
    SELECT
      AGENT_ID
    FROM bec_raw_dl_ext.PO_AGENTS
    WHERE
      (AGENT_ID, KCA_SEQ_ID) IN (
        SELECT
          AGENT_ID,
          MAX(KCA_SEQ_ID) AS KCA_SEQ_ID
        FROM bec_raw_dl_ext.PO_AGENTS
        GROUP BY
          AGENT_ID
      )
      AND kca_operation = 'DELETE'
  );
UPDATE bec_etl_ctrl.batch_ods_info SET last_refresh_date = CURRENT_TIMESTAMP()
WHERE
  ods_table_name = 'po_agents';