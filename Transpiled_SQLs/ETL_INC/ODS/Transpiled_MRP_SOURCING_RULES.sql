/* Delete Records */
DELETE FROM silver_bec_ods.MRP_SOURCING_RULES
WHERE
  COALESCE(SOURCING_RULE_ID, 0) IN (
    SELECT
      COALESCE(stg.SOURCING_RULE_ID, 0)
    FROM silver_bec_ods.MRP_SOURCING_RULES AS ods, bronze_bec_ods_stg.MRP_SOURCING_RULES AS stg
    WHERE
      ods.SOURCING_RULE_ID = stg.SOURCING_RULE_ID
      AND stg.kca_operation IN ('INSERT', 'UPDATE')
  );
/* Insert records */
INSERT INTO silver_bec_ods.MRP_SOURCING_RULES (
  sourcing_rule_id,
  sourcing_rule_name,
  organization_id,
  description,
  status,
  sourcing_rule_type,
  last_update_date,
  last_updated_by,
  creation_date,
  created_by,
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
  last_update_login,
  request_id,
  program_application_id,
  program_id,
  program_update_date,
  planning_active,
  KCA_OPERATION,
  IS_DELETED_FLG,
  kca_seq_id,
  kca_seq_date
)
(
  SELECT
    sourcing_rule_id,
    sourcing_rule_name,
    organization_id,
    description,
    status,
    sourcing_rule_type,
    last_update_date,
    last_updated_by,
    creation_date,
    created_by,
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
    last_update_login,
    request_id,
    program_application_id,
    program_id,
    program_update_date,
    planning_active,
    KCA_OPERATION,
    'N' AS IS_DELETED_FLG,
    CAST(NULLIF(KCA_SEQ_ID, '') AS DECIMAL(36, 0)) AS KCA_SEQ_ID,
    kca_seq_date
  FROM bronze_bec_ods_stg.MRP_SOURCING_RULES
  WHERE
    kca_operation IN ('INSERT', 'UPDATE')
    AND (SOURCING_RULE_ID, kca_seq_id) IN (
      SELECT
        SOURCING_RULE_ID,
        MAX(kca_seq_id)
      FROM bronze_bec_ods_stg.MRP_SOURCING_RULES
      WHERE
        kca_operation IN ('INSERT', 'UPDATE')
      GROUP BY
        SOURCING_RULE_ID
    )
);
/* Soft delete */
UPDATE silver_bec_ods.MRP_SOURCING_RULES SET IS_DELETED_FLG = 'N';
UPDATE silver_bec_ods.MRP_SOURCING_RULES SET IS_DELETED_FLG = 'Y'
WHERE
  (
    SOURCING_RULE_ID
  ) IN (
    SELECT
      SOURCING_RULE_ID
    FROM bec_raw_dl_ext.MRP_SOURCING_RULES
    WHERE
      (SOURCING_RULE_ID, KCA_SEQ_ID) IN (
        SELECT
          SOURCING_RULE_ID,
          MAX(KCA_SEQ_ID) AS KCA_SEQ_ID
        FROM bec_raw_dl_ext.MRP_SOURCING_RULES
        GROUP BY
          SOURCING_RULE_ID
      )
      AND kca_operation = 'DELETE'
  );
UPDATE bec_etl_ctrl.batch_ods_info SET last_refresh_date = CURRENT_TIMESTAMP()
WHERE
  ods_table_name = 'mrp_sourcing_rules';