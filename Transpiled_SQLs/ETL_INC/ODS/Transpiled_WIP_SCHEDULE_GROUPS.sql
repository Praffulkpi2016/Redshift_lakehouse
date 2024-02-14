/* Delete Records */
DELETE FROM silver_bec_ods.WIP_SCHEDULE_GROUPS
WHERE
  SCHEDULE_GROUP_ID IN (
    SELECT
      stg.SCHEDULE_GROUP_ID
    FROM silver_bec_ods.WIP_SCHEDULE_GROUPS AS ods, bronze_bec_ods_stg.WIP_SCHEDULE_GROUPS AS stg
    WHERE
      ods.SCHEDULE_GROUP_ID = stg.SCHEDULE_GROUP_ID
      AND stg.kca_operation IN ('INSERT', 'UPDATE')
  );
/* Insert records */
INSERT INTO silver_bec_ods.WIP_SCHEDULE_GROUPS (
  schedule_group_name,
  schedule_group_id,
  organization_id,
  description,
  inactive_on,
  last_update_date,
  last_updated_by,
  creation_date,
  created_by,
  last_update_login,
  program_application_id,
  program_id,
  program_update_date,
  attribute_category,
  request_id,
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
  kca_operation,
  is_deleted_flg,
  kca_seq_id,
  kca_seq_date
)
(
  SELECT
    schedule_group_name,
    schedule_group_id,
    organization_id,
    description,
    inactive_on,
    last_update_date,
    last_updated_by,
    creation_date,
    created_by,
    last_update_login,
    program_application_id,
    program_id,
    program_update_date,
    attribute_category,
    request_id,
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
    kca_operation,
    'N' AS IS_DELETED_FLG,
    CAST(NULLIF(KCA_SEQ_ID, '') AS DECIMAL(36, 0)) AS KCA_SEQ_ID,
    KCA_SEQ_DATE
  FROM bronze_bec_ods_stg.WIP_SCHEDULE_GROUPS
  WHERE
    kca_operation IN ('INSERT', 'UPDATE')
    AND (SCHEDULE_GROUP_ID, kca_seq_id) IN (
      SELECT
        SCHEDULE_GROUP_ID,
        MAX(kca_seq_id)
      FROM bronze_bec_ods_stg.WIP_SCHEDULE_GROUPS
      WHERE
        kca_operation IN ('INSERT', 'UPDATE')
      GROUP BY
        SCHEDULE_GROUP_ID
    )
);
/* Soft delete */
UPDATE silver_bec_ods.WIP_SCHEDULE_GROUPS SET IS_DELETED_FLG = 'N';
UPDATE silver_bec_ods.WIP_SCHEDULE_GROUPS SET IS_DELETED_FLG = 'Y'
WHERE
  (
    SCHEDULE_GROUP_ID
  ) IN (
    SELECT
      SCHEDULE_GROUP_ID
    FROM bec_raw_dl_ext.WIP_SCHEDULE_GROUPS
    WHERE
      (SCHEDULE_GROUP_ID, KCA_SEQ_ID) IN (
        SELECT
          SCHEDULE_GROUP_ID,
          MAX(KCA_SEQ_ID) AS KCA_SEQ_ID
        FROM bec_raw_dl_ext.WIP_SCHEDULE_GROUPS
        GROUP BY
          SCHEDULE_GROUP_ID
      )
      AND kca_operation = 'DELETE'
  );
UPDATE bec_etl_ctrl.batch_ods_info SET last_refresh_date = CURRENT_TIMESTAMP()
WHERE
  ods_table_name = 'wip_schedule_groups';