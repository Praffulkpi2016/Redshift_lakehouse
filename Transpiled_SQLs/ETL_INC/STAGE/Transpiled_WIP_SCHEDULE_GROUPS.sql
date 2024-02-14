TRUNCATE table
	table bronze_bec_ods_stg.WIP_SCHEDULE_GROUPS;
INSERT INTO bronze_bec_ods_stg.WIP_SCHEDULE_GROUPS (
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
  KCA_OPERATION,
  kca_seq_id,
  KCA_SEQ_DATE
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
    KCA_OPERATION,
    kca_seq_id,
    KCA_SEQ_DATE
  FROM bec_raw_dl_ext.WIP_SCHEDULE_GROUPS
  WHERE
    kca_operation <> 'DELETE'
    AND (SCHEDULE_GROUP_ID, kca_seq_id) IN (
      SELECT
        SCHEDULE_GROUP_ID,
        MAX(kca_seq_id)
      FROM bec_raw_dl_ext.WIP_SCHEDULE_GROUPS
      WHERE
        kca_operation <> 'DELETE'
      GROUP BY
        SCHEDULE_GROUP_ID
    )
    AND (
      KCA_SEQ_DATE > (
        SELECT
          (
            executebegints - prune_days
          )
        FROM bec_etl_ctrl.batch_ods_info
        WHERE
          ods_table_name = 'wip_schedule_groups'
      )
    )
);