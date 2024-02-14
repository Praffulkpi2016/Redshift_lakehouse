TRUNCATE table bronze_bec_ods_stg.WIP_ENTITIES;
INSERT INTO bronze_bec_ods_stg.WIP_ENTITIES (
  wip_entity_id,
  organization_id,
  last_update_date,
  last_updated_by,
  creation_date,
  created_by,
  last_update_login,
  request_id,
  program_application_id,
  program_id,
  program_update_date,
  wip_entity_name,
  entity_type,
  description,
  primary_item_id,
  gen_object_id,
  kca_operation,
  kca_seq_id,
  KCA_SEQ_DATE
)
(
  SELECT
    wip_entity_id,
    organization_id,
    last_update_date,
    last_updated_by,
    creation_date,
    created_by,
    last_update_login,
    request_id,
    program_application_id,
    program_id,
    program_update_date,
    wip_entity_name,
    entity_type,
    description,
    primary_item_id,
    gen_object_id,
    kca_operation,
    kca_seq_id,
    KCA_SEQ_DATE
  FROM bec_raw_dl_ext.WIP_ENTITIES
  WHERE
    kca_operation <> 'DELETE'
    AND COALESCE(kca_seq_id, '') <> ''
    AND (WIP_ENTITY_ID, kca_seq_id) IN (
      SELECT
        WIP_ENTITY_ID,
        MAX(kca_seq_id)
      FROM bec_raw_dl_ext.WIP_ENTITIES
      WHERE
        kca_operation <> 'DELETE' AND COALESCE(kca_seq_id, '') <> ''
      GROUP BY
        WIP_ENTITY_ID
    )
    AND (
      KCA_SEQ_DATE > (
        SELECT
          (
            executebegints - prune_days
          )
        FROM bec_etl_ctrl.batch_ods_info
        WHERE
          ods_table_name = 'wip_entities'
      )
    )
);