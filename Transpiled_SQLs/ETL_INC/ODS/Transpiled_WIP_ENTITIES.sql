/* Delete Records */
DELETE FROM silver_bec_ods.WIP_ENTITIES
WHERE
  (
    WIP_ENTITY_ID
  ) IN (
    SELECT
      stg.WIP_ENTITY_ID
    FROM silver_bec_ods.WIP_ENTITIES AS ods, bronze_bec_ods_stg.WIP_ENTITIES AS stg
    WHERE
      ods.WIP_ENTITY_ID = stg.WIP_ENTITY_ID AND stg.kca_operation IN ('INSERT', 'UPDATE')
  );
/* Insert records */
INSERT INTO silver_bec_ods.WIP_ENTITIES (
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
  is_deleted_flg,
  kca_seq_id,
  kca_seq_date
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
    KCA_OPERATION,
    'N' AS IS_DELETED_FLG,
    CAST(NULLIF(KCA_SEQ_ID, '') AS DECIMAL(36, 0)) AS KCA_SEQ_ID,
    KCA_SEQ_DATE
  FROM bronze_bec_ods_stg.WIP_ENTITIES
  WHERE
    kca_operation IN ('INSERT', 'UPDATE')
    AND (WIP_ENTITY_ID, kca_seq_id) IN (
      SELECT
        WIP_ENTITY_ID,
        MAX(kca_seq_id)
      FROM bronze_bec_ods_stg.WIP_ENTITIES
      WHERE
        kca_operation IN ('INSERT', 'UPDATE')
      GROUP BY
        WIP_ENTITY_ID
    )
);
/* Soft delete */
UPDATE silver_bec_ods.WIP_ENTITIES SET IS_DELETED_FLG = 'N';
UPDATE silver_bec_ods.WIP_ENTITIES SET IS_DELETED_FLG = 'Y'
WHERE
  (
    WIP_ENTITY_ID
  ) IN (
    SELECT
      WIP_ENTITY_ID
    FROM bec_raw_dl_ext.WIP_ENTITIES
    WHERE
      (WIP_ENTITY_ID, KCA_SEQ_ID) IN (
        SELECT
          WIP_ENTITY_ID,
          MAX(KCA_SEQ_ID) AS KCA_SEQ_ID
        FROM bec_raw_dl_ext.WIP_ENTITIES
        GROUP BY
          WIP_ENTITY_ID
      )
      AND kca_operation = 'DELETE'
  );
UPDATE bec_etl_ctrl.batch_ods_info SET last_refresh_date = CURRENT_TIMESTAMP()
WHERE
  ods_table_name = 'wip_entities';