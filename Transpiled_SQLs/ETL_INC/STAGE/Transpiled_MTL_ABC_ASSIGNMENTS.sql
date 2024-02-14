TRUNCATE table bronze_bec_ods_stg.MTL_ABC_ASSIGNMENTS;
INSERT INTO bronze_bec_ods_stg.MTL_ABC_ASSIGNMENTS (
  inventory_item_id,
  assignment_group_id,
  abc_class_id,
  last_update_date,
  last_updated_by,
  creation_date,
  created_by,
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
    inventory_item_id,
    assignment_group_id,
    abc_class_id,
    last_update_date,
    last_updated_by,
    creation_date,
    created_by,
    request_id,
    program_application_id,
    program_id,
    program_update_date,
    KCA_OPERATION,
    kca_seq_id,
    kca_seq_date
  FROM bec_raw_dl_ext.MTL_ABC_ASSIGNMENTS
  WHERE
    kca_operation <> 'DELETE'
    AND COALESCE(kca_seq_id, '') <> ''
    AND (COALESCE(INVENTORY_ITEM_ID, 0), COALESCE(ASSIGNMENT_GROUP_ID, 0), COALESCE(ABC_CLASS_ID, 0), kca_seq_id) IN (
      SELECT
        COALESCE(INVENTORY_ITEM_ID, 0) AS INVENTORY_ITEM_ID,
        COALESCE(ASSIGNMENT_GROUP_ID, 0) AS ASSIGNMENT_GROUP_ID,
        COALESCE(ABC_CLASS_ID, 0) AS ABC_CLASS_ID,
        MAX(kca_seq_id)
      FROM bec_raw_dl_ext.MTL_ABC_ASSIGNMENTS
      WHERE
        kca_operation <> 'DELETE' AND COALESCE(kca_seq_id, '') <> ''
      GROUP BY
        COALESCE(INVENTORY_ITEM_ID, 0),
        COALESCE(ASSIGNMENT_GROUP_ID, 0),
        COALESCE(ABC_CLASS_ID, 0)
    )
    AND kca_seq_date > (
      SELECT
        (
          executebegints - prune_days
        )
      FROM bec_etl_ctrl.batch_ods_info
      WHERE
        ods_table_name = 'mtl_abc_assignments'
    )
);