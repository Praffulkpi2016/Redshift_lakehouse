/* Delete Records */
DELETE FROM silver_bec_ods.MTL_ABC_ASSIGNMENTS
WHERE
  (COALESCE(INVENTORY_ITEM_ID, 0), COALESCE(ASSIGNMENT_GROUP_ID, 0), COALESCE(ABC_CLASS_ID, 0)) IN (
    SELECT
      COALESCE(stg.INVENTORY_ITEM_ID, 0) AS INVENTORY_ITEM_ID,
      COALESCE(stg.ASSIGNMENT_GROUP_ID, 0) AS ASSIGNMENT_GROUP_ID,
      COALESCE(stg.ABC_CLASS_ID, 0) AS ABC_CLASS_ID
    FROM silver_bec_ods.MTL_ABC_ASSIGNMENTS AS ods, bronze_bec_ods_stg.MTL_ABC_ASSIGNMENTS AS stg
    WHERE
      COALESCE(ods.INVENTORY_ITEM_ID, 0) = COALESCE(stg.INVENTORY_ITEM_ID, 0)
      AND COALESCE(ods.ASSIGNMENT_GROUP_ID, 0) = COALESCE(stg.ASSIGNMENT_GROUP_ID, 0)
      AND COALESCE(ods.ABC_CLASS_ID, 0) = COALESCE(stg.ABC_CLASS_ID, 0)
      AND stg.kca_operation IN ('INSERT', 'UPDATE')
  );
/* Insert records */
INSERT INTO silver_bec_ods.MTL_ABC_ASSIGNMENTS (
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
  IS_DELETED_FLG,
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
    'N' AS IS_DELETED_FLG,
    CAST(NULLIF(KCA_SEQ_ID, '') AS DECIMAL(36, 0)) AS KCA_SEQ_ID,
    kca_seq_date
  FROM bronze_bec_ods_stg.MTL_ABC_ASSIGNMENTS
  WHERE
    kca_operation IN ('INSERT', 'UPDATE')
    AND (COALESCE(INVENTORY_ITEM_ID, 0), COALESCE(ASSIGNMENT_GROUP_ID, 0), COALESCE(ABC_CLASS_ID, 0), kca_seq_id) IN (
      SELECT
        COALESCE(INVENTORY_ITEM_ID, 0) AS INVENTORY_ITEM_ID,
        COALESCE(ASSIGNMENT_GROUP_ID, 0) AS ASSIGNMENT_GROUP_ID,
        COALESCE(ABC_CLASS_ID, 0) AS ABC_CLASS_ID,
        MAX(kca_seq_id)
      FROM bronze_bec_ods_stg.MTL_ABC_ASSIGNMENTS
      WHERE
        kca_operation IN ('INSERT', 'UPDATE')
      GROUP BY
        COALESCE(INVENTORY_ITEM_ID, 0),
        COALESCE(ASSIGNMENT_GROUP_ID, 0),
        COALESCE(ABC_CLASS_ID, 0)
    )
);
/* Soft delete */
UPDATE silver_bec_ods.MTL_ABC_ASSIGNMENTS SET IS_DELETED_FLG = 'N';
UPDATE silver_bec_ods.MTL_ABC_ASSIGNMENTS SET IS_DELETED_FLG = 'Y'
WHERE
  (COALESCE(INVENTORY_ITEM_ID, 0), COALESCE(ASSIGNMENT_GROUP_ID, 0), COALESCE(ABC_CLASS_ID, 0)) IN (
    SELECT
      COALESCE(INVENTORY_ITEM_ID, 0),
      COALESCE(ASSIGNMENT_GROUP_ID, 0),
      COALESCE(ABC_CLASS_ID, 0)
    FROM bec_raw_dl_ext.MTL_ABC_ASSIGNMENTS
    WHERE
      (COALESCE(INVENTORY_ITEM_ID, 0), COALESCE(ASSIGNMENT_GROUP_ID, 0), COALESCE(ABC_CLASS_ID, 0), KCA_SEQ_ID) IN (
        SELECT
          COALESCE(INVENTORY_ITEM_ID, 0),
          COALESCE(ASSIGNMENT_GROUP_ID, 0),
          COALESCE(ABC_CLASS_ID, 0),
          MAX(KCA_SEQ_ID) AS KCA_SEQ_ID
        FROM bec_raw_dl_ext.MTL_ABC_ASSIGNMENTS
        GROUP BY
          COALESCE(INVENTORY_ITEM_ID, 0),
          COALESCE(ASSIGNMENT_GROUP_ID, 0),
          COALESCE(ABC_CLASS_ID, 0)
      )
      AND kca_operation = 'DELETE'
  );
UPDATE bec_etl_ctrl.batch_ods_info SET last_refresh_date = CURRENT_TIMESTAMP()
WHERE
  ods_table_name = 'mtl_abc_assignments';