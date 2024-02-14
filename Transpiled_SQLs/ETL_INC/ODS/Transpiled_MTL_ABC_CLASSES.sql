/* Delete Records */
DELETE FROM silver_bec_ods.MTL_ABC_CLASSES
WHERE
  (
    COALESCE(ABC_CLASS_ID, 0)
  ) IN (
    SELECT
      COALESCE(stg.ABC_CLASS_ID, 0) AS ABC_CLASS_ID
    FROM silver_bec_ods.mtl_abc_classes AS ods, bronze_bec_ods_stg.mtl_abc_classes AS stg
    WHERE
      COALESCE(ods.ABC_CLASS_ID, 0) = COALESCE(stg.ABC_CLASS_ID, 0)
      AND stg.kca_operation IN ('INSERT', 'UPDATE')
  );
/* Insert records */
INSERT INTO silver_bec_ods.mtl_abc_classes (
  abc_class_id,
  abc_class_name,
  organization_id,
  last_update_date,
  last_updated_by,
  creation_date,
  created_by,
  description,
  disable_date,
  request_id,
  program_application_id,
  program_id,
  program_update_date,
  kca_operation,
  IS_DELETED_FLG,
  kca_seq_id,
  kca_seq_date
)
(
  SELECT
    abc_class_id,
    abc_class_name,
    organization_id,
    last_update_date,
    last_updated_by,
    creation_date,
    created_by,
    description,
    disable_date,
    request_id,
    program_application_id,
    program_id,
    program_update_date,
    kca_operation,
    'N' AS IS_DELETED_FLG,
    CAST(NULLIF(KCA_SEQ_ID, '') AS DECIMAL(36, 0)) AS KCA_SEQ_ID,
    kca_seq_date
  FROM bronze_bec_ods_stg.mtl_abc_classes
  WHERE
    kca_operation IN ('INSERT', 'UPDATE')
    AND (COALESCE(ABC_CLASS_ID, 0), KCA_SEQ_ID) IN (
      SELECT
        COALESCE(ABC_CLASS_ID, 0) AS ABC_CLASS_ID,
        MAX(KCA_SEQ_ID)
      FROM bronze_bec_ods_stg.mtl_abc_classes
      WHERE
        kca_operation IN ('INSERT', 'UPDATE')
      GROUP BY
        COALESCE(ABC_CLASS_ID, 0)
    )
);
/* Soft delete */
UPDATE silver_bec_ods.mtl_abc_classes SET IS_DELETED_FLG = 'N';
UPDATE silver_bec_ods.mtl_abc_classes SET IS_DELETED_FLG = 'Y'
WHERE
  (
    ABC_CLASS_ID
  ) IN (
    SELECT
      ABC_CLASS_ID
    FROM bec_raw_dl_ext.mtl_abc_classes
    WHERE
      (ABC_CLASS_ID, KCA_SEQ_ID) IN (
        SELECT
          ABC_CLASS_ID,
          MAX(KCA_SEQ_ID) AS KCA_SEQ_ID
        FROM bec_raw_dl_ext.mtl_abc_classes
        GROUP BY
          ABC_CLASS_ID
      )
      AND kca_operation = 'DELETE'
  );
UPDATE bec_etl_ctrl.batch_ods_info SET last_refresh_date = CURRENT_TIMESTAMP()
WHERE
  ods_table_name = 'mtl_abc_classes';