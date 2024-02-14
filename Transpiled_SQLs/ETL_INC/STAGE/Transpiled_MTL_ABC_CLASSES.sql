TRUNCATE table bronze_bec_ods_stg.MTL_ABC_CLASSES;
INSERT INTO bronze_bec_ods_stg.MTL_ABC_CLASSES (
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
  KCA_OPERATION,
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
    KCA_OPERATION,
    kca_seq_id,
    kca_seq_date
  FROM bec_raw_dl_ext.MTL_ABC_CLASSES
  WHERE
    kca_operation <> 'DELETE'
    AND COALESCE(kca_seq_id, '') <> ''
    AND (COALESCE(ABC_CLASS_ID, 0), KCA_SEQ_ID) IN (
      SELECT
        COALESCE(ABC_CLASS_ID, 0) AS ABC_CLASS_ID,
        MAX(KCA_SEQ_ID)
      FROM bec_raw_dl_ext.MTL_ABC_CLASSES
      WHERE
        kca_operation <> 'DELETE' AND COALESCE(kca_seq_id, '') <> ''
      GROUP BY
        COALESCE(ABC_CLASS_ID, 0)
    )
    AND kca_seq_date > (
      SELECT
        (
          executebegints - prune_days
        )
      FROM bec_etl_ctrl.batch_ods_info
      WHERE
        ods_table_name = 'mtl_abc_classes'
    )
);