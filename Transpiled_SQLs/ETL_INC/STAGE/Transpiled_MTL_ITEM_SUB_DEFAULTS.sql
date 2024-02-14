TRUNCATE table bronze_bec_ods_stg.MTL_ITEM_SUB_DEFAULTS;
INSERT INTO bronze_bec_ods_stg.MTL_ITEM_SUB_DEFAULTS (
  inventory_item_id,
  organization_id,
  subinventory_code,
  default_type,
  last_update_date,
  last_updated_by,
  creation_date,
  created_by,
  last_update_login,
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
    organization_id,
    subinventory_code,
    default_type,
    last_update_date,
    last_updated_by,
    creation_date,
    created_by,
    last_update_login,
    request_id,
    program_application_id,
    program_id,
    program_update_date,
    KCA_OPERATION,
    kca_seq_id,
    kca_seq_date
  FROM bec_raw_dl_ext.MTL_ITEM_SUB_DEFAULTS
  WHERE
    kca_operation <> 'DELETE'
    AND COALESCE(kca_seq_id, '') <> ''
    AND (COALESCE(INVENTORY_ITEM_ID, 0), COALESCE(ORGANIZATION_ID, 0), COALESCE(SUBINVENTORY_CODE, 'NA'), COALESCE(DEFAULT_TYPE, 0), KCA_SEQ_ID) IN (
      SELECT
        COALESCE(INVENTORY_ITEM_ID, 0) AS INVENTORY_ITEM_ID,
        COALESCE(ORGANIZATION_ID, 0) AS ORGANIZATION_ID,
        COALESCE(SUBINVENTORY_CODE, 'NA') AS SUBINVENTORY_CODE,
        COALESCE(DEFAULT_TYPE, 0) AS DEFAULT_TYPE,
        MAX(KCA_SEQ_ID)
      FROM bec_raw_dl_ext.MTL_ITEM_SUB_DEFAULTS
      WHERE
        kca_operation <> 'DELETE' AND COALESCE(kca_seq_id, '') <> ''
      GROUP BY
        COALESCE(INVENTORY_ITEM_ID, 0),
        COALESCE(ORGANIZATION_ID, 0),
        COALESCE(SUBINVENTORY_CODE, 'NA'),
        COALESCE(DEFAULT_TYPE, 0)
    )
    AND kca_seq_date > (
      SELECT
        (
          executebegints - prune_days
        )
      FROM bec_etl_ctrl.batch_ods_info
      WHERE
        ods_table_name = 'mtl_item_sub_defaults'
    )
);