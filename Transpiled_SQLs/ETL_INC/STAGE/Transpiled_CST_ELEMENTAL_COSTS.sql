TRUNCATE table bronze_bec_ods_stg.CST_ELEMENTAL_COSTS;
INSERT INTO bronze_bec_ods_stg.CST_ELEMENTAL_COSTS (
  COST_UPDATE_ID,
  ORGANIZATION_ID,
  INVENTORY_ITEM_ID,
  COST_ELEMENT_ID,
  LAST_UPDATE_DATE,
  LAST_UPDATED_BY,
  CREATION_DATE,
  CREATED_BY,
  LAST_UPDATE_LOGIN,
  STANDARD_COST,
  REQUEST_ID,
  PROGRAM_APPLICATION_ID,
  PROGRAM_ID,
  PROGRAM_UPDATE_DATE,
  KCA_OPERATION,
  kca_seq_id,
  kca_seq_date
)
(
  SELECT
    COST_UPDATE_ID,
    ORGANIZATION_ID,
    INVENTORY_ITEM_ID,
    COST_ELEMENT_ID,
    LAST_UPDATE_DATE,
    LAST_UPDATED_BY,
    CREATION_DATE,
    CREATED_BY,
    LAST_UPDATE_LOGIN,
    STANDARD_COST,
    REQUEST_ID,
    PROGRAM_APPLICATION_ID,
    PROGRAM_ID,
    PROGRAM_UPDATE_DATE,
    KCA_OPERATION,
    kca_seq_id,
    kca_seq_date
  FROM bec_raw_dl_ext.CST_ELEMENTAL_COSTS
  WHERE
    kca_operation <> 'DELETE'
    AND COALESCE(kca_seq_id, '') <> ''
    AND (COST_UPDATE_ID, ORGANIZATION_ID, INVENTORY_ITEM_ID, COST_ELEMENT_ID, kca_seq_id) IN (
      SELECT
        COST_UPDATE_ID,
        ORGANIZATION_ID,
        INVENTORY_ITEM_ID,
        COST_ELEMENT_ID,
        MAX(kca_seq_id)
      FROM bec_raw_dl_ext.CST_ELEMENTAL_COSTS
      WHERE
        kca_operation <> 'DELETE' AND COALESCE(kca_seq_id, '') <> ''
      GROUP BY
        COST_UPDATE_ID,
        ORGANIZATION_ID,
        INVENTORY_ITEM_ID,
        COST_ELEMENT_ID
    )
    AND kca_seq_date > (
      SELECT
        (
          executebegints - prune_days
        )
      FROM bec_etl_ctrl.batch_ods_info
      WHERE
        ods_table_name = 'cst_elemental_costs'
    )
);