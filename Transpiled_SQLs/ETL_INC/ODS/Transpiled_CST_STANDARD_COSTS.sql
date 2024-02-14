/* Delete Records */
DELETE FROM silver_bec_ods.CST_STANDARD_COSTS
WHERE
  (COST_UPDATE_ID, INVENTORY_ITEM_ID, ORGANIZATION_ID) IN (
    SELECT
      stg.COST_UPDATE_ID,
      stg.INVENTORY_ITEM_ID,
      stg.ORGANIZATION_ID
    FROM silver_bec_ods.CST_STANDARD_COSTS AS ods, bronze_bec_ods_stg.CST_STANDARD_COSTS AS stg
    WHERE
      ods.COST_UPDATE_ID = stg.COST_UPDATE_ID
      AND ods.INVENTORY_ITEM_ID = stg.INVENTORY_ITEM_ID
      AND ods.ORGANIZATION_ID = stg.ORGANIZATION_ID
      AND stg.kca_operation IN ('INSERT', 'UPDATE')
  );
/* Insert records */
INSERT INTO silver_bec_ods.CST_STANDARD_COSTS (
  cost_update_id,
  inventory_item_id,
  organization_id,
  last_update_date,
  last_updated_by,
  creation_date,
  created_by,
  last_update_login,
  standard_cost_revision_date,
  standard_cost,
  inventory_adjustment_quantity,
  inventory_adjustment_value,
  intransit_adjustment_quantity,
  intransit_adjustment_value,
  wip_adjustment_quantity,
  wip_adjustment_value,
  last_cost_update_id,
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
    cost_update_id,
    inventory_item_id,
    organization_id,
    last_update_date,
    last_updated_by,
    creation_date,
    created_by,
    last_update_login,
    standard_cost_revision_date,
    standard_cost,
    inventory_adjustment_quantity,
    inventory_adjustment_value,
    intransit_adjustment_quantity,
    intransit_adjustment_value,
    wip_adjustment_quantity,
    wip_adjustment_value,
    last_cost_update_id,
    request_id,
    program_application_id,
    program_id,
    program_update_date,
    KCA_OPERATION,
    'N' AS IS_DELETED_FLG,
    CAST(NULLIF(KCA_SEQ_ID, '') AS DECIMAL(36, 0)) AS KCA_SEQ_ID,
    kca_seq_date
  FROM bronze_bec_ods_stg.CST_STANDARD_COSTS
  WHERE
    kca_operation IN ('INSERT', 'UPDATE')
    AND (COST_UPDATE_ID, INVENTORY_ITEM_ID, ORGANIZATION_ID, kca_seq_id) IN (
      SELECT
        COST_UPDATE_ID,
        INVENTORY_ITEM_ID,
        ORGANIZATION_ID,
        MAX(kca_seq_id)
      FROM bronze_bec_ods_stg.CST_STANDARD_COSTS
      WHERE
        kca_operation IN ('INSERT', 'UPDATE')
      GROUP BY
        COST_UPDATE_ID,
        INVENTORY_ITEM_ID,
        ORGANIZATION_ID
    )
);
/* Soft delete */
UPDATE silver_bec_ods.CST_STANDARD_COSTS SET IS_DELETED_FLG = 'N';
UPDATE silver_bec_ods.CST_STANDARD_COSTS SET IS_DELETED_FLG = 'Y'
WHERE
  (COST_UPDATE_ID, INVENTORY_ITEM_ID, ORGANIZATION_ID) IN (
    SELECT
      COST_UPDATE_ID,
      INVENTORY_ITEM_ID,
      ORGANIZATION_ID
    FROM bec_raw_dl_ext.CST_STANDARD_COSTS
    WHERE
      (COST_UPDATE_ID, INVENTORY_ITEM_ID, ORGANIZATION_ID, KCA_SEQ_ID) IN (
        SELECT
          COST_UPDATE_ID,
          INVENTORY_ITEM_ID,
          ORGANIZATION_ID,
          MAX(KCA_SEQ_ID) AS KCA_SEQ_ID
        FROM bec_raw_dl_ext.CST_STANDARD_COSTS
        GROUP BY
          COST_UPDATE_ID,
          INVENTORY_ITEM_ID,
          ORGANIZATION_ID
      )
      AND kca_operation = 'DELETE'
  );
UPDATE bec_etl_ctrl.batch_ods_info SET last_refresh_date = CURRENT_TIMESTAMP()
WHERE
  ods_table_name = 'cst_standard_costs';