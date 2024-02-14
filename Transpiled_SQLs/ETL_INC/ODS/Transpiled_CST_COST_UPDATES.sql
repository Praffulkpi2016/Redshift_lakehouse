/* Delete Records */
DELETE FROM silver_bec_ods.CST_COST_UPDATES
WHERE
  (
    COALESCE(COST_UPDATE_ID, 0)
  ) IN (
    SELECT
      COALESCE(stg.COST_UPDATE_ID, 0) AS COST_UPDATE_ID
    FROM silver_bec_ods.CST_COST_UPDATES AS ods, bronze_bec_ods_stg.CST_COST_UPDATES AS stg
    WHERE
      COALESCE(ods.COST_UPDATE_ID, 0) = COALESCE(stg.COST_UPDATE_ID, 0)
      AND stg.kca_operation IN ('INSERT', 'UPDATE')
  );
/* Insert records */
INSERT INTO silver_bec_ods.CST_COST_UPDATES (
  cost_update_id,
  last_update_date,
  last_updated_by,
  creation_date,
  created_by,
  last_update_login,
  status,
  organization_id,
  cost_type_id,
  update_date,
  description,
  range_option,
  update_resource_ovhd_flag,
  update_activity_flag,
  snapshot_saved_flag,
  inv_adjustment_account,
  single_item,
  item_range_low,
  item_range_high,
  category_id,
  category_set_id,
  inventory_adjustment_value,
  intransit_adjustment_value,
  wip_adjustment_value,
  scrap_adjustment_value,
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
    last_update_date,
    last_updated_by,
    creation_date,
    created_by,
    last_update_login,
    status,
    organization_id,
    cost_type_id,
    update_date,
    description,
    range_option,
    update_resource_ovhd_flag,
    update_activity_flag,
    snapshot_saved_flag,
    inv_adjustment_account,
    single_item,
    item_range_low,
    item_range_high,
    category_id,
    category_set_id,
    inventory_adjustment_value,
    intransit_adjustment_value,
    wip_adjustment_value,
    scrap_adjustment_value,
    request_id,
    program_application_id,
    program_id,
    program_update_date,
    kca_operation,
    'N' AS IS_DELETED_FLG,
    CAST(NULLIF(KCA_SEQ_ID, '') AS DECIMAL(36, 0)) AS KCA_SEQ_ID,
    kca_seq_date
  FROM bronze_bec_ods_stg.CST_COST_UPDATES
  WHERE
    kca_operation IN ('INSERT', 'UPDATE')
    AND (COALESCE(COST_UPDATE_ID, 0), KCA_SEQ_ID) IN (
      SELECT
        COALESCE(COST_UPDATE_ID, 0) AS COST_UPDATE_ID,
        MAX(KCA_SEQ_ID)
      FROM bronze_bec_ods_stg.CST_COST_UPDATES
      WHERE
        kca_operation IN ('INSERT', 'UPDATE')
      GROUP BY
        COALESCE(COST_UPDATE_ID, 0)
    )
);
/* Soft delete */
UPDATE silver_bec_ods.CST_COST_UPDATES SET IS_DELETED_FLG = 'N';
UPDATE silver_bec_ods.CST_COST_UPDATES SET IS_DELETED_FLG = 'Y'
WHERE
  (
    COST_UPDATE_ID
  ) IN (
    SELECT
      COST_UPDATE_ID
    FROM bec_raw_dl_ext.CST_COST_UPDATES
    WHERE
      (COST_UPDATE_ID, KCA_SEQ_ID) IN (
        SELECT
          COST_UPDATE_ID,
          MAX(KCA_SEQ_ID) AS KCA_SEQ_ID
        FROM bec_raw_dl_ext.CST_COST_UPDATES
        GROUP BY
          COST_UPDATE_ID
      )
      AND kca_operation = 'DELETE'
  );
UPDATE bec_etl_ctrl.batch_ods_info SET last_refresh_date = CURRENT_TIMESTAMP()
WHERE
  ods_table_name = 'cst_cost_updates';