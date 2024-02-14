/* Delete Records */
DELETE FROM silver_bec_ods.MTL_ONHAND_QUANTITIES_DETAIL
WHERE
  (
    COALESCE(ONHAND_QUANTITIES_ID, 0)
  ) IN (
    SELECT
      COALESCE(stg.ONHAND_QUANTITIES_ID, 0) AS ONHAND_QUANTITIES_ID
    FROM silver_bec_ods.MTL_ONHAND_QUANTITIES_DETAIL AS ods, bronze_bec_ods_stg.MTL_ONHAND_QUANTITIES_DETAIL AS stg
    WHERE
      COALESCE(ods.ONHAND_QUANTITIES_ID, 0) = COALESCE(stg.ONHAND_QUANTITIES_ID, 0)
      AND stg.kca_operation IN ('INSERT', 'UPDATE')
  );
/* Insert records */
INSERT INTO silver_bec_ods.MTL_ONHAND_QUANTITIES_DETAIL (
  inventory_item_id,
  organization_id,
  date_received,
  last_update_date,
  last_updated_by,
  creation_date,
  created_by,
  last_update_login,
  primary_transaction_quantity,
  subinventory_code,
  revision,
  locator_id,
  create_transaction_id,
  update_transaction_id,
  lot_number,
  orig_date_received,
  cost_group_id,
  containerized_flag,
  project_id,
  task_id,
  onhand_quantities_id,
  organization_type,
  owning_organization_id,
  owning_tp_type,
  planning_organization_id,
  planning_tp_type,
  transaction_uom_code,
  transaction_quantity,
  secondary_uom_code,
  secondary_transaction_quantity,
  is_consigned,
  lpn_id,
  status_id,
  mcc_code,
  kca_operation,
  IS_DELETED_FLG,
  kca_seq_id,
  kca_seq_date
)
(
  SELECT
    inventory_item_id,
    organization_id,
    date_received,
    last_update_date,
    last_updated_by,
    creation_date,
    created_by,
    last_update_login,
    primary_transaction_quantity,
    subinventory_code,
    revision,
    locator_id,
    create_transaction_id,
    update_transaction_id,
    lot_number,
    orig_date_received,
    cost_group_id,
    containerized_flag,
    project_id,
    task_id,
    onhand_quantities_id,
    organization_type,
    owning_organization_id,
    owning_tp_type,
    planning_organization_id,
    planning_tp_type,
    transaction_uom_code,
    transaction_quantity,
    secondary_uom_code,
    secondary_transaction_quantity,
    is_consigned,
    lpn_id,
    status_id,
    mcc_code,
    kca_operation,
    'N' AS IS_DELETED_FLG,
    CAST(NULLIF(KCA_SEQ_ID, '') AS DECIMAL(36, 0)) AS KCA_SEQ_ID,
    kca_seq_date
  FROM bronze_bec_ods_stg.MTL_ONHAND_QUANTITIES_DETAIL
  WHERE
    kca_operation IN ('INSERT', 'UPDATE')
    AND (COALESCE(ONHAND_QUANTITIES_ID, 0), KCA_SEQ_ID) IN (
      SELECT
        COALESCE(ONHAND_QUANTITIES_ID, 0) AS ONHAND_QUANTITIES_ID,
        MAX(KCA_SEQ_ID)
      FROM bronze_bec_ods_stg.MTL_ONHAND_QUANTITIES_DETAIL
      WHERE
        kca_operation IN ('INSERT', 'UPDATE')
      GROUP BY
        COALESCE(ONHAND_QUANTITIES_ID, 0)
    )
);
/* Soft delete */
UPDATE silver_bec_ods.MTL_ONHAND_QUANTITIES_DETAIL SET IS_DELETED_FLG = 'N';
UPDATE silver_bec_ods.MTL_ONHAND_QUANTITIES_DETAIL SET IS_DELETED_FLG = 'Y', kca_seq_date = CURRENT_TIMESTAMP()
WHERE
  (
    ONHAND_QUANTITIES_ID
  ) IN (
    SELECT
      ONHAND_QUANTITIES_ID
    FROM bec_raw_dl_ext.MTL_ONHAND_QUANTITIES_DETAIL
    WHERE
      (ONHAND_QUANTITIES_ID, KCA_SEQ_ID) IN (
        SELECT
          ONHAND_QUANTITIES_ID,
          MAX(KCA_SEQ_ID) AS KCA_SEQ_ID
        FROM bec_raw_dl_ext.MTL_ONHAND_QUANTITIES_DETAIL
        GROUP BY
          ONHAND_QUANTITIES_ID
      )
      AND kca_operation = 'DELETE'
  );
UPDATE bec_etl_ctrl.batch_ods_info SET last_refresh_date = CURRENT_TIMESTAMP()
WHERE
  ods_table_name = 'mtl_onhand_quantities_detail';