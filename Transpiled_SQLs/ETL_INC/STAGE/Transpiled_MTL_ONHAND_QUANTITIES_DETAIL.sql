TRUNCATE table
	table bronze_bec_ods_stg.MTL_ONHAND_QUANTITIES_DETAIL;
INSERT INTO bronze_bec_ods_stg.MTL_ONHAND_QUANTITIES_DETAIL (
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
  KCA_OPERATION,
  KCA_SEQ_ID,
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
    KCA_OPERATION,
    KCA_SEQ_ID,
    kca_seq_date
  FROM bec_raw_dl_ext.MTL_ONHAND_QUANTITIES_DETAIL
  WHERE
    kca_operation <> 'DELETE'
    AND COALESCE(kca_seq_id, '') <> ''
    AND (COALESCE(ONHAND_QUANTITIES_ID, 0), KCA_SEQ_ID) IN (
      SELECT
        COALESCE(ONHAND_QUANTITIES_ID, 0) AS ONHAND_QUANTITIES_ID,
        MAX(KCA_SEQ_ID)
      FROM bec_raw_dl_ext.MTL_ONHAND_QUANTITIES_DETAIL
      WHERE
        kca_operation <> 'DELETE' AND COALESCE(kca_seq_id, '') <> ''
      GROUP BY
        (
          COALESCE(ONHAND_QUANTITIES_ID, 0)
        )
    )
    AND (
      kca_seq_date > (
        SELECT
          (
            executebegints - prune_days
          )
        FROM bec_etl_ctrl.batch_ods_info
        WHERE
          ods_table_name = 'mtl_onhand_quantities_detail'
      )
    )
);