TRUNCATE table silver_bec_ods.MTL_ONHAND_LOCATOR_V;
INSERT INTO silver_bec_ods.MTL_ONHAND_LOCATOR_V (
  organization_id,
  inventory_item_id,
  padded_concatenated_segments,
  revision,
  total_qoh,
  subinventory_code,
  locator_id,
  item_description,
  primary_uom_code,
  organization_code,
  organization_name,
  net,
  rsv,
  atp,
  locator_type,
  item_lot_control,
  item_locator_control,
  item_serial_control,
  kca_operation,
  is_deleted_flg,
  kca_seq_id,
  kca_seq_date
)
SELECT
  organization_id,
  inventory_item_id,
  padded_concatenated_segments,
  revision,
  total_qoh,
  subinventory_code,
  locator_id,
  item_description,
  primary_uom_code,
  organization_code,
  organization_name,
  net,
  rsv,
  atp,
  locator_type,
  item_lot_control,
  item_locator_control,
  item_serial_control,
  kca_operation,
  'N' AS IS_DELETED_FLG,
  CAST(NULLIF(KCA_SEQ_ID, '') AS DECIMAL(36, 0)) AS KCA_SEQ_ID,
  kca_seq_date
FROM bronze_bec_ods_stg.MTL_ONHAND_LOCATOR_V;
UPDATE bec_etl_ctrl.batch_ods_info SET load_type = 'I', last_refresh_date = CURRENT_TIMESTAMP()
WHERE
  ods_table_name = 'mtl_onhand_locator_v';