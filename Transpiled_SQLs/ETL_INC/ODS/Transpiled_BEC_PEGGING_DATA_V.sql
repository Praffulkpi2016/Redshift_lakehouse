TRUNCATE table silver_bec_ods.BEC_PEGGING_DATA_V;
INSERT INTO silver_bec_ods.BEC_PEGGING_DATA_V (
  pegging_id,
  PREV_PEGGING_ID,
  PREV_ITEM_ID,
  prev_item_org,
  prev_item_desc,
  prev_pegged_qty,
  prev_order_type,
  kca_operation,
  is_deleted_flg,
  kca_seq_id,
  kca_seq_date
)
SELECT
  pegging_id,
  PREV_PEGGING_ID,
  PREV_ITEM_ID,
  prev_item_org,
  prev_item_desc,
  prev_pegged_qty,
  prev_order_type,
  kca_operation,
  'N' AS IS_DELETED_FLG,
  CAST(NULLIF(KCA_SEQ_ID, '') AS DECIMAL(36, 0)) AS KCA_SEQ_ID,
  kca_seq_date
FROM bronze_bec_ods_stg.BEC_PEGGING_DATA_V;
UPDATE bec_etl_ctrl.batch_ods_info SET load_type = 'I', last_refresh_date = CURRENT_TIMESTAMP()
WHERE
  ods_table_name = 'bec_pegging_data_v';