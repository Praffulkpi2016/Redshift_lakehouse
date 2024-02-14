DROP TABLE IF EXISTS silver_bec_ods.xxbec_pegging_data;
CREATE TABLE IF NOT EXISTS silver_bec_ods.xxbec_pegging_data (
  level_seq DECIMAL(15, 0),
  plan_id DECIMAL(15, 0),
  organization_id DECIMAL(15, 0),
  pegging_id DECIMAL(15, 0),
  prev_pegging_id DECIMAL(15, 0),
  parent_pegging_id DECIMAL(15, 0),
  demand_id DECIMAL(15, 0),
  transaction_id DECIMAL(15, 0),
  msc_trxn_id DECIMAL(15, 0),
  demand_qty DECIMAL(28, 10),
  demand_date TIMESTAMP,
  item_id DECIMAL(15, 0),
  item_org STRING,
  pegged_qty DECIMAL(28, 10),
  origination_name STRING,
  end_pegged_qty DECIMAL(28, 10),
  end_demand_qty DECIMAL(28, 10),
  end_demand_date TIMESTAMP,
  end_item_org STRING,
  end_origination_name STRING,
  end_satisfied_date TIMESTAMP,
  end_disposition STRING,
  order_type DECIMAL(15, 0),
  end_demand_class STRING,
  sr_instance_id DECIMAL(15, 0),
  op_seq_num DECIMAL(15, 0),
  item_desc_org STRING,
  new_order_date TIMESTAMP,
  new_dock_date TIMESTAMP,
  new_start_date TIMESTAMP,
  KCA_OPERATION STRING,
  IS_DELETED_FLG STRING,
  kca_seq_id DECIMAL(36, 0),
  kca_seq_date TIMESTAMP
);
INSERT INTO silver_bec_ods.xxbec_pegging_data (
  level_seq,
  plan_id,
  organization_id,
  pegging_id,
  prev_pegging_id,
  parent_pegging_id,
  demand_id,
  transaction_id,
  msc_trxn_id,
  demand_qty,
  demand_date,
  item_id,
  item_org,
  pegged_qty,
  origination_name,
  end_pegged_qty,
  end_demand_qty,
  end_demand_date,
  end_item_org,
  end_origination_name,
  end_satisfied_date,
  end_disposition,
  order_type,
  end_demand_class,
  sr_instance_id,
  op_seq_num,
  item_desc_org,
  new_order_date,
  new_dock_date,
  new_start_date,
  KCA_OPERATION,
  IS_DELETED_FLG,
  kca_seq_id,
  kca_seq_date
)
SELECT
  level_seq,
  plan_id,
  organization_id,
  pegging_id,
  prev_pegging_id,
  parent_pegging_id,
  demand_id,
  transaction_id,
  msc_trxn_id,
  demand_qty,
  demand_date,
  item_id,
  item_org,
  pegged_qty,
  origination_name,
  end_pegged_qty,
  end_demand_qty,
  end_demand_date,
  end_item_org,
  end_origination_name,
  end_satisfied_date,
  end_disposition,
  order_type,
  end_demand_class,
  sr_instance_id,
  op_seq_num,
  item_desc_org,
  new_order_date,
  new_dock_date,
  new_start_date,
  KCA_OPERATION,
  'N' AS IS_DELETED_FLG,
  CAST(NULLIF(KCA_SEQ_ID, '') AS DECIMAL(36, 0)) AS KCA_SEQ_ID,
  kca_seq_date
FROM bronze_bec_ods_stg.XXBEC_PEGGING_DATA;
UPDATE bec_etl_ctrl.batch_ods_info SET load_type = 'I', last_refresh_date = CURRENT_TIMESTAMP()
WHERE
  ods_table_name = 'xxbec_pegging_data';