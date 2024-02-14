DROP TABLE IF EXISTS silver_bec_ods.MSC_FULL_PEGGING;
CREATE TABLE IF NOT EXISTS silver_bec_ods.MSC_FULL_PEGGING (
  pegging_id DECIMAL(15, 0),
  demand_quantity DECIMAL(28, 10),
  supply_quantity DECIMAL(28, 10),
  allocated_quantity DECIMAL(28, 10),
  end_item_usage DECIMAL(28, 10),
  demand_date TIMESTAMP,
  supply_date TIMESTAMP,
  supply_type DECIMAL(15, 0),
  end_origination_type DECIMAL(15, 0),
  inventory_item_id DECIMAL(15, 0),
  organization_id DECIMAL(15, 0),
  plan_id DECIMAL(15, 0),
  prev_pegging_id DECIMAL(15, 0),
  end_pegging_id DECIMAL(15, 0),
  transaction_id DECIMAL(15, 0),
  disposition_id DECIMAL(15, 0),
  demand_id DECIMAL(15, 0),
  project_id DECIMAL(15, 0),
  task_id DECIMAL(15, 0),
  sr_instance_id DECIMAL(15, 0),
  demand_class STRING,
  updated DECIMAL(15, 0),
  status DECIMAL(15, 0),
  last_update_date TIMESTAMP,
  last_updated_by DECIMAL(15, 0),
  creation_date TIMESTAMP,
  created_by DECIMAL(15, 0),
  last_update_login DECIMAL(15, 0),
  request_id DECIMAL(15, 0),
  program_application_id DECIMAL(15, 0),
  program_id DECIMAL(15, 0),
  program_update_date TIMESTAMP,
  attribute_category STRING,
  attribute1 STRING,
  attribute2 STRING,
  attribute3 STRING,
  attribute4 STRING,
  attribute5 STRING,
  attribute6 STRING,
  attribute7 STRING,
  attribute8 STRING,
  attribute9 STRING,
  attribute10 STRING,
  attribute11 STRING,
  attribute12 STRING,
  attribute13 STRING,
  attribute14 STRING,
  attribute15 STRING,
  unit_number STRING,
  dest_transaction_id DECIMAL(15, 0),
  reserved_qty DECIMAL(28, 10),
  KCA_OPERATION STRING,
  IS_DELETED_FLG STRING,
  kca_seq_id DECIMAL(36, 0),
  kca_seq_date TIMESTAMP
);
INSERT INTO silver_bec_ods.MSC_FULL_PEGGING (
  pegging_id,
  demand_quantity,
  supply_quantity,
  allocated_quantity,
  end_item_usage,
  demand_date,
  supply_date,
  supply_type,
  end_origination_type,
  inventory_item_id,
  organization_id,
  plan_id,
  prev_pegging_id,
  end_pegging_id,
  transaction_id,
  disposition_id,
  demand_id,
  project_id,
  task_id,
  sr_instance_id,
  demand_class,
  updated,
  status,
  last_update_date,
  last_updated_by,
  creation_date,
  created_by,
  last_update_login,
  request_id,
  program_application_id,
  program_id,
  program_update_date,
  attribute_category,
  attribute1,
  attribute2,
  attribute3,
  attribute4,
  attribute5,
  attribute6,
  attribute7,
  attribute8,
  attribute9,
  attribute10,
  attribute11,
  attribute12,
  attribute13,
  attribute14,
  attribute15,
  unit_number,
  dest_transaction_id,
  reserved_qty,
  KCA_OPERATION,
  IS_DELETED_FLG,
  kca_seq_id,
  kca_seq_date
)
SELECT
  pegging_id,
  demand_quantity,
  supply_quantity,
  allocated_quantity,
  end_item_usage,
  demand_date,
  supply_date,
  supply_type,
  end_origination_type,
  inventory_item_id,
  organization_id,
  plan_id,
  prev_pegging_id,
  end_pegging_id,
  transaction_id,
  disposition_id,
  demand_id,
  project_id,
  task_id,
  sr_instance_id,
  demand_class,
  updated,
  status,
  last_update_date,
  last_updated_by,
  creation_date,
  created_by,
  last_update_login,
  request_id,
  program_application_id,
  program_id,
  program_update_date,
  attribute_category,
  attribute1,
  attribute2,
  attribute3,
  attribute4,
  attribute5,
  attribute6,
  attribute7,
  attribute8,
  attribute9,
  attribute10,
  attribute11,
  attribute12,
  attribute13,
  attribute14,
  attribute15,
  unit_number,
  dest_transaction_id,
  reserved_qty,
  KCA_OPERATION,
  'N' AS IS_DELETED_FLG,
  CAST(NULLIF(KCA_SEQ_ID, '') AS DECIMAL(36, 0)) AS KCA_SEQ_ID,
  kca_seq_date
FROM bronze_bec_ods_stg.MSC_FULL_PEGGING;
UPDATE bec_etl_ctrl.batch_ods_info SET load_type = 'I', last_refresh_date = CURRENT_TIMESTAMP()
WHERE
  ods_table_name = 'msc_full_pegging';