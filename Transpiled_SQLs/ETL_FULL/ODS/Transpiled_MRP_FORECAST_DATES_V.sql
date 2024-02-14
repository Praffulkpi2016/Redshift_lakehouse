DROP TABLE IF EXISTS silver_bec_ods.MRP_FORECAST_DATES_V;
CREATE TABLE IF NOT EXISTS silver_bec_ods.MRP_FORECAST_DATES_V (
  transaction_id DECIMAL(15, 0),
  last_update_date TIMESTAMP,
  last_updated_by DECIMAL(15, 0),
  creation_date TIMESTAMP,
  created_by DECIMAL(15, 0),
  last_update_login DECIMAL(15, 0),
  inventory_item_id DECIMAL(15, 0),
  organization_id DECIMAL(15, 0),
  forecast_designator STRING,
  forecast_date TIMESTAMP,
  original_forecast_quantity DECIMAL(28, 10),
  current_forecast_quantity DECIMAL(28, 10),
  confidence_percentage DECIMAL(28, 10),
  bucket_type DECIMAL(15, 0),
  rate_end_date TIMESTAMP,
  origination_type DECIMAL(15, 0),
  origination_type_desc STRING,
  customer_id DECIMAL(15, 0),
  ship_id DECIMAL(15, 0),
  bill_id DECIMAL(15, 0),
  comments STRING,
  source_organization_id DECIMAL(15, 0),
  source_organization_code STRING,
  source_forecast_designator STRING,
  source_code STRING,
  source_line_id DECIMAL(15, 0),
  end_item_id DECIMAL(15, 0),
  end_planning_bom_percent DECIMAL(28, 10),
  forecast_rule_id DECIMAL(15, 0),
  demand_usage_start_date TIMESTAMP,
  forecast_trend DECIMAL(28, 10),
  focus_type DECIMAL(15, 0),
  forecast_mad DECIMAL(28, 10),
  demand_class STRING,
  request_id DECIMAL(15, 0),
  program_application_id DECIMAL(15, 0),
  program_id DECIMAL(15, 0),
  program_update_date TIMESTAMP,
  ddf_context STRING,
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
  project_id DECIMAL(15, 0),
  project_number STRING,
  task_id DECIMAL(15, 0),
  task_number STRING,
  line_id DECIMAL(15, 0),
  line_code STRING,
  concatenated_segments STRING,
  KCA_OPERATION STRING,
  IS_DELETED_FLG STRING,
  kca_seq_id DECIMAL(36, 0),
  kca_seq_date TIMESTAMP
);
INSERT INTO silver_bec_ods.MRP_FORECAST_DATES_V (
  TRANSACTION_ID,
  LAST_UPDATE_DATE,
  LAST_UPDATED_BY,
  CREATION_DATE,
  CREATED_BY,
  LAST_UPDATE_LOGIN,
  INVENTORY_ITEM_ID,
  ORGANIZATION_ID,
  FORECAST_DESIGNATOR,
  FORECAST_DATE,
  ORIGINAL_FORECAST_QUANTITY,
  CURRENT_FORECAST_QUANTITY,
  CONFIDENCE_PERCENTAGE,
  BUCKET_TYPE,
  RATE_END_DATE,
  ORIGINATION_TYPE,
  ORIGINATION_TYPE_DESC,
  CUSTOMER_ID,
  SHIP_ID,
  BILL_ID,
  COMMENTS,
  SOURCE_ORGANIZATION_ID,
  SOURCE_ORGANIZATION_CODE,
  SOURCE_FORECAST_DESIGNATOR,
  SOURCE_CODE,
  SOURCE_LINE_ID,
  END_ITEM_ID,
  END_PLANNING_BOM_PERCENT,
  FORECAST_RULE_ID,
  DEMAND_USAGE_START_DATE,
  FORECAST_TREND,
  FOCUS_TYPE,
  FORECAST_MAD,
  DEMAND_CLASS,
  REQUEST_ID,
  PROGRAM_APPLICATION_ID,
  PROGRAM_ID,
  PROGRAM_UPDATE_DATE,
  DDF_CONTEXT,
  ATTRIBUTE_CATEGORY,
  ATTRIBUTE1,
  ATTRIBUTE2,
  ATTRIBUTE3,
  ATTRIBUTE4,
  ATTRIBUTE5,
  ATTRIBUTE6,
  ATTRIBUTE7,
  ATTRIBUTE8,
  ATTRIBUTE9,
  ATTRIBUTE10,
  ATTRIBUTE11,
  ATTRIBUTE12,
  ATTRIBUTE13,
  ATTRIBUTE14,
  ATTRIBUTE15,
  PROJECT_ID,
  PROJECT_NUMBER,
  TASK_ID,
  TASK_NUMBER,
  LINE_ID,
  LINE_CODE,
  CONCATENATED_SEGMENTS,
  KCA_OPERATION,
  IS_DELETED_FLG,
  kca_seq_id,
  kca_seq_date
)
SELECT
  TRANSACTION_ID,
  LAST_UPDATE_DATE,
  LAST_UPDATED_BY,
  CREATION_DATE,
  CREATED_BY,
  LAST_UPDATE_LOGIN,
  INVENTORY_ITEM_ID,
  ORGANIZATION_ID,
  FORECAST_DESIGNATOR,
  FORECAST_DATE,
  ORIGINAL_FORECAST_QUANTITY,
  CURRENT_FORECAST_QUANTITY,
  CONFIDENCE_PERCENTAGE,
  BUCKET_TYPE,
  RATE_END_DATE,
  ORIGINATION_TYPE,
  ORIGINATION_TYPE_DESC,
  CUSTOMER_ID,
  SHIP_ID,
  BILL_ID,
  COMMENTS,
  SOURCE_ORGANIZATION_ID,
  SOURCE_ORGANIZATION_CODE,
  SOURCE_FORECAST_DESIGNATOR,
  SOURCE_CODE,
  SOURCE_LINE_ID,
  END_ITEM_ID,
  END_PLANNING_BOM_PERCENT,
  FORECAST_RULE_ID,
  DEMAND_USAGE_START_DATE,
  FORECAST_TREND,
  FOCUS_TYPE,
  FORECAST_MAD,
  DEMAND_CLASS,
  REQUEST_ID,
  PROGRAM_APPLICATION_ID,
  PROGRAM_ID,
  PROGRAM_UPDATE_DATE,
  DDF_CONTEXT,
  ATTRIBUTE_CATEGORY,
  ATTRIBUTE1,
  ATTRIBUTE2,
  ATTRIBUTE3,
  ATTRIBUTE4,
  ATTRIBUTE5,
  ATTRIBUTE6,
  ATTRIBUTE7,
  ATTRIBUTE8,
  ATTRIBUTE9,
  ATTRIBUTE10,
  ATTRIBUTE11,
  ATTRIBUTE12,
  ATTRIBUTE13,
  ATTRIBUTE14,
  ATTRIBUTE15,
  PROJECT_ID,
  PROJECT_NUMBER,
  TASK_ID,
  TASK_NUMBER,
  LINE_ID,
  LINE_CODE,
  CONCATENATED_SEGMENTS,
  KCA_OPERATION,
  'N' AS IS_DELETED_FLG,
  CAST(NULLIF(KCA_SEQ_ID, '') AS DECIMAL(36, 0)) AS KCA_SEQ_ID,
  kca_seq_date
FROM bronze_bec_ods_stg.MRP_FORECAST_DATES_V;
UPDATE bec_etl_ctrl.batch_ods_info SET load_type = 'I', last_refresh_date = CURRENT_TIMESTAMP()
WHERE
  ods_table_name = 'mrp_forecast_dates_v';