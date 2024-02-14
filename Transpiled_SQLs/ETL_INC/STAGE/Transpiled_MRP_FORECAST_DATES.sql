TRUNCATE table bronze_bec_ods_stg.MRP_FORECAST_DATES;
INSERT INTO bronze_bec_ods_stg.MRP_FORECAST_DATES (
  transaction_id,
  last_update_date,
  last_updated_by,
  creation_date,
  created_by,
  last_update_login,
  inventory_item_id,
  organization_id,
  forecast_designator,
  forecast_date,
  original_forecast_quantity,
  current_forecast_quantity,
  confidence_percentage,
  bucket_type,
  rate_end_date,
  origination_type,
  customer_id,
  ship_id,
  bill_id,
  comments,
  source_organization_id,
  source_forecast_designator,
  source_code,
  source_line_id,
  end_item_id,
  end_planning_bom_percent,
  forecast_rule_id,
  demand_usage_start_date,
  forecast_trend,
  focus_type,
  forecast_mad,
  demand_class,
  request_id,
  program_application_id,
  program_id,
  program_update_date,
  old_transaction_id,
  to_update,
  ddf_context,
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
  project_id,
  task_id,
  line_id,
  KCA_OPERATION,
  kca_seq_id,
  kca_seq_date
)
(
  SELECT
    transaction_id,
    last_update_date,
    last_updated_by,
    creation_date,
    created_by,
    last_update_login,
    inventory_item_id,
    organization_id,
    forecast_designator,
    forecast_date,
    original_forecast_quantity,
    current_forecast_quantity,
    confidence_percentage,
    bucket_type,
    rate_end_date,
    origination_type,
    customer_id,
    ship_id,
    bill_id,
    comments,
    source_organization_id,
    source_forecast_designator,
    source_code,
    source_line_id,
    end_item_id,
    end_planning_bom_percent,
    forecast_rule_id,
    demand_usage_start_date,
    forecast_trend,
    focus_type,
    forecast_mad,
    demand_class,
    request_id,
    program_application_id,
    program_id,
    program_update_date,
    old_transaction_id,
    to_update,
    ddf_context,
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
    project_id,
    task_id,
    line_id,
    KCA_OPERATION,
    kca_seq_id,
    kca_seq_date
  FROM bec_raw_dl_ext.MRP_FORECAST_DATES
  WHERE
    kca_operation <> 'DELETE'
    AND COALESCE(kca_seq_id, '') <> ''
    AND (COALESCE(TRANSACTION_ID, 0), KCA_SEQ_ID) IN (
      SELECT
        COALESCE(TRANSACTION_ID, 0) AS TRANSACTION_ID,
        MAX(KCA_SEQ_ID)
      FROM bec_raw_dl_ext.MRP_FORECAST_DATES
      WHERE
        kca_operation <> 'DELETE' AND COALESCE(kca_seq_id, '') <> ''
      GROUP BY
        COALESCE(TRANSACTION_ID, 0)
    )
    AND kca_seq_date > (
      SELECT
        (
          executebegints - prune_days
        )
      FROM bec_etl_ctrl.batch_ods_info
      WHERE
        ods_table_name = 'mrp_forecast_dates'
    )
);