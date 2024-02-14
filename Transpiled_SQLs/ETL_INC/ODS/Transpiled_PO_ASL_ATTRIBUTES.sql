/* Delete Records */
DELETE FROM silver_bec_ods.PO_ASL_ATTRIBUTES
WHERE
  (COALESCE(ASL_ID, 0), COALESCE(USING_ORGANIZATION_ID, 0)) IN (
    SELECT
      COALESCE(stg.ASL_ID, 0) AS ASL_ID,
      COALESCE(stg.USING_ORGANIZATION_ID, 0) AS USING_ORGANIZATION_ID
    FROM silver_bec_ods.PO_ASL_ATTRIBUTES AS ods, bronze_bec_ods_stg.PO_ASL_ATTRIBUTES AS stg
    WHERE
      COALESCE(ods.ASL_ID, 0) = COALESCE(stg.ASL_ID, 0)
      AND COALESCE(ods.USING_ORGANIZATION_ID, 0) = COALESCE(stg.USING_ORGANIZATION_ID, 0)
      AND stg.kca_operation IN ('INSERT', 'UPDATE')
  );
/* Insert records */
INSERT INTO silver_bec_ods.PO_ASL_ATTRIBUTES (
  asl_id,
  using_organization_id,
  last_update_date,
  last_updated_by,
  creation_date,
  created_by,
  document_sourcing_method,
  release_generation_method,
  purchasing_unit_of_measure,
  enable_plan_schedule_flag,
  enable_ship_schedule_flag,
  plan_schedule_type,
  ship_schedule_type,
  plan_bucket_pattern_id,
  ship_bucket_pattern_id,
  enable_autoschedule_flag,
  scheduler_id,
  enable_authorizations_flag,
  vendor_id,
  vendor_site_id,
  item_id,
  category_id,
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
  last_update_login,
  request_id,
  program_application_id,
  program_id,
  program_update_date,
  price_update_tolerance,
  processing_lead_time,
  min_order_qty,
  fixed_lot_multiple,
  delivery_calendar,
  country_of_origin_code,
  enable_vmi_flag,
  vmi_min_qty,
  vmi_max_qty,
  enable_vmi_auto_replenish_flag,
  vmi_replenishment_approval,
  consigned_from_supplier_flag,
  last_billing_date,
  consigned_billing_cycle,
  consume_on_aging_flag,
  aging_period,
  replenishment_method,
  vmi_min_days,
  vmi_max_days,
  fixed_order_quantity,
  forecast_horizon,
  KCA_OPERATION,
  IS_DELETED_FLG,
  kca_seq_id,
  kca_seq_date
)
(
  SELECT
    asl_id,
    using_organization_id,
    last_update_date,
    last_updated_by,
    creation_date,
    created_by,
    document_sourcing_method,
    release_generation_method,
    purchasing_unit_of_measure,
    enable_plan_schedule_flag,
    enable_ship_schedule_flag,
    plan_schedule_type,
    ship_schedule_type,
    plan_bucket_pattern_id,
    ship_bucket_pattern_id,
    enable_autoschedule_flag,
    scheduler_id,
    enable_authorizations_flag,
    vendor_id,
    vendor_site_id,
    item_id,
    category_id,
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
    last_update_login,
    request_id,
    program_application_id,
    program_id,
    program_update_date,
    price_update_tolerance,
    processing_lead_time,
    min_order_qty,
    fixed_lot_multiple,
    delivery_calendar,
    country_of_origin_code,
    enable_vmi_flag,
    vmi_min_qty,
    vmi_max_qty,
    enable_vmi_auto_replenish_flag,
    vmi_replenishment_approval,
    consigned_from_supplier_flag,
    last_billing_date,
    consigned_billing_cycle,
    consume_on_aging_flag,
    aging_period,
    replenishment_method,
    vmi_min_days,
    vmi_max_days,
    fixed_order_quantity,
    forecast_horizon,
    KCA_OPERATION,
    'N' AS IS_DELETED_FLG,
    CAST(NULLIF(KCA_SEQ_ID, '') AS DECIMAL(36, 0)) AS KCA_SEQ_ID,
    kca_seq_date
  FROM bronze_bec_ods_stg.PO_ASL_ATTRIBUTES
  WHERE
    kca_operation IN ('INSERT', 'UPDATE')
    AND (COALESCE(ASL_ID, 0), COALESCE(USING_ORGANIZATION_ID, 0), kca_seq_id) IN (
      SELECT
        COALESCE(ASL_ID, 0) AS ASL_ID,
        COALESCE(USING_ORGANIZATION_ID, 0) AS USING_ORGANIZATION_ID,
        MAX(kca_seq_id)
      FROM bronze_bec_ods_stg.PO_ASL_ATTRIBUTES
      WHERE
        kca_operation IN ('INSERT', 'UPDATE')
      GROUP BY
        COALESCE(ASL_ID, 0),
        COALESCE(USING_ORGANIZATION_ID, 0)
    )
);
/* Soft delete */
UPDATE silver_bec_ods.PO_ASL_ATTRIBUTES SET IS_DELETED_FLG = 'N';
UPDATE silver_bec_ods.PO_ASL_ATTRIBUTES SET IS_DELETED_FLG = 'Y'
WHERE
  (COALESCE(ASL_ID, 0), COALESCE(USING_ORGANIZATION_ID, 0)) IN (
    SELECT
      COALESCE(ASL_ID, 0),
      COALESCE(USING_ORGANIZATION_ID, 0)
    FROM bec_raw_dl_ext.PO_ASL_ATTRIBUTES
    WHERE
      (COALESCE(ASL_ID, 0), COALESCE(USING_ORGANIZATION_ID, 0), KCA_SEQ_ID) IN (
        SELECT
          COALESCE(ASL_ID, 0),
          COALESCE(USING_ORGANIZATION_ID, 0),
          MAX(KCA_SEQ_ID) AS KCA_SEQ_ID
        FROM bec_raw_dl_ext.PO_ASL_ATTRIBUTES
        GROUP BY
          COALESCE(ASL_ID, 0),
          COALESCE(USING_ORGANIZATION_ID, 0)
      )
      AND kca_operation = 'DELETE'
  );
UPDATE bec_etl_ctrl.batch_ods_info SET last_refresh_date = CURRENT_TIMESTAMP()
WHERE
  ods_table_name = 'PO_ASL_ATTRIBUTES';