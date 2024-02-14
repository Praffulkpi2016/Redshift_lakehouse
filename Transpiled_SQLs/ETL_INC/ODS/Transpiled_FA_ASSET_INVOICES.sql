/* Delete Records */
DELETE FROM silver_bec_ods.FA_ASSET_INVOICES
WHERE
  COALESCE(SOURCE_LINE_ID, 0) IN (
    SELECT
      COALESCE(stg.SOURCE_LINE_ID, 0) AS SOURCE_LINE_ID
    FROM silver_bec_ods.FA_ASSET_INVOICES AS ods, bronze_bec_ods_stg.FA_ASSET_INVOICES AS stg
    WHERE
      COALESCE(ods.SOURCE_LINE_ID, 0) = COALESCE(stg.SOURCE_LINE_ID, 0)
      AND stg.kca_operation IN ('INSERT', 'UPDATE')
  );
/* Insert records */
INSERT INTO silver_bec_ods.FA_ASSET_INVOICES (
  asset_id,
  po_vendor_id,
  asset_invoice_id,
  fixed_assets_cost,
  date_effective,
  date_ineffective,
  invoice_transaction_id_in,
  invoice_transaction_id_out,
  deleted_flag,
  po_number,
  invoice_number,
  payables_batch_name,
  payables_code_combination_id,
  feeder_system_name,
  create_batch_date,
  create_batch_id,
  invoice_date,
  payables_cost,
  post_batch_id,
  invoice_id,
  ap_distribution_line_number,
  payables_units,
  split_merged_code,
  description,
  parent_mass_addition_id,
  last_update_date,
  last_updated_by,
  created_by,
  creation_date,
  last_update_login,
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
  attribute_category_code,
  unrevalued_cost,
  merged_code,
  split_code,
  merge_parent_mass_additions_id,
  split_parent_mass_additions_id,
  project_asset_line_id,
  project_id,
  task_id,
  source_line_id,
  depreciate_in_group_flag,
  material_indicator_flag,
  prior_source_line_id,
  invoice_distribution_id,
  invoice_line_number,
  po_distribution_id,
  kca_operation,
  IS_DELETED_FLG,
  kca_seq_id,
  kca_seq_date
)
(
  SELECT
    asset_id,
    po_vendor_id,
    asset_invoice_id,
    fixed_assets_cost,
    date_effective,
    date_ineffective,
    invoice_transaction_id_in,
    invoice_transaction_id_out,
    deleted_flag,
    po_number,
    invoice_number,
    payables_batch_name,
    payables_code_combination_id,
    feeder_system_name,
    create_batch_date,
    create_batch_id,
    invoice_date,
    payables_cost,
    post_batch_id,
    invoice_id,
    ap_distribution_line_number,
    payables_units,
    split_merged_code,
    description,
    parent_mass_addition_id,
    last_update_date,
    last_updated_by,
    created_by,
    creation_date,
    last_update_login,
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
    attribute_category_code,
    unrevalued_cost,
    merged_code,
    split_code,
    merge_parent_mass_additions_id,
    split_parent_mass_additions_id,
    project_asset_line_id,
    project_id,
    task_id,
    source_line_id,
    depreciate_in_group_flag,
    material_indicator_flag,
    prior_source_line_id,
    invoice_distribution_id,
    invoice_line_number,
    po_distribution_id,
    kca_operation,
    'N' AS IS_DELETED_FLG,
    CAST(NULLIF(KCA_SEQ_ID, '') AS DECIMAL(36, 0)) AS KCA_SEQ_ID,
    kca_seq_date
  FROM bronze_bec_ods_stg.FA_ASSET_INVOICES
  WHERE
    kca_operation IN ('INSERT', 'UPDATE')
    AND (COALESCE(SOURCE_LINE_ID, 0), KCA_SEQ_ID) IN (
      SELECT
        COALESCE(SOURCE_LINE_ID, 0) AS SOURCE_LINE_ID,
        MAX(KCA_SEQ_ID)
      FROM bronze_bec_ods_stg.FA_ASSET_INVOICES
      WHERE
        kca_operation IN ('INSERT', 'UPDATE')
      GROUP BY
        COALESCE(SOURCE_LINE_ID, 0)
    )
);
/* Soft delete */
UPDATE silver_bec_ods.FA_ASSET_INVOICES SET IS_DELETED_FLG = 'N';
UPDATE silver_bec_ods.FA_ASSET_INVOICES SET IS_DELETED_FLG = 'Y'
WHERE
  (
    SOURCE_LINE_ID
  ) IN (
    SELECT
      SOURCE_LINE_ID
    FROM bec_raw_dl_ext.FA_ASSET_INVOICES
    WHERE
      (SOURCE_LINE_ID, KCA_SEQ_ID) IN (
        SELECT
          SOURCE_LINE_ID,
          MAX(KCA_SEQ_ID) AS KCA_SEQ_ID
        FROM bec_raw_dl_ext.FA_ASSET_INVOICES
        GROUP BY
          SOURCE_LINE_ID
      )
      AND kca_operation = 'DELETE'
  );
UPDATE bec_etl_ctrl.batch_ods_info SET last_refresh_date = CURRENT_TIMESTAMP()
WHERE
  ods_table_name = 'fa_asset_invoices';