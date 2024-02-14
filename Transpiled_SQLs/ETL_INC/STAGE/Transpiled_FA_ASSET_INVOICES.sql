TRUNCATE table
	table bronze_bec_ods_stg.FA_ASSET_INVOICES;
INSERT INTO bronze_bec_ods_stg.FA_ASSET_INVOICES (
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
  KCA_OPERATION,
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
    KCA_OPERATION,
    kca_seq_id,
    kca_seq_date
  FROM bec_raw_dl_ext.FA_ASSET_INVOICES
  WHERE
    kca_operation <> 'DELETE'
    AND COALESCE(kca_seq_id, '') <> ''
    AND (COALESCE(SOURCE_LINE_ID, 0), KCA_SEQ_ID) IN (
      SELECT
        COALESCE(SOURCE_LINE_ID, 0) AS SOURCE_LINE_ID,
        MAX(KCA_SEQ_ID)
      FROM bec_raw_dl_ext.FA_ASSET_INVOICES
      WHERE
        kca_operation <> 'DELETE' AND COALESCE(kca_seq_id, '') <> ''
      GROUP BY
        COALESCE(SOURCE_LINE_ID, 0)
    )
    AND kca_seq_date > (
      SELECT
        (
          executebegints - prune_days
        )
      FROM bec_etl_ctrl.batch_ods_info
      WHERE
        ods_table_name = 'fa_asset_invoices'
    )
);