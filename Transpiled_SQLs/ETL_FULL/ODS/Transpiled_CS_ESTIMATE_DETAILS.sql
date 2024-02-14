DROP TABLE IF EXISTS silver_bec_ods.CS_ESTIMATE_DETAILS;
CREATE TABLE IF NOT EXISTS silver_bec_ods.CS_ESTIMATE_DETAILS (
  estimate_detail_id DECIMAL(15, 0),
  last_update_date TIMESTAMP,
  last_updated_by DECIMAL(15, 0),
  creation_date TIMESTAMP,
  created_by DECIMAL(15, 0),
  last_update_login DECIMAL(15, 0),
  estimate_id DECIMAL(15, 0),
  line_number DECIMAL(15, 0),
  inventory_item_id DECIMAL(15, 0),
  serial_number STRING,
  quantity_required DECIMAL(28, 10),
  unit_of_measure_code STRING,
  selling_price DECIMAL(28, 10),
  after_warranty_cost DECIMAL(28, 10),
  diagnosis_id DECIMAL(15, 0),
  estimate_business_group_id DECIMAL(15, 0),
  transaction_type_id DECIMAL(15, 0),
  customer_product_id DECIMAL(15, 0),
  order_header_id DECIMAL(15, 0),
  original_system_reference STRING,
  original_system_line_reference STRING,
  installed_cp_return_by_date TIMESTAMP,
  new_cp_return_by_date TIMESTAMP,
  interface_to_oe_flag STRING,
  rollup_flag STRING,
  add_to_order STRING,
  system_id DECIMAL(15, 0),
  rma_header_id DECIMAL(15, 0),
  rma_number DECIMAL(15, 0),
  rma_line_id DECIMAL(15, 0),
  rma_line_number DECIMAL(15, 0),
  pricing_context STRING,
  pricing_attribute1 STRING,
  pricing_attribute2 STRING,
  pricing_attribute3 STRING,
  pricing_attribute4 STRING,
  pricing_attribute5 STRING,
  pricing_attribute6 STRING,
  pricing_attribute7 STRING,
  pricing_attribute8 STRING,
  pricing_attribute9 STRING,
  pricing_attribute10 STRING,
  pricing_attribute11 STRING,
  pricing_attribute12 STRING,
  pricing_attribute13 STRING,
  pricing_attribute14 STRING,
  pricing_attribute15 STRING,
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
  context STRING,
  technician_id DECIMAL(15, 0),
  txn_start_time TIMESTAMP,
  txn_end_time TIMESTAMP,
  organization_id DECIMAL(15, 0),
  coverage_bill_rate_id DECIMAL(15, 0),
  coverage_billing_type_id DECIMAL(15, 0),
  time_zone_id DECIMAL(15, 0),
  txn_billing_type_id DECIMAL(15, 0),
  business_process_id DECIMAL(15, 0),
  incident_id DECIMAL(15, 0),
  original_source_id DECIMAL(15, 0),
  original_source_code STRING,
  source_id DECIMAL(15, 0),
  source_code STRING,
  contract_id DECIMAL(15, 0),
  coverage_id DECIMAL(15, 0),
  coverage_txn_group_id DECIMAL(15, 0),
  invoice_to_org_id DECIMAL(15, 0),
  ship_to_org_id DECIMAL(15, 0),
  purchase_order_num STRING,
  line_type_id DECIMAL(15, 0),
  line_category_code STRING,
  currency_code STRING,
  conversion_rate DECIMAL(28, 10),
  conversion_type_code STRING,
  conversion_rate_date TIMESTAMP,
  return_reason_code STRING,
  order_line_id DECIMAL(15, 0),
  price_list_header_id DECIMAL(15, 0),
  func_curr_aft_warr_cost DECIMAL(28, 10),
  orig_system_reference STRING,
  orig_system_line_reference STRING,
  add_to_order_flag STRING,
  exception_coverage_used STRING,
  tax_code STRING,
  est_tax_amount DECIMAL(28, 10),
  object_version_number DECIMAL(15, 0),
  pricing_attribute16 STRING,
  pricing_attribute17 STRING,
  pricing_attribute18 STRING,
  pricing_attribute19 STRING,
  pricing_attribute20 STRING,
  pricing_attribute21 STRING,
  pricing_attribute22 STRING,
  pricing_attribute23 STRING,
  pricing_attribute24 STRING,
  pricing_attribute25 STRING,
  pricing_attribute26 STRING,
  pricing_attribute27 STRING,
  pricing_attribute28 STRING,
  pricing_attribute29 STRING,
  pricing_attribute30 STRING,
  pricing_attribute31 STRING,
  pricing_attribute32 STRING,
  pricing_attribute33 STRING,
  pricing_attribute34 STRING,
  pricing_attribute35 STRING,
  pricing_attribute36 STRING,
  pricing_attribute37 STRING,
  pricing_attribute38 STRING,
  pricing_attribute39 STRING,
  pricing_attribute40 STRING,
  pricing_attribute41 STRING,
  pricing_attribute42 STRING,
  pricing_attribute43 STRING,
  pricing_attribute44 STRING,
  pricing_attribute45 STRING,
  pricing_attribute46 STRING,
  pricing_attribute47 STRING,
  pricing_attribute48 STRING,
  pricing_attribute49 STRING,
  pricing_attribute50 STRING,
  pricing_attribute51 STRING,
  pricing_attribute52 STRING,
  pricing_attribute53 STRING,
  pricing_attribute54 STRING,
  pricing_attribute55 STRING,
  pricing_attribute56 STRING,
  pricing_attribute57 STRING,
  pricing_attribute58 STRING,
  pricing_attribute59 STRING,
  pricing_attribute61 STRING,
  pricing_attribute62 STRING,
  pricing_attribute63 STRING,
  pricing_attribute64 STRING,
  pricing_attribute65 STRING,
  pricing_attribute66 STRING,
  pricing_attribute67 STRING,
  pricing_attribute68 STRING,
  pricing_attribute69 STRING,
  pricing_attribute70 STRING,
  pricing_attribute71 STRING,
  pricing_attribute72 STRING,
  pricing_attribute73 STRING,
  pricing_attribute74 STRING,
  pricing_attribute75 STRING,
  pricing_attribute76 STRING,
  pricing_attribute77 STRING,
  pricing_attribute78 STRING,
  pricing_attribute79 STRING,
  pricing_attribute80 STRING,
  pricing_attribute81 STRING,
  pricing_attribute82 STRING,
  pricing_attribute83 STRING,
  pricing_attribute84 STRING,
  pricing_attribute85 STRING,
  pricing_attribute86 STRING,
  pricing_attribute87 STRING,
  pricing_attribute88 STRING,
  pricing_attribute89 STRING,
  pricing_attribute90 STRING,
  pricing_attribute91 STRING,
  pricing_attribute92 STRING,
  pricing_attribute93 STRING,
  pricing_attribute94 STRING,
  pricing_attribute95 STRING,
  pricing_attribute96 STRING,
  pricing_attribute97 STRING,
  pricing_attribute98 STRING,
  pricing_attribute99 STRING,
  pricing_attribute100 STRING,
  pricing_attribute60 STRING,
  security_group_id DECIMAL(15, 0),
  upgraded_status_flag STRING,
  orig_system_reference_id DECIMAL(15, 0),
  no_charge_flag STRING,
  org_id DECIMAL(15, 0),
  item_revision STRING,
  trans_inv_organization_id DECIMAL(15, 0),
  trans_subinventory STRING,
  activity_date TIMESTAMP,
  activity_start_time TIMESTAMP,
  activity_end_time TIMESTAMP,
  generated_by_bca_engine_flag STRING,
  transaction_inventory_org DECIMAL(28, 10),
  transaction_sub_inventory STRING,
  activity_start_date_time TIMESTAMP,
  activity_end_date_time TIMESTAMP,
  charge_line_type STRING,
  ship_to_contact_id DECIMAL(15, 0),
  bill_to_contact_id DECIMAL(15, 0),
  ship_to_account_id DECIMAL(15, 0),
  invoice_to_account_id DECIMAL(15, 0),
  list_price DECIMAL(28, 10),
  contract_discount_amount DECIMAL(28, 10),
  bill_to_party_id DECIMAL(15, 0),
  ship_to_party_id DECIMAL(15, 0),
  submit_restriction_message STRING,
  submit_error_message STRING,
  submit_from_system STRING,
  line_submitted STRING,
  contract_line_id DECIMAL(15, 0),
  rate_type_code STRING,
  instrument_payment_use_id DECIMAL(15, 0),
  parent_instance_id DECIMAL(15, 0),
  shipping_method STRING,
  arrival_date_time TIMESTAMP,
  cost DECIMAL(28, 10),
  available_quantity DECIMAL(28, 10),
  return_type STRING,
  location_id DECIMAL(15, 0),
  shipping_distance STRING,
  need_by_date TIMESTAMP,
  project_id DECIMAL(15, 0),
  project_task_id DECIMAL(15, 0),
  expenditure_org_id DECIMAL(15, 0),
  carrier STRING,
  reservation_id DECIMAL(15, 0),
  price_adj_details STRING,
  ref_est_detail_id DECIMAL(15, 0),
  KCA_OPERATION STRING,
  IS_DELETED_FLG STRING,
  kca_seq_id DECIMAL(36, 0),
  kca_seq_date TIMESTAMP
);
INSERT INTO silver_bec_ods.CS_ESTIMATE_DETAILS (
  estimate_detail_id,
  last_update_date,
  last_updated_by,
  creation_date,
  created_by,
  last_update_login,
  estimate_id,
  line_number,
  inventory_item_id,
  serial_number,
  quantity_required,
  unit_of_measure_code,
  selling_price,
  after_warranty_cost,
  diagnosis_id,
  estimate_business_group_id,
  transaction_type_id,
  customer_product_id,
  order_header_id,
  original_system_reference,
  original_system_line_reference,
  installed_cp_return_by_date,
  new_cp_return_by_date,
  interface_to_oe_flag,
  rollup_flag,
  add_to_order,
  system_id,
  rma_header_id,
  rma_number,
  rma_line_id,
  rma_line_number,
  pricing_context,
  pricing_attribute1,
  pricing_attribute2,
  pricing_attribute3,
  pricing_attribute4,
  pricing_attribute5,
  pricing_attribute6,
  pricing_attribute7,
  pricing_attribute8,
  pricing_attribute9,
  pricing_attribute10,
  pricing_attribute11,
  pricing_attribute12,
  pricing_attribute13,
  pricing_attribute14,
  pricing_attribute15,
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
  context,
  technician_id,
  txn_start_time,
  txn_end_time,
  organization_id,
  coverage_bill_rate_id,
  coverage_billing_type_id,
  time_zone_id,
  txn_billing_type_id,
  business_process_id,
  incident_id,
  original_source_id,
  original_source_code,
  source_id,
  source_code,
  contract_id,
  coverage_id,
  coverage_txn_group_id,
  invoice_to_org_id,
  ship_to_org_id,
  purchase_order_num,
  line_type_id,
  line_category_code,
  currency_code,
  conversion_rate,
  conversion_type_code,
  conversion_rate_date,
  return_reason_code,
  order_line_id,
  price_list_header_id,
  func_curr_aft_warr_cost,
  orig_system_reference,
  orig_system_line_reference,
  add_to_order_flag,
  exception_coverage_used,
  tax_code,
  est_tax_amount,
  object_version_number,
  pricing_attribute16,
  pricing_attribute17,
  pricing_attribute18,
  pricing_attribute19,
  pricing_attribute20,
  pricing_attribute21,
  pricing_attribute22,
  pricing_attribute23,
  pricing_attribute24,
  pricing_attribute25,
  pricing_attribute26,
  pricing_attribute27,
  pricing_attribute28,
  pricing_attribute29,
  pricing_attribute30,
  pricing_attribute31,
  pricing_attribute32,
  pricing_attribute33,
  pricing_attribute34,
  pricing_attribute35,
  pricing_attribute36,
  pricing_attribute37,
  pricing_attribute38,
  pricing_attribute39,
  pricing_attribute40,
  pricing_attribute41,
  pricing_attribute42,
  pricing_attribute43,
  pricing_attribute44,
  pricing_attribute45,
  pricing_attribute46,
  pricing_attribute47,
  pricing_attribute48,
  pricing_attribute49,
  pricing_attribute50,
  pricing_attribute51,
  pricing_attribute52,
  pricing_attribute53,
  pricing_attribute54,
  pricing_attribute55,
  pricing_attribute56,
  pricing_attribute57,
  pricing_attribute58,
  pricing_attribute59,
  pricing_attribute61,
  pricing_attribute62,
  pricing_attribute63,
  pricing_attribute64,
  pricing_attribute65,
  pricing_attribute66,
  pricing_attribute67,
  pricing_attribute68,
  pricing_attribute69,
  pricing_attribute70,
  pricing_attribute71,
  pricing_attribute72,
  pricing_attribute73,
  pricing_attribute74,
  pricing_attribute75,
  pricing_attribute76,
  pricing_attribute77,
  pricing_attribute78,
  pricing_attribute79,
  pricing_attribute80,
  pricing_attribute81,
  pricing_attribute82,
  pricing_attribute83,
  pricing_attribute84,
  pricing_attribute85,
  pricing_attribute86,
  pricing_attribute87,
  pricing_attribute88,
  pricing_attribute89,
  pricing_attribute90,
  pricing_attribute91,
  pricing_attribute92,
  pricing_attribute93,
  pricing_attribute94,
  pricing_attribute95,
  pricing_attribute96,
  pricing_attribute97,
  pricing_attribute98,
  pricing_attribute99,
  pricing_attribute100,
  pricing_attribute60,
  security_group_id,
  upgraded_status_flag,
  orig_system_reference_id,
  no_charge_flag,
  org_id,
  item_revision,
  trans_inv_organization_id,
  trans_subinventory,
  activity_date,
  activity_start_time,
  activity_end_time,
  generated_by_bca_engine_flag,
  transaction_inventory_org,
  transaction_sub_inventory,
  activity_start_date_time,
  activity_end_date_time,
  charge_line_type,
  ship_to_contact_id,
  bill_to_contact_id,
  ship_to_account_id,
  invoice_to_account_id,
  list_price,
  contract_discount_amount,
  bill_to_party_id,
  ship_to_party_id,
  submit_restriction_message,
  submit_error_message,
  submit_from_system,
  line_submitted,
  contract_line_id,
  rate_type_code,
  instrument_payment_use_id,
  parent_instance_id,
  shipping_method,
  arrival_date_time,
  cost,
  available_quantity,
  return_type,
  location_id,
  shipping_distance,
  need_by_date,
  project_id,
  project_task_id,
  expenditure_org_id,
  carrier,
  reservation_id,
  price_adj_details,
  ref_est_detail_id,
  KCA_OPERATION,
  IS_DELETED_FLG,
  kca_seq_id,
  kca_seq_date
)
SELECT
  estimate_detail_id,
  last_update_date,
  last_updated_by,
  creation_date,
  created_by,
  last_update_login,
  estimate_id,
  line_number,
  inventory_item_id,
  serial_number,
  quantity_required,
  unit_of_measure_code,
  selling_price,
  after_warranty_cost,
  diagnosis_id,
  estimate_business_group_id,
  transaction_type_id,
  customer_product_id,
  order_header_id,
  original_system_reference,
  original_system_line_reference,
  installed_cp_return_by_date,
  new_cp_return_by_date,
  interface_to_oe_flag,
  rollup_flag,
  add_to_order,
  system_id,
  rma_header_id,
  rma_number,
  rma_line_id,
  rma_line_number,
  pricing_context,
  pricing_attribute1,
  pricing_attribute2,
  pricing_attribute3,
  pricing_attribute4,
  pricing_attribute5,
  pricing_attribute6,
  pricing_attribute7,
  pricing_attribute8,
  pricing_attribute9,
  pricing_attribute10,
  pricing_attribute11,
  pricing_attribute12,
  pricing_attribute13,
  pricing_attribute14,
  pricing_attribute15,
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
  context,
  technician_id,
  txn_start_time,
  txn_end_time,
  organization_id,
  coverage_bill_rate_id,
  coverage_billing_type_id,
  time_zone_id,
  txn_billing_type_id,
  business_process_id,
  incident_id,
  original_source_id,
  original_source_code,
  source_id,
  source_code,
  contract_id,
  coverage_id,
  coverage_txn_group_id,
  invoice_to_org_id,
  ship_to_org_id,
  purchase_order_num,
  line_type_id,
  line_category_code,
  currency_code,
  conversion_rate,
  conversion_type_code,
  conversion_rate_date,
  return_reason_code,
  order_line_id,
  price_list_header_id,
  func_curr_aft_warr_cost,
  orig_system_reference,
  orig_system_line_reference,
  add_to_order_flag,
  exception_coverage_used,
  tax_code,
  est_tax_amount,
  object_version_number,
  pricing_attribute16,
  pricing_attribute17,
  pricing_attribute18,
  pricing_attribute19,
  pricing_attribute20,
  pricing_attribute21,
  pricing_attribute22,
  pricing_attribute23,
  pricing_attribute24,
  pricing_attribute25,
  pricing_attribute26,
  pricing_attribute27,
  pricing_attribute28,
  pricing_attribute29,
  pricing_attribute30,
  pricing_attribute31,
  pricing_attribute32,
  pricing_attribute33,
  pricing_attribute34,
  pricing_attribute35,
  pricing_attribute36,
  pricing_attribute37,
  pricing_attribute38,
  pricing_attribute39,
  pricing_attribute40,
  pricing_attribute41,
  pricing_attribute42,
  pricing_attribute43,
  pricing_attribute44,
  pricing_attribute45,
  pricing_attribute46,
  pricing_attribute47,
  pricing_attribute48,
  pricing_attribute49,
  pricing_attribute50,
  pricing_attribute51,
  pricing_attribute52,
  pricing_attribute53,
  pricing_attribute54,
  pricing_attribute55,
  pricing_attribute56,
  pricing_attribute57,
  pricing_attribute58,
  pricing_attribute59,
  pricing_attribute61,
  pricing_attribute62,
  pricing_attribute63,
  pricing_attribute64,
  pricing_attribute65,
  pricing_attribute66,
  pricing_attribute67,
  pricing_attribute68,
  pricing_attribute69,
  pricing_attribute70,
  pricing_attribute71,
  pricing_attribute72,
  pricing_attribute73,
  pricing_attribute74,
  pricing_attribute75,
  pricing_attribute76,
  pricing_attribute77,
  pricing_attribute78,
  pricing_attribute79,
  pricing_attribute80,
  pricing_attribute81,
  pricing_attribute82,
  pricing_attribute83,
  pricing_attribute84,
  pricing_attribute85,
  pricing_attribute86,
  pricing_attribute87,
  pricing_attribute88,
  pricing_attribute89,
  pricing_attribute90,
  pricing_attribute91,
  pricing_attribute92,
  pricing_attribute93,
  pricing_attribute94,
  pricing_attribute95,
  pricing_attribute96,
  pricing_attribute97,
  pricing_attribute98,
  pricing_attribute99,
  pricing_attribute100,
  pricing_attribute60,
  security_group_id,
  upgraded_status_flag,
  orig_system_reference_id,
  no_charge_flag,
  org_id,
  item_revision,
  trans_inv_organization_id,
  trans_subinventory,
  activity_date,
  activity_start_time,
  activity_end_time,
  generated_by_bca_engine_flag,
  transaction_inventory_org,
  transaction_sub_inventory,
  activity_start_date_time,
  activity_end_date_time,
  charge_line_type,
  ship_to_contact_id,
  bill_to_contact_id,
  ship_to_account_id,
  invoice_to_account_id,
  list_price,
  contract_discount_amount,
  bill_to_party_id,
  ship_to_party_id,
  submit_restriction_message,
  submit_error_message,
  submit_from_system,
  line_submitted,
  contract_line_id,
  rate_type_code,
  instrument_payment_use_id,
  parent_instance_id,
  shipping_method,
  arrival_date_time,
  cost,
  available_quantity,
  return_type,
  location_id,
  shipping_distance,
  need_by_date,
  project_id,
  project_task_id,
  expenditure_org_id,
  carrier,
  reservation_id,
  price_adj_details,
  ref_est_detail_id,
  KCA_OPERATION,
  'N' AS IS_DELETED_FLG,
  CAST(NULLIF(KCA_SEQ_ID, '') AS DECIMAL(36, 0)) AS KCA_SEQ_ID,
  kca_seq_date
FROM bronze_bec_ods_stg.CS_ESTIMATE_DETAILS;
UPDATE bec_etl_ctrl.batch_ods_info SET load_type = 'I', last_refresh_date = CURRENT_TIMESTAMP()
WHERE
  ods_table_name = 'cs_estimate_details';