DROP TABLE IF EXISTS silver_bec_ods.hz_cust_site_uses_all;
CREATE TABLE IF NOT EXISTS silver_bec_ods.hz_cust_site_uses_all (
  site_use_id DECIMAL(15, 0),
  cust_acct_site_id DECIMAL(15, 0),
  last_update_date TIMESTAMP,
  last_updated_by DECIMAL(15, 0),
  creation_date TIMESTAMP,
  created_by DECIMAL(15, 0),
  site_use_code STRING,
  primary_flag STRING,
  status STRING,
  `location` STRING,
  last_update_login DECIMAL(15, 0),
  contact_id DECIMAL(15, 0),
  bill_to_site_use_id DECIMAL(15, 0),
  orig_system_reference STRING,
  sic_code STRING,
  payment_term_id DECIMAL(15, 0),
  gsa_indicator STRING,
  ship_partial STRING,
  ship_via STRING,
  fob_point STRING,
  order_type_id DECIMAL(15, 0),
  price_list_id DECIMAL(15, 0),
  freight_term STRING,
  warehouse_id DECIMAL(15, 0),
  territory_id DECIMAL(15, 0),
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
  request_id DECIMAL(15, 0),
  program_application_id DECIMAL(15, 0),
  program_id DECIMAL(15, 0),
  program_update_date TIMESTAMP,
  tax_reference STRING,
  sort_priority DECIMAL(5, 0),
  tax_code STRING,
  attribute11 STRING,
  attribute12 STRING,
  attribute13 STRING,
  attribute14 STRING,
  attribute15 STRING,
  attribute16 STRING,
  attribute17 STRING,
  attribute18 STRING,
  attribute19 STRING,
  attribute20 STRING,
  attribute21 STRING,
  attribute22 STRING,
  attribute23 STRING,
  attribute24 STRING,
  attribute25 STRING,
  last_accrue_charge_date TIMESTAMP,
  second_last_accrue_charge_date TIMESTAMP,
  last_unaccrue_charge_date TIMESTAMP,
  second_last_unaccrue_chrg_date TIMESTAMP,
  demand_class_code STRING,
  org_id DECIMAL(15, 0),
  tax_header_level_flag STRING,
  tax_rounding_rule STRING,
  wh_update_date TIMESTAMP,
  global_attribute1 STRING,
  global_attribute2 STRING,
  global_attribute3 STRING,
  global_attribute4 STRING,
  global_attribute5 STRING,
  global_attribute6 STRING,
  global_attribute7 STRING,
  global_attribute8 STRING,
  global_attribute9 STRING,
  global_attribute10 STRING,
  global_attribute11 STRING,
  global_attribute12 STRING,
  global_attribute13 STRING,
  global_attribute14 STRING,
  global_attribute15 STRING,
  global_attribute16 STRING,
  global_attribute17 STRING,
  global_attribute18 STRING,
  global_attribute19 STRING,
  global_attribute20 STRING,
  global_attribute_category STRING,
  primary_salesrep_id DECIMAL(15, 0),
  finchrg_receivables_trx_id DECIMAL(15, 0),
  dates_negative_tolerance DECIMAL(15, 0),
  dates_positive_tolerance DECIMAL(15, 0),
  date_type_preference STRING,
  over_shipment_tolerance DECIMAL(15, 0),
  under_shipment_tolerance DECIMAL(15, 0),
  item_cross_ref_pref STRING,
  over_return_tolerance DECIMAL(15, 0),
  under_return_tolerance DECIMAL(15, 0),
  ship_sets_include_lines_flag STRING,
  arrivalsets_include_lines_flag STRING,
  sched_date_push_flag STRING,
  invoice_quantity_rule STRING,
  pricing_event STRING,
  gl_id_rec DECIMAL(15, 0),
  gl_id_rev DECIMAL(15, 0),
  gl_id_tax DECIMAL(15, 0),
  gl_id_freight DECIMAL(15, 0),
  gl_id_clearing DECIMAL(15, 0),
  gl_id_unbilled DECIMAL(15, 0),
  gl_id_unearned DECIMAL(15, 0),
  gl_id_unpaid_rec DECIMAL(15, 0),
  gl_id_remittance DECIMAL(15, 0),
  gl_id_factor DECIMAL(15, 0),
  tax_classification STRING,
  object_version_number DECIMAL(15, 0),
  created_by_module STRING,
  application_id DECIMAL(15, 0),
  KCA_OPERATION STRING,
  IS_DELETED_FLG STRING,
  kca_seq_id DECIMAL(36, 0),
  kca_seq_date TIMESTAMP
);
INSERT INTO silver_bec_ods.hz_cust_site_uses_all (
  site_use_id,
  cust_acct_site_id,
  last_update_date,
  last_updated_by,
  creation_date,
  created_by,
  site_use_code,
  primary_flag,
  status,
  `location`,
  last_update_login,
  contact_id,
  bill_to_site_use_id,
  orig_system_reference,
  sic_code,
  payment_term_id,
  gsa_indicator,
  ship_partial,
  ship_via,
  fob_point,
  order_type_id,
  price_list_id,
  freight_term,
  warehouse_id,
  territory_id,
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
  request_id,
  program_application_id,
  program_id,
  program_update_date,
  tax_reference,
  sort_priority,
  tax_code,
  attribute11,
  attribute12,
  attribute13,
  attribute14,
  attribute15,
  attribute16,
  attribute17,
  attribute18,
  attribute19,
  attribute20,
  attribute21,
  attribute22,
  attribute23,
  attribute24,
  attribute25,
  last_accrue_charge_date,
  second_last_accrue_charge_date,
  last_unaccrue_charge_date,
  second_last_unaccrue_chrg_date,
  demand_class_code,
  org_id,
  tax_header_level_flag,
  tax_rounding_rule,
  wh_update_date,
  global_attribute1,
  global_attribute2,
  global_attribute3,
  global_attribute4,
  global_attribute5,
  global_attribute6,
  global_attribute7,
  global_attribute8,
  global_attribute9,
  global_attribute10,
  global_attribute11,
  global_attribute12,
  global_attribute13,
  global_attribute14,
  global_attribute15,
  global_attribute16,
  global_attribute17,
  global_attribute18,
  global_attribute19,
  global_attribute20,
  global_attribute_category,
  primary_salesrep_id,
  finchrg_receivables_trx_id,
  dates_negative_tolerance,
  dates_positive_tolerance,
  date_type_preference,
  over_shipment_tolerance,
  under_shipment_tolerance,
  item_cross_ref_pref,
  over_return_tolerance,
  under_return_tolerance,
  ship_sets_include_lines_flag,
  arrivalsets_include_lines_flag,
  sched_date_push_flag,
  invoice_quantity_rule,
  pricing_event,
  gl_id_rec,
  gl_id_rev,
  gl_id_tax,
  gl_id_freight,
  gl_id_clearing,
  gl_id_unbilled,
  gl_id_unearned,
  gl_id_unpaid_rec,
  gl_id_remittance,
  gl_id_factor,
  tax_classification,
  object_version_number,
  created_by_module,
  application_id,
  kca_operation,
  is_deleted_flg,
  kca_seq_id,
  kca_seq_date
)
SELECT
  site_use_id,
  cust_acct_site_id,
  last_update_date,
  last_updated_by,
  creation_date,
  created_by,
  site_use_code,
  primary_flag,
  status,
  `location`,
  last_update_login,
  contact_id,
  bill_to_site_use_id,
  orig_system_reference,
  sic_code,
  payment_term_id,
  gsa_indicator,
  ship_partial,
  ship_via,
  fob_point,
  order_type_id,
  price_list_id,
  freight_term,
  warehouse_id,
  territory_id,
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
  request_id,
  program_application_id,
  program_id,
  program_update_date,
  tax_reference,
  sort_priority,
  tax_code,
  attribute11,
  attribute12,
  attribute13,
  attribute14,
  attribute15,
  attribute16,
  attribute17,
  attribute18,
  attribute19,
  attribute20,
  attribute21,
  attribute22,
  attribute23,
  attribute24,
  attribute25,
  last_accrue_charge_date,
  second_last_accrue_charge_date,
  last_unaccrue_charge_date,
  second_last_unaccrue_chrg_date,
  demand_class_code,
  org_id,
  tax_header_level_flag,
  tax_rounding_rule,
  wh_update_date,
  global_attribute1,
  global_attribute2,
  global_attribute3,
  global_attribute4,
  global_attribute5,
  global_attribute6,
  global_attribute7,
  global_attribute8,
  global_attribute9,
  global_attribute10,
  global_attribute11,
  global_attribute12,
  global_attribute13,
  global_attribute14,
  global_attribute15,
  global_attribute16,
  global_attribute17,
  global_attribute18,
  global_attribute19,
  global_attribute20,
  global_attribute_category,
  primary_salesrep_id,
  finchrg_receivables_trx_id,
  dates_negative_tolerance,
  dates_positive_tolerance,
  date_type_preference,
  over_shipment_tolerance,
  under_shipment_tolerance,
  item_cross_ref_pref,
  over_return_tolerance,
  under_return_tolerance,
  ship_sets_include_lines_flag,
  arrivalsets_include_lines_flag,
  sched_date_push_flag,
  invoice_quantity_rule,
  pricing_event,
  gl_id_rec,
  gl_id_rev,
  gl_id_tax,
  gl_id_freight,
  gl_id_clearing,
  gl_id_unbilled,
  gl_id_unearned,
  gl_id_unpaid_rec,
  gl_id_remittance,
  gl_id_factor,
  tax_classification,
  object_version_number,
  created_by_module,
  application_id,
  KCA_OPERATION,
  'N' AS IS_DELETED_FLG,
  CAST(NULLIF(KCA_SEQ_ID, '') AS DECIMAL(36, 0)) AS KCA_SEQ_ID,
  kca_seq_date
FROM bronze_bec_ods_stg.hz_cust_site_uses_all;
UPDATE bec_etl_ctrl.batch_ods_info SET load_type = 'I', last_refresh_date = CURRENT_TIMESTAMP()
WHERE
  ods_table_name = 'hz_cust_site_uses_all';