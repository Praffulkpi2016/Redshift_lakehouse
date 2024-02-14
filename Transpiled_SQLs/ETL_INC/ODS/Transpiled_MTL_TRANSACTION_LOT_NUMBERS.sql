TRUNCATE table silver_bec_ods.MTL_TRANSACTION_LOT_NUMBERS;
INSERT INTO silver_bec_ods.MTL_TRANSACTION_LOT_NUMBERS (
  transaction_id,
  last_update_date,
  last_updated_by,
  creation_date,
  created_by,
  last_update_login,
  inventory_item_id,
  organization_id,
  transaction_date,
  transaction_source_id,
  transaction_source_type_id,
  transaction_source_name,
  transaction_quantity,
  primary_quantity,
  lot_number,
  serial_transaction_id,
  description,
  vendor_name,
  supplier_lot_number,
  origination_date,
  date_code,
  grade_code,
  change_date,
  maturity_date,
  status_id,
  retest_date,
  age,
  item_size,
  color,
  volume,
  volume_uom,
  place_of_origin,
  best_by_date,
  length,
  length_uom,
  width,
  width_uom,
  recycled_content,
  thickness,
  thickness_uom,
  curl_wrinkle_fold,
  lot_attribute_category,
  c_attribute1,
  c_attribute2,
  c_attribute3,
  c_attribute4,
  c_attribute5,
  c_attribute6,
  c_attribute7,
  c_attribute8,
  c_attribute9,
  c_attribute10,
  c_attribute11,
  c_attribute12,
  c_attribute13,
  c_attribute14,
  c_attribute15,
  c_attribute16,
  c_attribute17,
  c_attribute18,
  c_attribute19,
  c_attribute20,
  d_attribute1,
  d_attribute2,
  d_attribute3,
  d_attribute4,
  d_attribute5,
  d_attribute6,
  d_attribute7,
  d_attribute8,
  d_attribute9,
  d_attribute10,
  n_attribute1,
  n_attribute2,
  n_attribute3,
  n_attribute4,
  n_attribute5,
  n_attribute6,
  n_attribute7,
  n_attribute8,
  n_attribute9,
  n_attribute10,
  vendor_id,
  territory_code,
  product_code,
  product_transaction_id,
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
  expiration_action_code,
  expiration_action_date,
  hold_date,
  origination_type,
  parent_lot_number,
  reason_id,
  secondary_transaction_quantity,
  parent_object_type,
  parent_object_id,
  parent_object_number,
  parent_item_id,
  parent_object_type2,
  parent_object_id2,
  parent_object_number2,
  c_attribute21,
  c_attribute22,
  c_attribute23,
  c_attribute24,
  c_attribute25,
  c_attribute26,
  c_attribute27,
  c_attribute28,
  c_attribute29,
  c_attribute30,
  d_attribute11,
  d_attribute12,
  d_attribute13,
  d_attribute14,
  d_attribute15,
  d_attribute16,
  d_attribute17,
  d_attribute18,
  d_attribute19,
  d_attribute20,
  n_attribute11,
  n_attribute12,
  n_attribute13,
  n_attribute14,
  n_attribute15,
  n_attribute16,
  n_attribute17,
  n_attribute18,
  n_attribute19,
  n_attribute20,
  n_attribute21,
  n_attribute22,
  n_attribute23,
  n_attribute24,
  n_attribute25,
  n_attribute26,
  n_attribute27,
  n_attribute28,
  n_attribute29,
  n_attribute30,
  kill_date,
  country_of_origin,
  KCA_OPERATION,
  IS_DELETED_FLG,
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
    transaction_date,
    transaction_source_id,
    transaction_source_type_id,
    transaction_source_name,
    transaction_quantity,
    primary_quantity,
    lot_number,
    serial_transaction_id,
    description,
    vendor_name,
    supplier_lot_number,
    origination_date,
    date_code,
    grade_code,
    change_date,
    maturity_date,
    status_id,
    retest_date,
    age,
    item_size,
    color,
    volume,
    volume_uom,
    place_of_origin,
    best_by_date,
    length,
    length_uom,
    width,
    width_uom,
    recycled_content,
    thickness,
    thickness_uom,
    curl_wrinkle_fold,
    lot_attribute_category,
    c_attribute1,
    c_attribute2,
    c_attribute3,
    c_attribute4,
    c_attribute5,
    c_attribute6,
    c_attribute7,
    c_attribute8,
    c_attribute9,
    c_attribute10,
    c_attribute11,
    c_attribute12,
    c_attribute13,
    c_attribute14,
    c_attribute15,
    c_attribute16,
    c_attribute17,
    c_attribute18,
    c_attribute19,
    c_attribute20,
    d_attribute1,
    d_attribute2,
    d_attribute3,
    d_attribute4,
    d_attribute5,
    d_attribute6,
    d_attribute7,
    d_attribute8,
    d_attribute9,
    d_attribute10,
    n_attribute1,
    n_attribute2,
    n_attribute3,
    n_attribute4,
    n_attribute5,
    n_attribute6,
    n_attribute7,
    n_attribute8,
    n_attribute9,
    n_attribute10,
    vendor_id,
    territory_code,
    product_code,
    product_transaction_id,
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
    expiration_action_code,
    expiration_action_date,
    hold_date,
    origination_type,
    parent_lot_number,
    reason_id,
    secondary_transaction_quantity,
    parent_object_type,
    parent_object_id,
    parent_object_number,
    parent_item_id,
    parent_object_type2,
    parent_object_id2,
    parent_object_number2,
    c_attribute21,
    c_attribute22,
    c_attribute23,
    c_attribute24,
    c_attribute25,
    c_attribute26,
    c_attribute27,
    c_attribute28,
    c_attribute29,
    c_attribute30,
    d_attribute11,
    d_attribute12,
    d_attribute13,
    d_attribute14,
    d_attribute15,
    d_attribute16,
    d_attribute17,
    d_attribute18,
    d_attribute19,
    d_attribute20,
    n_attribute11,
    n_attribute12,
    n_attribute13,
    n_attribute14,
    n_attribute15,
    n_attribute16,
    n_attribute17,
    n_attribute18,
    n_attribute19,
    n_attribute20,
    n_attribute21,
    n_attribute22,
    n_attribute23,
    n_attribute24,
    n_attribute25,
    n_attribute26,
    n_attribute27,
    n_attribute28,
    n_attribute29,
    n_attribute30,
    kill_date,
    country_of_origin,
    KCA_OPERATION,
    'N' AS IS_DELETED_FLG,
    CAST(NULLIF(KCA_SEQ_ID, '') AS DECIMAL(36, 0)) AS KCA_SEQ_ID,
    kca_seq_date
  FROM bronze_bec_ods_stg.MTL_TRANSACTION_LOT_NUMBERS
);
UPDATE bec_etl_ctrl.batch_ods_info SET load_type = 'I', last_refresh_date = CURRENT_TIMESTAMP()
WHERE
  ods_table_name = 'mtl_transaction_lot_numbers';