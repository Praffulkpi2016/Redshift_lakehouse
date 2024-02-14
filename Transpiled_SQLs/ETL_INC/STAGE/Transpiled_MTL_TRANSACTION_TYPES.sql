TRUNCATE table
	table bronze_bec_ods_stg.MTL_TRANSACTION_TYPES;
INSERT INTO bronze_bec_ods_stg.MTL_TRANSACTION_TYPES (
  transaction_type_id,
  last_update_date,
  last_updated_by,
  creation_date,
  created_by,
  transaction_type_name,
  description,
  transaction_action_id,
  transaction_source_type_id,
  disable_date,
  user_defined_flag,
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
  attribute_category,
  type_class,
  shortage_msg_background_flag,
  shortage_msg_online_flag,
  status_control_flag,
  location_required_flag,
  KCA_OPERATION,
  kca_seq_id,
  kca_seq_date
)
(
  SELECT
    transaction_type_id,
    last_update_date,
    last_updated_by,
    creation_date,
    created_by,
    transaction_type_name,
    description,
    transaction_action_id,
    transaction_source_type_id,
    disable_date,
    user_defined_flag,
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
    attribute_category,
    type_class,
    shortage_msg_background_flag,
    shortage_msg_online_flag,
    status_control_flag,
    location_required_flag,
    KCA_OPERATION,
    kca_seq_id,
    kca_seq_date
  FROM bec_raw_dl_ext.MTL_TRANSACTION_TYPES
  WHERE
    kca_operation <> 'DELETE'
    AND COALESCE(kca_seq_id, '') <> ''
    AND (COALESCE(TRANSACTION_TYPE_ID, 0), COALESCE(TRANSACTION_TYPE_NAME, 'NA'), kca_seq_id, last_update_date) IN (
      SELECT
        COALESCE(TRANSACTION_TYPE_ID, 0) AS TRANSACTION_TYPE_ID,
        COALESCE(TRANSACTION_TYPE_NAME, 'NA') AS TRANSACTION_TYPE_NAME,
        MAX(kca_seq_id),
        MAX(last_update_date)
      FROM bec_raw_dl_ext.MTL_TRANSACTION_TYPES
      WHERE
        kca_operation <> 'DELETE' AND COALESCE(kca_seq_id, '') <> ''
      GROUP BY
        COALESCE(TRANSACTION_TYPE_ID, 0),
        COALESCE(TRANSACTION_TYPE_NAME, 'NA')
    )
    AND (
      kca_seq_date > (
        SELECT
          (
            executebegints - prune_days
          )
        FROM bec_etl_ctrl.batch_ods_info
        WHERE
          ods_table_name = 'mtl_transaction_types'
      )
    )
);