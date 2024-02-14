TRUNCATE table
	table bronze_bec_ods_stg.MTL_TXN_SOURCE_TYPES;
INSERT INTO bronze_bec_ods_stg.MTL_TXN_SOURCE_TYPES (
  transaction_source_type_id,
  last_update_date,
  last_updated_by,
  creation_date,
  created_by,
  transaction_source_type_name,
  description,
  disable_date,
  user_defined_flag,
  validated_flag,
  descriptive_flex_context_code,
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
  request_id,
  program_application_id,
  program_id,
  program_update_date,
  transaction_source_category,
  transaction_source,
  KCA_OPERATION,
  kca_seq_id,
  kca_seq_date
)
(
  SELECT
    transaction_source_type_id,
    last_update_date,
    last_updated_by,
    creation_date,
    created_by,
    transaction_source_type_name,
    description,
    disable_date,
    user_defined_flag,
    validated_flag,
    descriptive_flex_context_code,
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
    request_id,
    program_application_id,
    program_id,
    program_update_date,
    transaction_source_category,
    transaction_source,
    KCA_OPERATION,
    kca_seq_id,
    kca_seq_date
  FROM bec_raw_dl_ext.MTL_TXN_SOURCE_TYPES
  WHERE
    kca_operation <> 'DELETE'
    AND COALESCE(kca_seq_id, '') <> ''
    AND (TRANSACTION_SOURCE_TYPE_ID, kca_seq_id) IN (
      SELECT
        TRANSACTION_SOURCE_TYPE_ID,
        MAX(kca_seq_id)
      FROM bec_raw_dl_ext.MTL_TXN_SOURCE_TYPES
      WHERE
        kca_operation <> 'DELETE' AND COALESCE(kca_seq_id, '') <> ''
      GROUP BY
        TRANSACTION_SOURCE_TYPE_ID
    )
    AND (
      kca_seq_date > (
        SELECT
          (
            executebegints - prune_days
          )
        FROM bec_etl_ctrl.batch_ods_info
        WHERE
          ods_table_name = 'mtl_txn_source_types'
      )
    )
);