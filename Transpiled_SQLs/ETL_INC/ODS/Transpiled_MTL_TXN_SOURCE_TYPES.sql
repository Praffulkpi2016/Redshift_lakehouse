/* Delete Records */
DELETE FROM silver_bec_ods.MTL_TXN_SOURCE_TYPES
WHERE
  TRANSACTION_SOURCE_TYPE_ID IN (
    SELECT
      stg.TRANSACTION_SOURCE_TYPE_ID
    FROM silver_bec_ods.MTL_TXN_SOURCE_TYPES AS ods, bronze_bec_ods_stg.MTL_TXN_SOURCE_TYPES AS stg
    WHERE
      ods.TRANSACTION_SOURCE_TYPE_ID = stg.TRANSACTION_SOURCE_TYPE_ID
      AND stg.kca_operation IN ('INSERT', 'UPDATE')
  );
/* Insert records */
INSERT INTO silver_bec_ods.MTL_TXN_SOURCE_TYPES (
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
  kca_operation,
  is_deleted_flg,
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
    kca_operation,
    'N' AS IS_DELETED_FLG,
    CAST(NULLIF(KCA_SEQ_ID, '') AS DECIMAL(36, 0)) AS KCA_SEQ_ID,
    kca_seq_date
  FROM bronze_bec_ods_stg.MTL_TXN_SOURCE_TYPES
  WHERE
    kca_operation IN ('INSERT', 'UPDATE')
    AND (TRANSACTION_SOURCE_TYPE_ID, kca_seq_id) IN (
      SELECT
        TRANSACTION_SOURCE_TYPE_ID,
        MAX(kca_seq_id)
      FROM bronze_bec_ods_stg.MTL_TXN_SOURCE_TYPES
      WHERE
        kca_operation IN ('INSERT', 'UPDATE')
      GROUP BY
        TRANSACTION_SOURCE_TYPE_ID
    )
);
/* Soft delete */
UPDATE silver_bec_ods.MTL_TXN_SOURCE_TYPES SET IS_DELETED_FLG = 'N';
UPDATE silver_bec_ods.MTL_TXN_SOURCE_TYPES SET IS_DELETED_FLG = 'Y'
WHERE
  (
    TRANSACTION_SOURCE_TYPE_ID
  ) IN (
    SELECT
      TRANSACTION_SOURCE_TYPE_ID
    FROM bec_raw_dl_ext.MTL_TXN_SOURCE_TYPES
    WHERE
      (TRANSACTION_SOURCE_TYPE_ID, KCA_SEQ_ID) IN (
        SELECT
          TRANSACTION_SOURCE_TYPE_ID,
          MAX(KCA_SEQ_ID) AS KCA_SEQ_ID
        FROM bec_raw_dl_ext.MTL_TXN_SOURCE_TYPES
        GROUP BY
          TRANSACTION_SOURCE_TYPE_ID
      )
      AND kca_operation = 'DELETE'
  );
UPDATE bec_etl_ctrl.batch_ods_info SET last_refresh_date = CURRENT_TIMESTAMP()
WHERE
  ods_table_name = 'mtl_txn_source_types';