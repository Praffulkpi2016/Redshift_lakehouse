/* Delete Records */
DELETE FROM silver_bec_ods.MTL_TRANSACTION_TYPES
WHERE
  (COALESCE(TRANSACTION_TYPE_ID, 0), COALESCE(TRANSACTION_TYPE_NAME, 'NA')) IN (
    SELECT
      COALESCE(stg.TRANSACTION_TYPE_ID, 0) AS TRANSACTION_TYPE_ID,
      COALESCE(stg.TRANSACTION_TYPE_NAME, 'NA') AS TRANSACTION_TYPE_NAME
    FROM silver_bec_ods.MTL_TRANSACTION_TYPES AS ods, bronze_bec_ods_stg.MTL_TRANSACTION_TYPES AS stg
    WHERE
      COALESCE(ods.TRANSACTION_TYPE_ID, 0) = COALESCE(stg.TRANSACTION_TYPE_ID, 0)
      AND COALESCE(ods.TRANSACTION_TYPE_NAME, 'NA') = COALESCE(stg.TRANSACTION_TYPE_NAME, 'NA')
      AND stg.kca_operation IN ('INSERT', 'UPDATE')
  );
/* Insert records */
INSERT INTO silver_bec_ods.MTL_TRANSACTION_TYPES (
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
  kca_operation,
  is_deleted_flg,
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
    kca_operation,
    'N' AS IS_DELETED_FLG,
    CAST(NULLIF(KCA_SEQ_ID, '') AS DECIMAL(36, 0)) AS KCA_SEQ_ID,
    kca_seq_date
  FROM bronze_bec_ods_stg.MTL_TRANSACTION_TYPES
  WHERE
    kca_operation IN ('INSERT', 'UPDATE')
    AND (COALESCE(TRANSACTION_TYPE_ID, 0), COALESCE(TRANSACTION_TYPE_NAME, 'NA'), kca_seq_id) IN (
      SELECT
        COALESCE(TRANSACTION_TYPE_ID, 0) AS TRANSACTION_TYPE_ID,
        COALESCE(TRANSACTION_TYPE_NAME, 'NA') AS TRANSACTION_TYPE_NAME,
        MAX(kca_seq_id)
      FROM bronze_bec_ods_stg.MTL_TRANSACTION_TYPES
      WHERE
        kca_operation IN ('INSERT', 'UPDATE')
      GROUP BY
        COALESCE(TRANSACTION_TYPE_ID, 0),
        COALESCE(TRANSACTION_TYPE_NAME, 'NA')
    )
);
/* Soft delete */
UPDATE silver_bec_ods.MTL_TRANSACTION_TYPES SET IS_DELETED_FLG = 'N';
UPDATE silver_bec_ods.MTL_TRANSACTION_TYPES SET IS_DELETED_FLG = 'Y'
WHERE
  (COALESCE(TRANSACTION_TYPE_ID, 0), COALESCE(TRANSACTION_TYPE_NAME, 'NA')) IN (
    SELECT
      COALESCE(TRANSACTION_TYPE_ID, 0),
      COALESCE(TRANSACTION_TYPE_NAME, 'NA')
    FROM bec_raw_dl_ext.MTL_TRANSACTION_TYPES
    WHERE
      (COALESCE(TRANSACTION_TYPE_ID, 0), COALESCE(TRANSACTION_TYPE_NAME, 'NA'), KCA_SEQ_ID) IN (
        SELECT
          COALESCE(TRANSACTION_TYPE_ID, 0),
          COALESCE(TRANSACTION_TYPE_NAME, 'NA'),
          MAX(KCA_SEQ_ID) AS KCA_SEQ_ID
        FROM bec_raw_dl_ext.MTL_TRANSACTION_TYPES
        GROUP BY
          COALESCE(TRANSACTION_TYPE_ID, 0),
          COALESCE(TRANSACTION_TYPE_NAME, 'NA')
      )
      AND kca_operation = 'DELETE'
  );
UPDATE bec_etl_ctrl.batch_ods_info SET last_refresh_date = CURRENT_TIMESTAMP()
WHERE
  ods_table_name = 'mtl_transaction_types';