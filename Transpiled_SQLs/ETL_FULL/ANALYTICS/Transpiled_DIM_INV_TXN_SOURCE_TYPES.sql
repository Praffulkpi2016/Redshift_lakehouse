DROP table IF EXISTS gold_bec_dwh.DIM_INV_TXN_SOURCE_TYPES;
CREATE TABLE gold_bec_dwh.DIM_INV_TXN_SOURCE_TYPES AS
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
    'N' AS is_deleted_flg,
    (
      SELECT
        system_id
      FROM bec_etl_ctrl.etlsourceappid
      WHERE
        source_system = 'EBS'
    ) AS source_app_id,
    (
      SELECT
        system_id
      FROM bec_etl_ctrl.etlsourceappid
      WHERE
        source_system = 'EBS'
    ) || '-' || COALESCE(TRANSACTION_SOURCE_TYPE_ID, 0) AS dw_load_id,
    CURRENT_TIMESTAMP() AS dw_insert_date,
    CURRENT_TIMESTAMP() AS dw_update_date
  FROM silver_bec_ods.mtl_txn_source_types
);
UPDATE bec_etl_ctrl.batch_dw_info SET load_type = 'I', last_refresh_date = CURRENT_TIMESTAMP()
WHERE
  dw_table_name = 'dim_inv_txn_source_types' AND batch_name = 'inv';