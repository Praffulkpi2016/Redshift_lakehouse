DROP table IF EXISTS gold_bec_dwh.DIM_INV_TRANSACTION_TYPES;
CREATE TABLE gold_bec_dwh.DIM_INV_TRANSACTION_TYPES AS
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
    'N' AS is_deleted_flg, /* audit columns */
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
    ) || '-' || COALESCE(transaction_type_id, 0) || '-' || COALESCE(transaction_type_name, 'NA') AS dw_load_id,
    CURRENT_TIMESTAMP() AS dw_insert_date,
    CURRENT_TIMESTAMP() AS dw_update_date
  FROM silver_bec_ods.mtl_transaction_types
);
UPDATE bec_etl_ctrl.batch_dw_info SET load_type = 'I', last_refresh_date = CURRENT_TIMESTAMP()
WHERE
  dw_table_name = 'dim_inv_transaction_types' AND batch_name = 'inv';