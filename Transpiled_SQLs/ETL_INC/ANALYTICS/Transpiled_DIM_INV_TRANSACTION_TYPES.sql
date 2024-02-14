/* Delete Records */
DELETE FROM gold_bec_dwh.DIM_INV_TRANSACTION_TYPES
WHERE
  (COALESCE(transaction_type_id, 0), COALESCE(transaction_type_name, 'NA')) IN (
    SELECT
      COALESCE(ods.transaction_type_id, 0),
      COALESCE(ods.transaction_type_name, 'NA')
    FROM gold_bec_dwh.DIM_INV_TRANSACTION_TYPES AS dw, silver_bec_ods.mtl_transaction_types AS ods
    WHERE
      dw.dw_load_id = (
        SELECT
          system_id
        FROM bec_etl_ctrl.etlsourceappid
        WHERE
          source_system = 'EBS'
      ) || '-' || COALESCE(ods.transaction_type_id, 0) || '-' || COALESCE(ods.transaction_type_name, 'NA')
      AND (
        ods.kca_seq_date > (
          SELECT
            (
              executebegints - prune_days
            )
          FROM bec_etl_ctrl.batch_dw_info
          WHERE
            dw_table_name = 'dim_inv_transaction_types' AND batch_name = 'inv'
        )
      )
  );
/* Insert records */
INSERT INTO gold_bec_dwh.DIM_INV_TRANSACTION_TYPES (
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
  is_deleted_flg,
  source_app_id,
  dw_load_id,
  dw_insert_date,
  dw_update_date
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
  WHERE
    1 = 1
    AND (
      kca_seq_date > (
        SELECT
          (
            executebegints - prune_days
          )
        FROM bec_etl_ctrl.batch_dw_info
        WHERE
          dw_table_name = 'dim_inv_transaction_types' AND batch_name = 'inv'
      )
    )
);
/* Soft delete */
UPDATE gold_bec_dwh.DIM_INV_TRANSACTION_TYPES SET is_deleted_flg = 'Y'
WHERE
  NOT (COALESCE(transaction_type_id, 0), COALESCE(transaction_type_name, 'NA')) IN (
    SELECT
      COALESCE(ods.transaction_type_id, 0),
      COALESCE(ods.transaction_type_name, 'NA')
    FROM gold_bec_dwh.DIM_INV_TRANSACTION_TYPES AS dw, (
      SELECT
        *
      FROM silver_bec_ods.mtl_transaction_types
      WHERE
        is_deleted_flg <> 'Y'
    ) AS ods
    WHERE
      dw.dw_load_id = (
        SELECT
          system_id
        FROM bec_etl_ctrl.etlsourceappid
        WHERE
          source_system = 'EBS'
      ) || '-' || COALESCE(ods.transaction_type_id, 0) || '-' || COALESCE(ods.transaction_type_name, 'NA')
  );
UPDATE bec_etl_ctrl.batch_dw_info SET last_refresh_date = CURRENT_TIMESTAMP()
WHERE
  dw_table_name = 'dim_inv_transaction_types' AND batch_name = 'inv';