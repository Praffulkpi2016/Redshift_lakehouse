/* Delete Records */
DELETE FROM gold_bec_dwh.DIM_INV_TXN_SOURCE_TYPES
WHERE
  (
    COALESCE(TRANSACTION_SOURCE_TYPE_ID, 0)
  ) IN (
    SELECT
      COALESCE(ods.TRANSACTION_SOURCE_TYPE_ID, 0)
    FROM gold_bec_dwh.DIM_INV_TXN_SOURCE_TYPES AS dw, silver_bec_ods.MTL_TXN_SOURCE_TYPES AS ods
    WHERE
      dw.dw_load_id = (
        SELECT
          system_id
        FROM bec_etl_ctrl.etlsourceappid
        WHERE
          source_system = 'EBS'
      ) || '-' || COALESCE(ods.TRANSACTION_SOURCE_TYPE_ID, 0)
      AND (
        ods.kca_seq_date > (
          SELECT
            (
              executebegints - prune_days
            )
          FROM bec_etl_ctrl.batch_dw_info
          WHERE
            dw_table_name = 'dim_inv_txn_source_types' AND batch_name = 'inv'
        )
      )
  );
/* Insert records */
INSERT INTO gold_bec_dwh.DIM_INV_TXN_SOURCE_TYPES (
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
  is_deleted_flg,
  source_app_id,
  dw_load_id,
  dw_insert_date,
  dw_update_date
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
          dw_table_name = 'dim_inv_txn_source_types' AND batch_name = 'inv'
      )
    )
);
/* Soft delete */
UPDATE gold_bec_dwh.DIM_INV_TXN_SOURCE_TYPES SET is_deleted_flg = 'Y'
WHERE
  NOT (
    COALESCE(TRANSACTION_SOURCE_TYPE_ID, 0)
  ) IN (
    SELECT
      COALESCE(ods.TRANSACTION_SOURCE_TYPE_ID, 0) AS TRANSACTION_SOURCE_TYPE_ID
    FROM gold_bec_dwh.DIM_INV_TXN_SOURCE_TYPES AS dw, silver_bec_ods.MTL_TXN_SOURCE_TYPES AS ods
    WHERE
      dw.dw_load_id = (
        SELECT
          system_id
        FROM bec_etl_ctrl.etlsourceappid
        WHERE
          source_system = 'EBS'
      ) || '-' || COALESCE(ods.TRANSACTION_SOURCE_TYPE_ID, 0)
      AND ods.is_deleted_flg <> 'Y'
  );
UPDATE bec_etl_ctrl.batch_dw_info SET last_refresh_date = CURRENT_TIMESTAMP()
WHERE
  dw_table_name = 'dim_inv_txn_source_types' AND batch_name = 'inv';