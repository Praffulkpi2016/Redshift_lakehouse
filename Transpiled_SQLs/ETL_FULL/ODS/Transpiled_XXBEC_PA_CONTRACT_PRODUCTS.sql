DROP TABLE IF EXISTS silver_bec_ods.XXBEC_PA_CONTRACT_PRODUCTS;
CREATE TABLE IF NOT EXISTS silver_bec_ods.XXBEC_PA_CONTRACT_PRODUCTS (
  product_id DECIMAL(15, 0),
  product_name STRING,
  product_rating DECIMAL(28, 10),
  creation_date TIMESTAMP,
  created_by DECIMAL(15, 0),
  last_update_date TIMESTAMP,
  last_updated_by DECIMAL(15, 0),
  inventory_item_id DECIMAL(15, 0),
  KCA_OPERATION STRING,
  IS_DELETED_FLG STRING,
  kca_seq_id DECIMAL(36, 0),
  kca_seq_date TIMESTAMP
);
INSERT INTO silver_bec_ods.XXBEC_PA_CONTRACT_PRODUCTS (
  product_id,
  product_name,
  product_rating,
  creation_date,
  created_by,
  last_update_date,
  last_updated_by,
  inventory_item_id,
  kca_operation,
  IS_DELETED_FLG,
  kca_seq_id,
  kca_seq_date
)
SELECT
  product_id,
  product_name,
  product_rating,
  creation_date,
  created_by,
  last_update_date,
  last_updated_by,
  inventory_item_id,
  KCA_OPERATION,
  'N' AS IS_DELETED_FLG,
  CAST(NULLIF(KCA_SEQ_ID, '') AS DECIMAL(36, 0)) AS KCA_SEQ_ID,
  kca_seq_date
FROM bronze_bec_ods_stg.XXBEC_PA_CONTRACT_PRODUCTS;
UPDATE bec_etl_ctrl.batch_ods_info SET load_type = 'I', last_refresh_date = CURRENT_TIMESTAMP()
WHERE
  ods_table_name = 'xxbec_pa_contract_products';