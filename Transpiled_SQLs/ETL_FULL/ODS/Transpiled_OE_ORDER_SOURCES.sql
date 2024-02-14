DROP TABLE IF EXISTS silver_bec_ods.OE_ORDER_SOURCES;
CREATE TABLE IF NOT EXISTS silver_bec_ods.OE_ORDER_SOURCES (
  order_source_id DECIMAL(15, 0),
  creation_date TIMESTAMP,
  created_by DECIMAL(15, 0),
  last_update_date TIMESTAMP,
  last_updated_by DECIMAL(15, 0),
  last_update_login DECIMAL(15, 0),
  name STRING,
  description STRING,
  enabled_flag STRING,
  create_customers_flag STRING,
  use_ids_flag STRING,
  aia_enabled_flag STRING,
  zd_edition_name STRING,
  KCA_OPERATION STRING,
  IS_DELETED_FLG STRING,
  kca_seq_id DECIMAL(36, 0),
  kca_seq_date TIMESTAMP
);
INSERT INTO silver_bec_ods.OE_ORDER_SOURCES (
  order_source_id,
  creation_date,
  created_by,
  last_update_date,
  last_updated_by,
  last_update_login,
  `name`,
  description,
  enabled_flag,
  create_customers_flag,
  use_ids_flag,
  aia_enabled_flag,
  zd_edition_name,
  kca_operation,
  IS_DELETED_FLG,
  kca_seq_id,
  kca_seq_date
)
SELECT
  order_source_id,
  creation_date,
  created_by,
  last_update_date,
  last_updated_by,
  last_update_login,
  `name`,
  description,
  enabled_flag,
  create_customers_flag,
  use_ids_flag,
  aia_enabled_flag,
  zd_edition_name,
  KCA_OPERATION,
  'N' AS IS_DELETED_FLG,
  CAST(NULLIF(KCA_SEQ_ID, '') AS DECIMAL(36, 0)) AS KCA_SEQ_ID,
  kca_seq_date
FROM bronze_bec_ods_stg.OE_ORDER_SOURCES;
UPDATE bec_etl_ctrl.batch_ods_info SET load_type = 'I', last_refresh_date = CURRENT_TIMESTAMP()
WHERE
  ods_table_name = 'oe_order_sources';