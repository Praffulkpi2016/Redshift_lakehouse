DROP TABLE IF EXISTS silver_bec_ods.FA_ASSET_HISTORY;
CREATE TABLE IF NOT EXISTS silver_bec_ods.FA_ASSET_HISTORY (
  asset_id DECIMAL(15, 0),
  category_id DECIMAL(15, 0),
  asset_type STRING,
  units DECIMAL(15, 0),
  date_effective TIMESTAMP,
  date_ineffective TIMESTAMP,
  transaction_header_id_in DECIMAL(15, 0),
  transaction_header_id_out DECIMAL(15, 0),
  last_update_date TIMESTAMP,
  last_updated_by DECIMAL(15, 0),
  last_update_login DECIMAL(15, 0),
  KCA_OPERATION STRING,
  IS_DELETED_FLG STRING,
  kca_seq_id DECIMAL(36, 0),
  kca_seq_date TIMESTAMP
);
INSERT INTO silver_bec_ods.FA_ASSET_HISTORY (
  asset_id,
  category_id,
  asset_type,
  units,
  date_effective,
  date_ineffective,
  transaction_header_id_in,
  transaction_header_id_out,
  last_update_date,
  last_updated_by,
  last_update_login,
  kca_operation,
  IS_DELETED_FLG,
  kca_seq_id,
  kca_seq_date
)
SELECT
  asset_id,
  category_id,
  asset_type,
  units,
  date_effective,
  date_ineffective,
  transaction_header_id_in,
  transaction_header_id_out,
  last_update_date,
  last_updated_by,
  last_update_login,
  KCA_OPERATION,
  'N' AS IS_DELETED_FLG,
  CAST(NULLIF(KCA_SEQ_ID, '') AS DECIMAL(36, 0)) AS KCA_SEQ_ID,
  kca_seq_date
FROM bronze_bec_ods_stg.FA_ASSET_HISTORY;
UPDATE bec_etl_ctrl.batch_ods_info SET load_type = 'I', last_refresh_date = CURRENT_TIMESTAMP()
WHERE
  ods_table_name = 'fa_asset_history';