DROP TABLE IF EXISTS silver_bec_ods.CST_ORGANIZATION_DEFINITIONS;
CREATE TABLE IF NOT EXISTS silver_bec_ods.CST_ORGANIZATION_DEFINITIONS (
  organization_id DECIMAL(15, 0),
  business_group_id DECIMAL(15, 0),
  user_definition_enable_date TIMESTAMP,
  disable_date TIMESTAMP,
  organization_code STRING,
  organization_name STRING,
  set_of_books_id DECIMAL(15, 0),
  chart_of_accounts_id DECIMAL(15, 0),
  currency_code STRING,
  operating_unit DECIMAL(15, 0),
  legal_entity DECIMAL(15, 0),
  kca_operation STRING,
  is_deleted_flg STRING,
  kca_seq_id DECIMAL(36, 0),
  kca_seq_date TIMESTAMP
);
INSERT INTO silver_bec_ods.CST_ORGANIZATION_DEFINITIONS (
  organization_id,
  business_group_id,
  user_definition_enable_date,
  disable_date,
  organization_code,
  organization_name,
  set_of_books_id,
  chart_of_accounts_id,
  currency_code,
  operating_unit,
  legal_entity,
  kca_operation,
  is_deleted_flg,
  kca_seq_id,
  kca_seq_date
)
SELECT
  organization_id,
  business_group_id,
  user_definition_enable_date,
  disable_date,
  organization_code,
  organization_name,
  set_of_books_id,
  chart_of_accounts_id,
  currency_code,
  operating_unit,
  legal_entity,
  kca_operation,
  'N' AS IS_DELETED_FLG,
  CAST(NULLIF(KCA_SEQ_ID, '') AS DECIMAL(36, 0)) AS KCA_SEQ_ID,
  kca_seq_date
FROM bronze_bec_ods_stg.CST_ORGANIZATION_DEFINITIONS;
UPDATE bec_etl_ctrl.batch_ods_info SET load_type = 'I', last_refresh_date = CURRENT_TIMESTAMP()
WHERE
  ods_table_name = 'cst_organization_definitions';