TRUNCATE table silver_bec_ods.ORG_ORGANIZATION_DEFINITIONS;
INSERT INTO silver_bec_ods.ORG_ORGANIZATION_DEFINITIONS (
  organization_id,
  business_group_id,
  user_definition_enable_date,
  disable_date,
  organization_code,
  organization_name,
  set_of_books_id,
  chart_of_accounts_id,
  inventory_enabled_flag,
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
  inventory_enabled_flag,
  operating_unit,
  legal_entity,
  kca_operation,
  'N' AS IS_DELETED_FLG,
  CAST(NULLIF(KCA_SEQ_ID, '') AS DECIMAL(36, 0)) AS KCA_SEQ_ID,
  KCA_SEQ_DATE
FROM bronze_bec_ods_stg.ORG_ORGANIZATION_DEFINITIONS;
UPDATE bec_etl_ctrl.batch_ods_info SET load_type = 'I', last_refresh_date = CURRENT_TIMESTAMP()
WHERE
  ods_table_name = 'org_organization_definitions';