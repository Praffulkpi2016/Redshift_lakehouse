TRUNCATE table silver_bec_ods.MSC_VMI_ITEM_V;
INSERT INTO silver_bec_ods.MSC_VMI_ITEM_V
SELECT
  PLAN_ID,
  SR_INSTANCE_ID,
  ORGANIZATION_ID,
  INVENTORY_ITEM_ID,
  SUPPLIER_ID,
  SUPPLIER_SITE_ID,
  ITEM_NAME,
  DESCRIPTION,
  PLANNER_CODE,
  BUYER_CODE,
  'N' AS IS_DELETED_FLG,
  kca_operation,
  CAST(NULLIF(kca_seq_id, '') AS DECIMAL(36, 0)) AS kca_seq_id,
  kca_seq_date
FROM bronze_bec_ods_stg.MSC_VMI_ITEM_V;
UPDATE bec_etl_ctrl.batch_ods_info SET load_type = 'I', last_refresh_date = CURRENT_TIMESTAMP()
WHERE
  ods_table_name = 'msc_vmi_item_v';