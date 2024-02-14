TRUNCATE table silver_bec_ods.bom_item_current_rev_view;
INSERT INTO silver_bec_ods.bom_item_current_rev_view (
  ORGANIZATION_ID,
  INVENTORY_ITEM_ID,
  CURRENT_REVISION,
  EFFECTIVITY_DATE,
  REVISION_LABEL,
  REVISION_ID,
  kca_operation,
  IS_DELETED_FLG,
  kca_seq_id,
  kca_seq_date
)
SELECT
  ORGANIZATION_ID,
  INVENTORY_ITEM_ID,
  CURRENT_REVISION,
  EFFECTIVITY_DATE,
  REVISION_LABEL,
  REVISION_ID,
  kca_operation,
  'N' AS IS_DELETED_FLG,
  CAST(NULLIF(KCA_SEQ_ID, '') AS DECIMAL(36, 0)) AS KCA_SEQ_ID,
  kca_seq_date
FROM bronze_bec_ods_stg.bom_item_current_rev_view;
UPDATE bec_etl_ctrl.batch_ods_info SET load_type = 'I', last_refresh_date = CURRENT_TIMESTAMP()
WHERE
  ods_table_name = 'bom_item_current_rev_view';