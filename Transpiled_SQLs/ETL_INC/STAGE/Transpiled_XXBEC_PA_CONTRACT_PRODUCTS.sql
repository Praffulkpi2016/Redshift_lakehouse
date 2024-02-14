TRUNCATE table
	table bronze_bec_ods_stg.XXBEC_PA_CONTRACT_PRODUCTS;
INSERT INTO bronze_bec_ods_stg.XXBEC_PA_CONTRACT_PRODUCTS (
  product_id,
  product_name,
  product_rating,
  creation_date,
  created_by,
  last_update_date,
  last_updated_by,
  inventory_item_id,
  KCA_OPERATION,
  KCA_SEQ_ID,
  kca_seq_date
)
(
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
    KCA_SEQ_ID,
    kca_seq_date
  FROM bec_raw_dl_ext.XXBEC_PA_CONTRACT_PRODUCTS
  WHERE
    kca_operation <> 'DELETE'
    AND COALESCE(kca_seq_id, '') <> ''
    AND (COALESCE(PRODUCT_ID, 0), COALESCE(INVENTORY_ITEM_ID, 0), KCA_SEQ_ID) IN (
      SELECT
        COALESCE(PRODUCT_ID, 0) AS PRODUCT_ID,
        COALESCE(INVENTORY_ITEM_ID, 0) AS INVENTORY_ITEM_ID,
        MAX(KCA_SEQ_ID)
      FROM bec_raw_dl_ext.XXBEC_PA_CONTRACT_PRODUCTS
      WHERE
        kca_operation <> 'DELETE' AND COALESCE(kca_seq_id, '') <> ''
      GROUP BY
        COALESCE(PRODUCT_ID, 0),
        COALESCE(INVENTORY_ITEM_ID, 0)
    )
    AND kca_seq_date > (
      SELECT
        (
          executebegints - prune_days
        )
      FROM bec_etl_ctrl.batch_ods_info
      WHERE
        ods_table_name = 'xxbec_pa_contract_products'
    )
);