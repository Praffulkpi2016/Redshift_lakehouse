TRUNCATE table
	table bronze_bec_ods_stg.mtl_item_categories;
INSERT INTO bronze_bec_ods_stg.mtl_item_categories (
  INVENTORY_ITEM_ID,
  ORGANIZATION_ID,
  CATEGORY_SET_ID,
  CATEGORY_ID,
  LAST_UPDATE_DATE,
  LAST_UPDATED_BY,
  CREATION_DATE,
  CREATED_BY,
  LAST_UPDATE_LOGIN,
  REQUEST_ID,
  PROGRAM_APPLICATION_ID,
  PROGRAM_ID,
  PROGRAM_UPDATE_DATE,
  WH_UPDATE_DATE,
  KCA_OPERATION,
  kca_seq_id,
  kca_seq_date
)
(
  SELECT
    INVENTORY_ITEM_ID,
    ORGANIZATION_ID,
    CATEGORY_SET_ID,
    CATEGORY_ID,
    LAST_UPDATE_DATE,
    LAST_UPDATED_BY,
    CREATION_DATE,
    CREATED_BY,
    LAST_UPDATE_LOGIN,
    REQUEST_ID,
    PROGRAM_APPLICATION_ID,
    PROGRAM_ID,
    PROGRAM_UPDATE_DATE,
    WH_UPDATE_DATE,
    KCA_OPERATION,
    kca_seq_id,
    kca_seq_date
  FROM bec_raw_dl_ext.mtl_item_categories
  WHERE
    kca_operation <> 'DELETE'
    AND COALESCE(kca_seq_id, '') <> ''
    AND (INVENTORY_ITEM_ID, ORGANIZATION_ID, CATEGORY_SET_ID, kca_seq_id) IN (
      SELECT
        INVENTORY_ITEM_ID,
        ORGANIZATION_ID,
        CATEGORY_SET_ID,
        MAX(kca_seq_id)
      FROM bec_raw_dl_ext.mtl_item_categories
      WHERE
        kca_operation <> 'DELETE' AND COALESCE(kca_seq_id, '') <> ''
      GROUP BY
        INVENTORY_ITEM_ID,
        ORGANIZATION_ID,
        CATEGORY_SET_ID
    )
    AND (
      kca_seq_date > (
        SELECT
          (
            executebegints - prune_days
          )
        FROM bec_etl_ctrl.batch_ods_info
        WHERE
          ods_table_name = 'mtl_item_categories'
      )
    )
);