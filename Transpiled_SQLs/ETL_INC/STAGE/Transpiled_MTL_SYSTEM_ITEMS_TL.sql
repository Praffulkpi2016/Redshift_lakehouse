TRUNCATE table bronze_bec_ods_stg.MTL_SYSTEM_ITEMS_TL;
INSERT INTO bronze_bec_ods_stg.MTL_SYSTEM_ITEMS_TL (
  inventory_item_id,
  organization_id,
  `language`,
  source_lang,
  description,
  last_update_date,
  last_updated_by,
  creation_date,
  created_by,
  last_update_login,
  long_description,
  KCA_OPERATION,
  KCA_SEQ_ID,
  kca_seq_date
)
(
  SELECT
    inventory_item_id,
    organization_id,
    `language`,
    source_lang,
    description,
    last_update_date,
    last_updated_by,
    creation_date,
    created_by,
    last_update_login,
    long_description,
    KCA_OPERATION,
    KCA_SEQ_ID,
    kca_seq_date
  FROM bec_raw_dl_ext.MTL_SYSTEM_ITEMS_TL
  WHERE
    kca_operation <> 'DELETE'
    AND COALESCE(kca_seq_id, '') <> ''
    AND (COALESCE(INVENTORY_ITEM_ID, 0), COALESCE(ORGANIZATION_ID, 0), COALESCE(LANGUAGE, ''), KCA_SEQ_ID) IN (
      SELECT
        COALESCE(INVENTORY_ITEM_ID, 0) AS INVENTORY_ITEM_ID,
        COALESCE(ORGANIZATION_ID, 0) AS ORGANIZATION_ID,
        COALESCE(LANGUAGE, '') AS LANGUAGE,
        MAX(KCA_SEQ_ID)
      FROM bec_raw_dl_ext.MTL_SYSTEM_ITEMS_TL
      WHERE
        kca_operation <> 'DELETE' AND COALESCE(kca_seq_id, '') <> ''
      GROUP BY
        COALESCE(INVENTORY_ITEM_ID, 0),
        COALESCE(ORGANIZATION_ID, 0),
        COALESCE(LANGUAGE, '')
    )
    AND (
      kca_seq_date > (
        SELECT
          (
            executebegints - prune_days
          )
        FROM bec_etl_ctrl.batch_ods_info
        WHERE
          ods_table_name = 'mtl_system_items_tl'
      )
    )
);