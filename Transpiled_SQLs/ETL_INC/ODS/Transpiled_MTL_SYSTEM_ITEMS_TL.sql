/* Delete Records */
DELETE FROM silver_bec_ods.MTL_SYSTEM_ITEMS_TL
WHERE
  (COALESCE(INVENTORY_ITEM_ID, 0), COALESCE(ORGANIZATION_ID, 0), COALESCE(LANGUAGE, '')) IN (
    SELECT
      COALESCE(stg.INVENTORY_ITEM_ID, 0) AS INVENTORY_ITEM_ID,
      COALESCE(stg.ORGANIZATION_ID, 0) AS ORGANIZATION_ID,
      COALESCE(stg.LANGUAGE, '') AS LANGUAGE
    FROM silver_bec_ods.MTL_SYSTEM_ITEMS_TL AS ods, bronze_bec_ods_stg.MTL_SYSTEM_ITEMS_TL AS stg
    WHERE
      COALESCE(ods.INVENTORY_ITEM_ID, 0) = COALESCE(stg.INVENTORY_ITEM_ID, 0)
      AND COALESCE(ods.ORGANIZATION_ID, 0) = COALESCE(stg.ORGANIZATION_ID, 0)
      AND COALESCE(ods.LANGUAGE, '') = COALESCE(stg.LANGUAGE, '')
      AND stg.kca_operation IN ('INSERT', 'UPDATE')
  );
/* Insert records */
INSERT INTO silver_bec_ods.MTL_SYSTEM_ITEMS_TL (
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
  IS_DELETED_FLG,
  kca_seq_id,
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
    'N' AS IS_DELETED_FLG,
    CAST(NULLIF(KCA_SEQ_ID, '') AS DECIMAL(36, 0)) AS KCA_SEQ_ID,
    kca_seq_date
  FROM bronze_bec_ods_stg.MTL_SYSTEM_ITEMS_TL
  WHERE
    kca_operation IN ('INSERT', 'UPDATE')
    AND (COALESCE(INVENTORY_ITEM_ID, 0), COALESCE(ORGANIZATION_ID, 0), COALESCE(LANGUAGE, ''), KCA_SEQ_ID) IN (
      SELECT
        COALESCE(INVENTORY_ITEM_ID, 0) AS INVENTORY_ITEM_ID,
        COALESCE(ORGANIZATION_ID, 0) AS ORGANIZATION_ID,
        COALESCE(LANGUAGE, '') AS LANGUAGE,
        MAX(KCA_SEQ_ID)
      FROM bronze_bec_ods_stg.MTL_SYSTEM_ITEMS_TL
      WHERE
        kca_operation IN ('INSERT', 'UPDATE')
      GROUP BY
        COALESCE(INVENTORY_ITEM_ID, 0),
        COALESCE(ORGANIZATION_ID, 0),
        COALESCE(LANGUAGE, '')
    )
);
/* Soft delete */
UPDATE silver_bec_ods.MTL_SYSTEM_ITEMS_TL SET IS_DELETED_FLG = 'N';
UPDATE silver_bec_ods.MTL_SYSTEM_ITEMS_TL SET IS_DELETED_FLG = 'Y'
WHERE
  (COALESCE(INVENTORY_ITEM_ID, 0), COALESCE(ORGANIZATION_ID, 0), COALESCE(LANGUAGE, '')) IN (
    SELECT
      COALESCE(INVENTORY_ITEM_ID, 0),
      COALESCE(ORGANIZATION_ID, 0),
      COALESCE(LANGUAGE, '')
    FROM bec_raw_dl_ext.MTL_SYSTEM_ITEMS_TL
    WHERE
      (COALESCE(INVENTORY_ITEM_ID, 0), COALESCE(ORGANIZATION_ID, 0), COALESCE(LANGUAGE, ''), KCA_SEQ_ID) IN (
        SELECT
          COALESCE(INVENTORY_ITEM_ID, 0),
          COALESCE(ORGANIZATION_ID, 0),
          COALESCE(LANGUAGE, ''),
          MAX(KCA_SEQ_ID) AS KCA_SEQ_ID
        FROM bec_raw_dl_ext.MTL_SYSTEM_ITEMS_TL
        GROUP BY
          COALESCE(INVENTORY_ITEM_ID, 0),
          COALESCE(ORGANIZATION_ID, 0),
          COALESCE(LANGUAGE, '')
      )
      AND kca_operation = 'DELETE'
  );
UPDATE bec_etl_ctrl.batch_ods_info SET last_refresh_date = CURRENT_TIMESTAMP()
WHERE
  ods_table_name = 'mtl_system_items_tl';