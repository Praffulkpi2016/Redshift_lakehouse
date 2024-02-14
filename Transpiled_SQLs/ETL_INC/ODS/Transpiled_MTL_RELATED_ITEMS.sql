/* Delete Records   */
DELETE FROM silver_bec_ods.MTL_RELATED_ITEMS
WHERE
  (COALESCE(INVENTORY_ITEM_ID, 0), COALESCE(RELATED_ITEM_ID, 0), COALESCE(RELATIONSHIP_TYPE_ID, 0), COALESCE(ORGANIZATION_ID, 0)) IN (
    SELECT
      COALESCE(stg.INVENTORY_ITEM_ID, 0) AS INVENTORY_ITEM_ID,
      COALESCE(stg.RELATED_ITEM_ID, 0) AS RELATED_ITEM_ID,
      COALESCE(stg.RELATIONSHIP_TYPE_ID, 0) AS RELATIONSHIP_TYPE_ID,
      COALESCE(stg.ORGANIZATION_ID, 0) AS ORGANIZATION_ID
    FROM silver_bec_ods.MTL_RELATED_ITEMS AS ods, bronze_bec_ods_stg.MTL_RELATED_ITEMS AS stg
    WHERE
      COALESCE(ods.INVENTORY_ITEM_ID, 0) = COALESCE(stg.INVENTORY_ITEM_ID, 0)
      AND COALESCE(ods.RELATED_ITEM_ID, 0) = COALESCE(stg.RELATED_ITEM_ID, 0)
      AND COALESCE(ods.RELATIONSHIP_TYPE_ID, 0) = COALESCE(stg.RELATIONSHIP_TYPE_ID, 0)
      AND COALESCE(ods.ORGANIZATION_ID, 0) = COALESCE(stg.ORGANIZATION_ID, 0)
      AND stg.kca_operation IN ('INSERT', 'UPDATE')
  );
/* Insert records */
INSERT INTO silver_bec_ods.MTL_RELATED_ITEMS (
  inventory_item_id,
  organization_id,
  related_item_id,
  relationship_type_id,
  reciprocal_flag,
  last_update_date,
  last_updated_by,
  creation_date,
  created_by,
  last_update_login,
  request_id,
  program_application_id,
  program_id,
  program_update_date,
  object_version_number,
  start_date,
  end_date,
  attr_context,
  attr_char1,
  attr_char2,
  attr_char3,
  attr_char4,
  attr_char5,
  attr_char6,
  attr_char7,
  attr_char8,
  attr_char9,
  attr_char10,
  attr_num1,
  attr_num2,
  attr_num3,
  attr_num4,
  attr_num5,
  attr_num6,
  attr_num7,
  attr_num8,
  attr_num9,
  attr_num10,
  attr_date1,
  attr_date2,
  attr_date3,
  attr_date4,
  attr_date5,
  attr_date6,
  attr_date7,
  attr_date8,
  attr_date9,
  attr_date10,
  planning_enabled_flag,
  KCA_OPERATION,
  IS_DELETED_FLG,
  kca_seq_id,
  kca_seq_date
)
(
  SELECT
    inventory_item_id,
    organization_id,
    related_item_id,
    relationship_type_id,
    reciprocal_flag,
    last_update_date,
    last_updated_by,
    creation_date,
    created_by,
    last_update_login,
    request_id,
    program_application_id,
    program_id,
    program_update_date,
    object_version_number,
    start_date,
    end_date,
    attr_context,
    attr_char1,
    attr_char2,
    attr_char3,
    attr_char4,
    attr_char5,
    attr_char6,
    attr_char7,
    attr_char8,
    attr_char9,
    attr_char10,
    attr_num1,
    attr_num2,
    attr_num3,
    attr_num4,
    attr_num5,
    attr_num6,
    attr_num7,
    attr_num8,
    attr_num9,
    attr_num10,
    attr_date1,
    attr_date2,
    attr_date3,
    attr_date4,
    attr_date5,
    attr_date6,
    attr_date7,
    attr_date8,
    attr_date9,
    attr_date10,
    planning_enabled_flag,
    KCA_OPERATION,
    'N' AS IS_DELETED_FLG,
    CAST(NULLIF(KCA_SEQ_ID, '') AS DECIMAL(36, 0)) AS KCA_SEQ_ID,
    kca_seq_date
  FROM bronze_bec_ods_stg.MTL_RELATED_ITEMS
  WHERE
    kca_operation IN ('INSERT', 'UPDATE')
    AND (COALESCE(INVENTORY_ITEM_ID, 0), COALESCE(RELATED_ITEM_ID, 0), COALESCE(RELATIONSHIP_TYPE_ID, 0), COALESCE(ORGANIZATION_ID, 0), KCA_SEQ_ID) IN (
      SELECT
        COALESCE(INVENTORY_ITEM_ID, 0) AS INVENTORY_ITEM_ID,
        COALESCE(RELATED_ITEM_ID, 0) AS RELATED_ITEM_ID,
        COALESCE(RELATIONSHIP_TYPE_ID, 0) AS RELATIONSHIP_TYPE_ID,
        COALESCE(ORGANIZATION_ID, 0) AS ORGANIZATION_ID,
        MAX(KCA_SEQ_ID)
      FROM bronze_bec_ods_stg.MTL_RELATED_ITEMS
      WHERE
        kca_operation IN ('INSERT', 'UPDATE')
      GROUP BY
        COALESCE(INVENTORY_ITEM_ID, 0),
        COALESCE(RELATED_ITEM_ID, 0),
        COALESCE(RELATIONSHIP_TYPE_ID, 0),
        COALESCE(ORGANIZATION_ID, 0)
    )
);
/* Soft delete */
UPDATE silver_bec_ods.MTL_RELATED_ITEMS SET IS_DELETED_FLG = 'N';
UPDATE silver_bec_ods.MTL_RELATED_ITEMS SET IS_DELETED_FLG = 'Y'
WHERE
  (COALESCE(INVENTORY_ITEM_ID, 0), COALESCE(RELATED_ITEM_ID, 0), COALESCE(RELATIONSHIP_TYPE_ID, 0), COALESCE(ORGANIZATION_ID, 0)) IN (
    SELECT
      COALESCE(INVENTORY_ITEM_ID, 0),
      COALESCE(RELATED_ITEM_ID, 0),
      COALESCE(RELATIONSHIP_TYPE_ID, 0),
      COALESCE(ORGANIZATION_ID, 0)
    FROM bec_raw_dl_ext.MTL_RELATED_ITEMS
    WHERE
      (COALESCE(INVENTORY_ITEM_ID, 0), COALESCE(RELATED_ITEM_ID, 0), COALESCE(RELATIONSHIP_TYPE_ID, 0), COALESCE(ORGANIZATION_ID, 0), KCA_SEQ_ID) IN (
        SELECT
          COALESCE(INVENTORY_ITEM_ID, 0),
          COALESCE(RELATED_ITEM_ID, 0),
          COALESCE(RELATIONSHIP_TYPE_ID, 0),
          COALESCE(ORGANIZATION_ID, 0),
          MAX(KCA_SEQ_ID) AS KCA_SEQ_ID
        FROM bec_raw_dl_ext.MTL_RELATED_ITEMS
        GROUP BY
          COALESCE(INVENTORY_ITEM_ID, 0),
          COALESCE(RELATED_ITEM_ID, 0),
          COALESCE(RELATIONSHIP_TYPE_ID, 0),
          COALESCE(ORGANIZATION_ID, 0)
      )
      AND kca_operation = 'DELETE'
  );
UPDATE bec_etl_ctrl.batch_ods_info SET last_refresh_date = CURRENT_TIMESTAMP()
WHERE
  ods_table_name = 'mtl_related_items';