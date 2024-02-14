TRUNCATE table bronze_bec_ods_stg.MTL_RELATED_ITEMS;
INSERT INTO bronze_bec_ods_stg.MTL_RELATED_ITEMS (
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
    kca_seq_id,
    kca_seq_date
  FROM bec_raw_dl_ext.MTL_RELATED_ITEMS
  WHERE
    kca_operation <> 'DELETE'
    AND COALESCE(kca_seq_id, '') <> ''
    AND (COALESCE(INVENTORY_ITEM_ID, 0), COALESCE(RELATED_ITEM_ID, 0), COALESCE(RELATIONSHIP_TYPE_ID, 0), COALESCE(ORGANIZATION_ID, 0), KCA_SEQ_ID) IN (
      SELECT
        COALESCE(INVENTORY_ITEM_ID, 0) AS INVENTORY_ITEM_ID,
        COALESCE(RELATED_ITEM_ID, 0) AS RELATED_ITEM_ID,
        COALESCE(RELATIONSHIP_TYPE_ID, 0) AS RELATIONSHIP_TYPE_ID,
        COALESCE(ORGANIZATION_ID, 0) AS ORGANIZATION_ID,
        MAX(KCA_SEQ_ID)
      FROM bec_raw_dl_ext.MTL_RELATED_ITEMS
      WHERE
        kca_operation <> 'DELETE' AND COALESCE(kca_seq_id, '') <> ''
      GROUP BY
        COALESCE(INVENTORY_ITEM_ID, 0),
        COALESCE(RELATED_ITEM_ID, 0),
        COALESCE(RELATIONSHIP_TYPE_ID, 0),
        COALESCE(ORGANIZATION_ID, 0)
    )
    AND kca_seq_date > (
      SELECT
        (
          executebegints - prune_days
        )
      FROM bec_etl_ctrl.batch_ods_info
      WHERE
        ods_table_name = 'mtl_related_items'
    )
);