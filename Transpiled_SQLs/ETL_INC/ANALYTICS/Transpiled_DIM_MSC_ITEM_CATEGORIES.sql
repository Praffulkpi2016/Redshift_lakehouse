TRUNCATE table gold_bec_dwh.DIM_MSC_ITEM_CATEGORIES;
INSERT INTO gold_bec_dwh.dim_msc_item_categories
(
  SELECT
    organization_id,
    inventory_item_id,
    SR_INSTANCE_ID,
    SR_CATEGORY_ID,
    CATEGORY_SET_ID,
    CATEGORY_NAME,
    DESCRIPTION,
    DISABLE_DATE,
    SUMMARY_FLAG,
    ENABLED_FLAG,
    START_DATE_ACTIVE,
    END_DATE_ACTIVE,
    CREATION_DATE,
    CREATED_BY,
    LAST_UPDATE_DATE,
    LAST_UPDATED_BY,
    'N' AS is_deleted_flg,
    (
      SELECT
        system_id
      FROM bec_etl_ctrl.etlsourceappid
      WHERE
        source_system = 'EBS'
    ) AS source_app_id,
    (
      SELECT
        system_id
      FROM bec_etl_ctrl.etlsourceappid
      WHERE
        source_system = 'EBS'
    ) || '-' || COALESCE(organization_id, 0) || '-' || COALESCE(inventory_item_id, 0) || '-' || COALESCE(SR_CATEGORY_ID, 0) || '-' || COALESCE(CATEGORY_SET_ID, 0) || '-' || COALESCE(SR_INSTANCE_ID, 0) AS dw_load_id,
    CURRENT_TIMESTAMP() AS dw_insert_date,
    CURRENT_TIMESTAMP() AS dw_update_date
  FROM BEC_ODS.msc_item_categories
  WHERE
    is_deleted_flg <> 'Y'
);
UPDATE bec_etl_ctrl.batch_dw_info SET load_type = 'I', last_refresh_date = CURRENT_TIMESTAMP()
WHERE
  dw_table_name = 'dim_msc_item_categories' AND batch_name = 'ascp';