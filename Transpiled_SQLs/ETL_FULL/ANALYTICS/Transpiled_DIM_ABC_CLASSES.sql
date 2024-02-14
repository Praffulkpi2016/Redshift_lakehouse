DROP TABLE IF EXISTS gold_bec_dwh.dim_abc_classes;
CREATE TABLE gold_bec_dwh.dim_abc_classes AS
(
  SELECT
    maag.assignment_group_id,
    maa.inventory_item_id,
    maag.organization_id,
    mac.abc_class_id,
    mac.abc_class_name,
    mac.description,
    mac.disable_date,
    maag.assignment_group_name,
    maag.compile_id,
    maag.secondary_inventory,
    maag.item_scope_type,
    maag.classification_method_type,
    'N' AS is_deleted_flg, /* audit columns */
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
    ) || '-' || COALESCE(maag.assignment_group_id, 0) || '-' || COALESCE(maa.abc_class_id, 0) || '-' || COALESCE(maa.inventory_item_id, 0) || '-' || COALESCE(maag.organization_id, 0) AS dw_load_id,
    CURRENT_TIMESTAMP() AS dw_insert_date,
    CURRENT_TIMESTAMP() AS dw_update_date
  FROM silver_bec_ods.mtl_abc_assignment_groups AS maag, silver_bec_ods.mtl_abc_assignments AS maa, silver_bec_ods.mtl_abc_classes AS mac
  WHERE
    1 = 1
    AND maag.assignment_group_id = maa.assignment_group_id
    AND maa.abc_class_id = mac.abc_class_id
);
UPDATE bec_etl_ctrl.batch_dw_info SET load_type = 'I', last_refresh_date = CURRENT_TIMESTAMP()
WHERE
  dw_table_name = 'dim_abc_classes' AND batch_name = 'inv';