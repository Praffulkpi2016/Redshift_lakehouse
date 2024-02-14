DROP table IF EXISTS gold_bec_dwh.dim_cost_types;
CREATE TABLE gold_bec_dwh.dim_cost_types AS
(
  SELECT
    cost_type_id,
    last_update_date,
    last_updated_by,
    creation_date,
    created_by,
    last_update_login,
    organization_id,
    cost_type,
    description,
    costing_method_type,
    frozen_standard_flag,
    default_cost_type_id,
    bom_snapshot_flag,
    alternate_bom_designator,
    allow_updates_flag,
    pl_element_flag,
    pl_resource_flag,
    pl_operation_flag,
    pl_activity_flag,
    disable_date,
    available_to_eng_flag,
    component_yield_flag,
    attribute_category,
    attribute1,
    attribute2,
    attribute3,
    attribute4,
    attribute5,
    attribute6,
    attribute7,
    attribute8,
    attribute9,
    attribute10,
    attribute11,
    attribute12,
    attribute13,
    attribute14,
    attribute15,
    request_id,
    program_application_id,
    program_id,
    program_update_date,
    zd_edition_name,
    zd_sync,
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
    ) || '-' || COALESCE(cost_type_id, 0) AS dw_load_id,
    CURRENT_TIMESTAMP() AS dw_insert_date,
    CURRENT_TIMESTAMP() AS dw_update_date
  FROM silver_bec_ods.cst_cost_types
);
UPDATE bec_etl_ctrl.batch_dw_info SET load_type = 'I', last_refresh_date = CURRENT_TIMESTAMP()
WHERE
  dw_table_name = 'dim_cost_types' AND batch_name = 'costing';