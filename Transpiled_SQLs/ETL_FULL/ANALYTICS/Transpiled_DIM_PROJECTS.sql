DROP table IF EXISTS gold_bec_dwh.dim_projects;
CREATE TABLE gold_bec_dwh.dim_projects AS
(
  SELECT
    project_id AS project_id,
    name AS name,
    segment1 AS project_num,
    project_type AS project_type,
    project_status_code AS project_status_code,
    description AS description,
    start_date AS start_date,
    completion_date AS completion_date,
    enabled_flag AS enabled_flag,
    attribute_category AS attribute_category,
    org_id AS org_id,
    project_currency_code AS project_currency_code,
    long_name AS long_name,
    last_update_date AS last_update_date,
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
    ) || '-' || COALESCE(project_id, 0) AS dw_load_id,
    CURRENT_TIMESTAMP() AS dw_insert_date,
    CURRENT_TIMESTAMP() AS dw_update_date
  FROM silver_bec_ods.pa_projects_all AS pap
);
UPDATE bec_etl_ctrl.batch_dw_info SET load_type = 'I', last_refresh_date = CURRENT_TIMESTAMP()
WHERE
  dw_table_name = 'dim_projects' AND batch_name = 'ap';