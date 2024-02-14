DROP table IF EXISTS gold_bec_dwh.dim_project_tasks;
CREATE TABLE gold_bec_dwh.dim_project_tasks AS
(
  SELECT
    pt.project_id,
    PPA.SEGMENT1 AS Project_Number,
    PPA.NAME AS Project_Name,
    pp.person_id,
    pt.task_id,
    pt.parent_task_id,
    pt.task_number,
    pt.creation_date,
    pt.created_by,
    pt.last_update_date,
    pt.task_name,
    pt.wbs_level,
    pt.description AS `TASK_DESCRIPTION`,
    pt.carrying_out_organization_id,
    pt.service_type_code,
    pt.chargeable_flag,
    pt.billable_flag,
    pt.start_date,
    pt.completion_date,
    pt.work_type_id,
    pt.long_task_name,
    pp.person_name AS `TASK_MANAGER`,
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
    ) || '-' || COALESCE(pt.project_id, 0) || '-' || COALESCE(pt.task_id, 0) AS dw_load_id,
    CURRENT_TIMESTAMP() AS dw_insert_date,
    CURRENT_TIMESTAMP() AS dw_update_date
  FROM silver_bec_ods.PA_PROJECTS_ALL AS PPA
  INNER JOIN silver_bec_ods.pa_tasks AS pt
    ON PPA.PROJECT_ID = PT.PROJECT_ID
  LEFT JOIN (
    SELECT
      person_id,
      MAX(full_name) AS person_name
    FROM silver_bec_ods.per_all_people_f
    GROUP BY
      person_id
  ) AS pp
    ON pt.task_manager_person_id = pp.person_id
);
UPDATE bec_etl_ctrl.batch_dw_info SET load_type = 'I', last_refresh_date = CURRENT_TIMESTAMP()
WHERE
  dw_table_name = 'dim_project_tasks' AND batch_name = 'ap';