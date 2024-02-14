/* Delete Records */
DELETE FROM gold_bec_dwh.DIM_PROJECT_TASKS
WHERE
  EXISTS(
    SELECT
      1
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
    WHERE
      COALESCE(dim_project_tasks.project_id, 0) = COALESCE(PPA.project_id, 0)
      AND COALESCE(dim_project_tasks.task_id, 0) = COALESCE(pt.task_id, 0)
      AND pt.kca_seq_date > (
        SELECT
          (
            executebegints - prune_days
          )
        FROM bec_etl_ctrl.batch_dw_info
        WHERE
          dw_table_name = 'dim_project_tasks' AND batch_name = 'ap'
      )
  );
/* Insert records */
INSERT INTO gold_bec_dwh.dim_project_tasks (
  project_id,
  Project_Number,
  Project_Name,
  person_id,
  task_id,
  parent_task_id,
  task_number,
  creation_date,
  created_by,
  last_update_date,
  task_name,
  wbs_level,
  task_description,
  carrying_out_organization_id,
  service_type_code,
  chargeable_flag,
  billable_flag,
  start_date,
  completion_date,
  work_type_id,
  long_task_name,
  task_manager,
  is_deleted_flg,
  source_app_id,
  dw_load_id,
  dw_insert_date,
  dw_update_date
)
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
    'N' AS is_deleted_flg, /* audit column */
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
  WHERE
    1 = 1
    AND (
      pt.kca_seq_date > (
        SELECT
          (
            executebegints - prune_days
          )
        FROM bec_etl_ctrl.batch_dw_info
        WHERE
          dw_table_name = 'dim_project_tasks' AND batch_name = 'ap'
      )
    )
);
/* Soft delete */
UPDATE gold_bec_dwh.dim_project_tasks SET is_deleted_flg = 'Y'
WHERE
  NOT EXISTS(
    SELECT
      1
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
    WHERE
      COALESCE(dim_project_tasks.project_id, 0) = COALESCE(PPA.project_id, 0)
      AND COALESCE(dim_project_tasks.task_id, 0) = COALESCE(pt.task_id, 0)
      AND (
        ppa.is_deleted_flg <> 'Y' OR pt.is_deleted_flg <> 'Y'
      )
  );
UPDATE bec_etl_ctrl.batch_dw_info SET last_refresh_date = CURRENT_TIMESTAMP()
WHERE
  dw_table_name = 'dim_project_tasks' AND batch_name = 'ap';