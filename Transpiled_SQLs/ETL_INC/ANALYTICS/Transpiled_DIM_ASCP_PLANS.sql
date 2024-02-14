TRUNCATE table gold_bec_dwh.DIM_ASCP_PLANS;
INSERT INTO gold_bec_dwh.DIM_ASCP_PLANS
(
  SELECT
    plan_id,
    compile_designator,
    sr_instance_id,
    description,
    curr_start_date,
    curr_cutoff_date,
    cutoff_date,
    curr_plan_type,
    data_start_date,
    data_completion_date,
    daily_cutoff_bucket,
    weekly_cutoff_bucket,
    CASE
      WHEN compile_designator IN ('BE-GLOBAL', 'MPS-Plan-1', 'MRP-Plan-1')
      THEN 'Y'
      ELSE 'N'
    END AS LOAD_FLG,
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
    ) || '-' || COALESCE(plan_id, 0) AS dw_load_id,
    CURRENT_TIMESTAMP() AS dw_insert_date,
    CURRENT_TIMESTAMP() AS dw_update_date
  FROM silver_bec_ods.msc_plans
  WHERE
    is_deleted_flg <> 'Y'
);
UPDATE bec_etl_ctrl.batch_dw_info SET load_type = 'I', last_refresh_date = CURRENT_TIMESTAMP()
WHERE
  dw_table_name = 'dim_ascp_plans' AND batch_name = 'ascp';