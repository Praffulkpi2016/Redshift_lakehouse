DROP table IF EXISTS gold_bec_dwh.DIM_FND_USER;
CREATE TABLE gold_bec_dwh.DIM_FND_USER AS
(
  SELECT DISTINCT
    fu.user_id,
    ppf.person_id,
    COALESCE(ppf.full_name, fu.user_name) AS employee_name,
    fu.user_name,
    ppf.employee_number,
    fu.start_date,
    fu.end_date,
    fu.description,
    fu.employee_id,
    fu.email_address,
    fu.customer_id,
    fu.supplier_id,
    ppf.effective_end_date,
    ppf.full_name,
    0 AS employee_count,
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
    ) || '-' || COALESCE(fu.user_id, 0) || '-' || COALESCE(fu.employee_id, 0) || '-' || COALESCE(ppf.person_id, 0) AS dw_load_id,
    CURRENT_TIMESTAMP() AS dw_insert_date,
    CURRENT_TIMESTAMP() AS dw_update_date
  FROM (
    SELECT
      *
    FROM silver_bec_ods.fnd_user
    WHERE
      is_deleted_flg <> 'Y'
  ) AS fu
  LEFT JOIN (
    SELECT
      *
    FROM silver_bec_ods.per_all_people_f
    WHERE
      is_deleted_flg <> 'Y'
  ) AS ppf
    ON fu.employee_id = ppf.person_id
    AND CURRENT_TIMESTAMP() BETWEEN ppf.effective_start_date AND ppf.effective_end_date
  LEFT JOIN (
    SELECT
      person_id,
      MAX(object_version_number) AS object_version_number
    FROM silver_bec_ods.per_all_people_f
    WHERE
      is_deleted_flg <> 'Y'
    GROUP BY
      person_id
  ) AS ppf_max
    ON ppf.person_id = ppf_max.person_id
    AND ppf.object_version_number = ppf_max.object_version_number
  WHERE
    1 = 1
);
UPDATE gold_bec_dwh.dim_fnd_user AS fnd SET employee_count = (
  SELECT
    COUNT(employee_id)
  FROM silver_bec_ods.fnd_user AS fnd1
  WHERE
    fnd.employee_id = fnd1.employee_id
);
UPDATE bec_etl_ctrl.batch_dw_info SET load_type = 'I', last_refresh_date = CURRENT_TIMESTAMP()
WHERE
  dw_table_name = 'dim_fnd_user' AND batch_name = 'ap';