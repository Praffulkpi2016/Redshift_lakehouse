/* DELETE */
DELETE FROM gold_bec_dwh.DIM_FND_USER
WHERE
  (COALESCE(user_id, 0), COALESCE(employee_id, 0), COALESCE(person_id, 0)) IN (
    SELECT
      ods.user_id,
      ods.employee_id,
      ods.person_id
    FROM gold_bec_dwh.DIM_FND_USER AS dw, (
      SELECT
        COALESCE(fu.user_id, 0) AS user_id,
        COALESCE(fu.employee_id, 0) AS employee_id,
        COALESCE(ppf.person_id, 0) AS person_id
      FROM silver_bec_ods.fnd_user AS fu
      LEFT JOIN silver_bec_ods.per_all_people_f AS ppf
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
        (
          fu.kca_seq_date > (
            SELECT
              (
                executebegints - prune_days
              )
            FROM bec_etl_ctrl.batch_dw_info
            WHERE
              dw_table_name = 'dim_fnd_user' AND batch_name = 'ap'
          )
          OR ppf.kca_seq_date > (
            SELECT
              (
                executebegints - prune_days
              )
            FROM bec_etl_ctrl.batch_dw_info
            WHERE
              dw_table_name = 'dim_fnd_user' AND batch_name = 'ap'
          )
        )
    ) AS ods
    WHERE
      dw.dw_load_id = (
        SELECT
          system_id
        FROM bec_etl_ctrl.etlsourceappid
        WHERE
          source_system = 'EBS'
      ) || '-' || COALESCE(ods.user_id, 0) || '-' || COALESCE(ods.employee_id, 0) || '-' || COALESCE(ods.person_id, 0)
  );
/* insert */
INSERT INTO gold_bec_dwh.DIM_FND_USER
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
  FROM silver_bec_ods.fnd_user AS fu
  LEFT JOIN silver_bec_ods.per_all_people_f AS ppf
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
    (
      fu.kca_seq_date > (
        SELECT
          (
            executebegints - prune_days
          )
        FROM bec_etl_ctrl.batch_dw_info
        WHERE
          dw_table_name = 'dim_fnd_user' AND batch_name = 'ap'
      )
      OR ppf.kca_seq_date > (
        SELECT
          (
            executebegints - prune_days
          )
        FROM bec_etl_ctrl.batch_dw_info
        WHERE
          dw_table_name = 'dim_fnd_user' AND batch_name = 'ap'
      )
    )
);
UPDATE gold_bec_dwh.dim_fnd_user AS fnd SET employee_count = (
  SELECT
    COUNT(employee_id)
  FROM silver_bec_ods.fnd_user AS fnd1
  WHERE
    fnd.employee_id = fnd1.employee_id
);
/* soft delete */
UPDATE gold_bec_dwh.DIM_FND_USER SET is_deleted_flg = 'Y'
WHERE
  NOT (COALESCE(user_id, 0), COALESCE(employee_id, 0), COALESCE(person_id, 0)) IN (
    SELECT
      ods.user_id,
      ods.employee_id,
      ods.person_id
    FROM gold_bec_dwh.DIM_FND_USER AS dw, (
      SELECT
        COALESCE(fu.user_id, 0) AS user_id,
        COALESCE(fu.employee_id, 0) AS employee_id,
        COALESCE(ppf.person_id, 0) AS person_id
      FROM (
        SELECT
          user_id,
          employee_id
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
    ) AS ods
    WHERE
      dw.dw_load_id = (
        SELECT
          system_id
        FROM bec_etl_ctrl.etlsourceappid
        WHERE
          source_system = 'EBS'
      ) || '-' || COALESCE(ods.user_id, 0) || '-' || COALESCE(ods.employee_id, 0) || '-' || COALESCE(ods.person_id, 0)
  );
UPDATE bec_etl_ctrl.batch_dw_info SET load_type = 'I', last_refresh_date = CURRENT_TIMESTAMP()
WHERE
  dw_table_name = 'dim_fnd_user' AND batch_name = 'ap';