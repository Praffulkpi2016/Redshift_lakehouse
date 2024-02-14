/* delete */
DELETE FROM gold_bec_dwh.dim_user
WHERE
  (COALESCE(user_id, 0), COALESCE(person_id, 0)) IN (
    SELECT
      ods.user_id,
      ods.person_id
    FROM gold_bec_dwh.dim_user AS dw, (
      SELECT
        COALESCE(fu.user_id, 0) AS user_id,
        COALESCE(ppf.person_id, 0) AS person_id
      FROM (
        SELECT
          *
        FROM silver_bec_ods.fnd_user
        WHERE
          user_id IN (
            SELECT
              MAX(user_id)
            FROM silver_bec_ods.fnd_user
            GROUP BY
              employee_id
          )
          AND kca_seq_date > (
            SELECT
              (
                executebegints - prune_days
              )
            FROM bec_etl_ctrl.batch_dw_info
            WHERE
              dw_table_name = 'dim_user' AND batch_name = 'ap'
          )
      ) AS fu
      LEFT JOIN (
        SELECT
          *
        FROM silver_bec_ods.per_all_people_f
        WHERE
          1 = 1
          AND kca_seq_date > (
            SELECT
              (
                executebegints - prune_days
              )
            FROM bec_etl_ctrl.batch_dw_info
            WHERE
              dw_table_name = 'dim_user' AND batch_name = 'ap'
          )
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
      ) || '-' || COALESCE(ods.user_id, 0) || '-' || COALESCE(ods.person_id, 0)
  );
/* insert */
INSERT INTO gold_bec_dwh.DIM_USER
(
  SELECT DISTINCT
    fu.user_id,
    ppf.person_id,
    COALESCE(ppf.full_name, fu.user_name) AS user_name,
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
    ) || '-' || COALESCE(fu.user_id, 0) || '-' || COALESCE(ppf.person_id, 0) AS dw_load_id,
    CURRENT_TIMESTAMP() AS dw_insert_date,
    CURRENT_TIMESTAMP() AS dw_update_date
  FROM (
    SELECT
      *
    FROM silver_bec_ods.fnd_user
    WHERE
      is_deleted_flg <> 'Y'
      AND user_id IN (
        SELECT
          MAX(user_id)
        FROM silver_bec_ods.fnd_user
        GROUP BY
          employee_id
      )
      AND kca_seq_date > (
        SELECT
          (
            executebegints - prune_days
          )
        FROM bec_etl_ctrl.batch_dw_info
        WHERE
          dw_table_name = 'dim_user' AND batch_name = 'ap'
      )
  ) AS fu
  LEFT JOIN (
    SELECT
      *
    FROM silver_bec_ods.per_all_people_f
    WHERE
      is_deleted_flg <> 'Y'
      AND kca_seq_date > (
        SELECT
          (
            executebegints - prune_days
          )
        FROM bec_etl_ctrl.batch_dw_info
        WHERE
          dw_table_name = 'dim_user' AND batch_name = 'ap'
      )
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
/* soft delete */
UPDATE gold_bec_dwh.dim_user SET is_deleted_flg = 'Y'
WHERE
  NOT (COALESCE(user_id, 0), COALESCE(person_id, 0)) IN (
    SELECT
      ods.user_id,
      ods.person_id
    FROM gold_bec_dwh.dim_user AS dw, (
      SELECT
        COALESCE(fu.user_id, 0) AS user_id,
        COALESCE(ppf.person_id, 0) AS person_id
      FROM (
        SELECT
          *
        FROM silver_bec_ods.fnd_user
        WHERE
          is_deleted_flg <> 'Y'
          AND user_id IN (
            SELECT
              MAX(user_id)
            FROM silver_bec_ods.fnd_user
            GROUP BY
              employee_id
          )
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
      ) || '-' || COALESCE(ods.user_id, 0) || '-' || COALESCE(ods.person_id, 0)
  );
UPDATE bec_etl_ctrl.batch_dw_info SET load_type = 'I', last_refresh_date = CURRENT_TIMESTAMP()
WHERE
  dw_table_name = 'dim_user' AND batch_name = 'ap';