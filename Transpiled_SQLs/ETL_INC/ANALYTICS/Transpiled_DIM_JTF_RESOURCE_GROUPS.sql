/* delete */
DELETE FROM gold_bec_dwh.DIM_JTF_RESOURCE_GROUPS
WHERE
  (COALESCE(owner_id, 0), COALESCE(owner_type_code, 'NA')) IN (
    SELECT
      COALESCE(ods.owner_id, 0) AS owner_id,
      COALESCE(ods.owner_type_code, 'NA') AS owner_type_code
    FROM gold_bec_dwh.DIM_JTF_RESOURCE_GROUPS AS dw, (
      SELECT
        'RS_GROUP' AS owner_type_code,
        b.group_id AS owner_id
      FROM silver_bec_ods.jtf_rs_groups_tl AS t, silver_bec_ods.jtf_rs_groups_b AS b
      WHERE
        b.group_id = t.group_id
        AND t.language = 'US'
        AND b.kca_seq_date > (
          SELECT
            (
              executebegints - prune_days
            )
          FROM bec_etl_ctrl.batch_dw_info
          WHERE
            dw_table_name = 'dim_jtf_resource_groups' AND batch_name = 'inv'
        )
      UNION ALL
      SELECT
        'RS_EMPLOYEE' AS owner_type_code,
        b.resource_id AS owner_id
      FROM silver_bec_ods.jtf_rs_resource_extns AS b, silver_bec_ods.jtf_rs_resource_extns_tl AS t
      WHERE
        t.language = 'US'
        AND b.resource_id = t.resource_id
        AND b.category = t.category
        AND b.kca_seq_date > (
          SELECT
            (
              executebegints - prune_days
            )
          FROM bec_etl_ctrl.batch_dw_info
          WHERE
            dw_table_name = 'dim_jtf_resource_groups' AND batch_name = 'inv'
        )
    ) AS ods
    WHERE
      dw.dw_load_id = (
        SELECT
          system_id
        FROM bec_etl_ctrl.etlsourceappid
        WHERE
          source_system = 'EBS'
      ) || '-' || COALESCE(ods.owner_id, 0) || '-' || COALESCE(ods.owner_type_code, 'NA')
  );
/* INSERT */
INSERT INTO gold_bec_dwh.DIM_JTF_RESOURCE_GROUPS
SELECT
  owner_type_code,
  owner_id,
  owner_name,
  descripton,
  owner_number,
  email,
  created_by,
  creation_date,
  last_updated_by,
  last_update_date,
  last_update_login,
  start_date_active,
  end_date_active,
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
  ) || '-' || COALESCE(owner_id, 0) || '-' || COALESCE(owner_type_code, 'NA') AS dw_load_id,
  CURRENT_TIMESTAMP() AS dw_insert_date,
  CURRENT_TIMESTAMP() AS dw_update_date
FROM (
  SELECT
    'RS_GROUP' AS owner_type_code,
    b.group_id AS owner_id,
    t.group_name AS owner_name,
    t.group_desc AS descripton,
    b.group_number AS owner_number,
    b.email_address AS email,
    b.created_by,
    b.creation_date,
    b.last_updated_by,
    b.last_update_date,
    b.last_update_login,
    b.start_date_active,
    b.end_date_active
  FROM silver_bec_ods.jtf_rs_groups_tl AS t, silver_bec_ods.jtf_rs_groups_b AS b
  WHERE
    b.group_id = t.group_id
    AND t.language = 'US'
    AND b.kca_seq_date > (
      SELECT
        (
          executebegints - prune_days
        )
      FROM bec_etl_ctrl.batch_dw_info
      WHERE
        dw_table_name = 'dim_jtf_resource_groups' AND batch_name = 'inv'
    )
  UNION ALL
  SELECT
    'RS_EMPLOYEE' AS owner_type_code,
    b.resource_id AS owner_id,
    t.resource_name AS owner_name,
    NULL AS descripton,
    b.resource_number AS owner_number,
    b.source_email AS email,
    b.created_by,
    b.creation_date,
    b.last_updated_by,
    b.last_update_date,
    b.last_update_login,
    b.start_date_active,
    b.end_date_active
  FROM silver_bec_ods.jtf_rs_resource_extns AS b, silver_bec_ods.jtf_rs_resource_extns_tl AS t
  WHERE
    t.language = 'US'
    AND b.resource_id = t.resource_id
    AND b.category = t.category
    AND b.kca_seq_date > (
      SELECT
        (
          executebegints - prune_days
        )
      FROM bec_etl_ctrl.batch_dw_info
      WHERE
        dw_table_name = 'dim_jtf_resource_groups' AND batch_name = 'inv'
    )
);
/* soft delete */
UPDATE gold_bec_dwh.DIM_JTF_RESOURCE_GROUPS SET is_deleted_flg = 'Y'
WHERE
  NOT (COALESCE(owner_id, 0), COALESCE(owner_type_code, 'NA')) IN (
    SELECT
      COALESCE(ods.owner_id, 0) AS owner_id,
      COALESCE(ods.owner_type_code, 'NA') AS owner_type_code
    FROM gold_bec_dwh.DIM_JTF_RESOURCE_GROUPS AS dw, (
      SELECT
        'RS_GROUP' AS owner_type_code,
        b.group_id AS owner_id
      FROM (
        SELECT
          *
        FROM silver_bec_ods.jtf_rs_groups_tl
        WHERE
          is_deleted_flg <> 'Y'
      ) AS t, (
        SELECT
          *
        FROM silver_bec_ods.jtf_rs_groups_b
        WHERE
          is_deleted_flg <> 'Y'
      ) AS b
      WHERE
        b.group_id = t.group_id AND t.language = 'US'
      UNION ALL
      SELECT
        'RS_EMPLOYEE' AS owner_type_code,
        b.resource_id AS owner_id
      FROM (
        SELECT
          *
        FROM silver_bec_ods.jtf_rs_resource_extns
        WHERE
          is_deleted_flg <> 'Y'
      ) AS b, (
        SELECT
          *
        FROM silver_bec_ods.jtf_rs_resource_extns_tl
        WHERE
          is_deleted_flg <> 'Y'
      ) AS t
      WHERE
        t.language = 'US' AND b.resource_id = t.resource_id AND b.category = t.category
    ) AS ods
    WHERE
      dw.dw_load_id = (
        SELECT
          system_id
        FROM bec_etl_ctrl.etlsourceappid
        WHERE
          source_system = 'EBS'
      ) || '-' || COALESCE(ods.owner_id, 0) || '-' || COALESCE(ods.owner_type_code, 'NA')
  );
UPDATE bec_etl_ctrl.batch_dw_info SET load_type = 'I', last_refresh_date = CURRENT_TIMESTAMP()
WHERE
  dw_table_name = 'dim_jtf_resource_groups' AND batch_name = 'inv';