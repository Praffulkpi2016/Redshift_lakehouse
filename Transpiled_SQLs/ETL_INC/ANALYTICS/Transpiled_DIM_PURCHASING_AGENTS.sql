/* Delete Records */
DELETE FROM gold_bec_dwh.dim_purchasing_agents
WHERE
  COALESCE(agent_id, 0) IN (
    SELECT
      COALESCE(ods.agent_id, 0) AS agent_id
    FROM gold_bec_dwh.dim_purchasing_agents AS dw, (
      SELECT
        FLOOR(a.agent_id) AS agent_id
      FROM silver_bec_ods.po_agents AS a, silver_bec_ods.hr_locations_all AS b, (
        SELECT
          PERSON_ID,
          MAX(full_name) AS PERSON_NAME
        FROM silver_bec_ods.PER_ALL_PEOPLE_F
        GROUP BY
          person_id
      ) AS c
      WHERE
        1 = 1
        AND a.agent_id = c.person_id
        AND a.location_id = b.LOCATION_ID()
        AND (
          a.kca_seq_date > (
            SELECT
              (
                executebegints - prune_days
              )
            FROM bec_etl_ctrl.batch_dw_info
            WHERE
              dw_table_name = 'dim_purchasing_agents' AND batch_name = 'po'
          )
          OR b.kca_seq_date > (
            SELECT
              (
                executebegints - prune_days
              )
            FROM bec_etl_ctrl.batch_dw_info
            WHERE
              dw_table_name = 'dim_purchasing_agents' AND batch_name = 'po'
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
      ) || '-' || COALESCE(ods.agent_id, 0)
  );
/* Insert records */
INSERT INTO gold_bec_dwh.DIM_PURCHASING_AGENTS (
  agent_id,
  creation_date,
  last_update_date,
  location_code,
  location_id,
  authorization_limit,
  start_date_active,
  end_date_active,
  agent_name,
  CREATED_BY,
  is_deleted_flg,
  source_app_id,
  dw_load_id,
  dw_insert_date,
  dw_update_date
)
(
  SELECT
    FLOOR(a.agent_id) AS agent_id,
    a.creation_date AS creation_date,
    a.last_update_date AS last_update_date,
    b.location_code AS location_code,
    b.location_id,
    a.authorization_limit AS authorization_limit,
    a.start_date_active AS start_date_active,
    a.end_date_active AS end_date_active,
    c.person_name AS agent_name,
    b.CREATED_BY AS CREATED_BY,
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
    ) || '-' || COALESCE(a.agent_id, 0) AS dw_load_id,
    CURRENT_TIMESTAMP() AS dw_insert_date,
    CURRENT_TIMESTAMP() AS dw_update_date
  FROM silver_bec_ods.po_agents AS a, silver_bec_ods.hr_locations_all AS b, (
    SELECT
      PERSON_ID,
      MAX(full_name) AS PERSON_NAME
    FROM silver_bec_ods.PER_ALL_PEOPLE_F
    GROUP BY
      person_id
  ) AS c
  WHERE
    1 = 1
    AND a.agent_id = c.person_id
    AND a.location_id = b.LOCATION_ID()
    AND (
      a.kca_seq_date > (
        SELECT
          (
            executebegints - prune_days
          )
        FROM bec_etl_ctrl.batch_dw_info
        WHERE
          dw_table_name = 'dim_purchasing_agents' AND batch_name = 'po'
      )
      OR b.kca_seq_date > (
        SELECT
          (
            executebegints - prune_days
          )
        FROM bec_etl_ctrl.batch_dw_info
        WHERE
          dw_table_name = 'dim_purchasing_agents' AND batch_name = 'po'
      )
    )
);
/* Soft delete */
UPDATE gold_bec_dwh.dim_purchasing_agents SET is_deleted_flg = 'Y'
WHERE
  NOT COALESCE(agent_id, 0) IN (
    SELECT
      COALESCE(ods.agent_id, 0) AS agent_id
    FROM gold_bec_dwh.dim_purchasing_agents AS dw, (
      SELECT
        FLOOR(a.agent_id) AS agent_id,
        a.kca_operation
      FROM (
        SELECT
          *
        FROM silver_bec_ods.po_agents
        WHERE
          is_deleted_flg <> 'Y'
      ) AS a, (
        SELECT
          *
        FROM silver_bec_ods.hr_locations_all
        WHERE
          is_deleted_flg <> 'Y'
      ) AS b, (
        SELECT
          PERSON_ID,
          MAX(full_name) AS PERSON_NAME
        FROM silver_bec_ods.PER_ALL_PEOPLE_F
        WHERE
          is_deleted_flg <> 'Y'
        GROUP BY
          person_id
      ) AS c
      WHERE
        1 = 1 AND a.agent_id = c.person_id AND a.location_id = b.LOCATION_ID()
    ) AS ods
    WHERE
      dw.dw_load_id = (
        SELECT
          system_id
        FROM bec_etl_ctrl.etlsourceappid
        WHERE
          source_system = 'EBS'
      ) || '-' || COALESCE(ods.agent_id, 0)
  );
UPDATE bec_etl_ctrl.batch_dw_info SET last_refresh_date = CURRENT_TIMESTAMP()
WHERE
  dw_table_name = 'dim_purchasing_agents' AND batch_name = 'po';