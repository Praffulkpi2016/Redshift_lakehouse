DROP table IF EXISTS gold_bec_dwh.DIM_PURCHASING_AGENTS;
CREATE TABLE gold_bec_dwh.DIM_PURCHASING_AGENTS AS
(
  SELECT
    a.agent_id,
    a.creation_date,
    a.last_update_date,
    b.location_code,
    b.location_id,
    a.authorization_limit,
    a.start_date_active,
    a.end_date_active,
    c.person_name AS agent_name,
    b.CREATED_BY,
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
    1 = 1 AND a.agent_id = c.person_id AND a.location_id = b.LOCATION_ID()
);
UPDATE bec_etl_ctrl.batch_dw_info SET load_type = 'I', last_refresh_date = CURRENT_TIMESTAMP()
WHERE
  dw_table_name = 'dim_purchasing_agents' AND batch_name = 'po';