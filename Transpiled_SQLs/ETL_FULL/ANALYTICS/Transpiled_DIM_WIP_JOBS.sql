DROP table IF EXISTS gold_bec_dwh.DIM_WIP_JOBS;
CREATE TABLE gold_bec_dwh.DIM_WIP_JOBS AS
(
  SELECT
    WIP_ENTITY_ID,
    ORGANIZATION_ID,
    LAST_UPDATE_DATE,
    LAST_UPDATED_BY,
    CREATION_DATE,
    CREATED_BY,
    LAST_UPDATE_LOGIN,
    REQUEST_ID,
    PROGRAM_APPLICATION_ID,
    PROGRAM_ID,
    PROGRAM_UPDATE_DATE,
    WIP_ENTITY_NAME,
    ENTITY_TYPE,
    DESCRIPTION,
    PRIMARY_ITEM_ID,
    GEN_OBJECT_ID,
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
    ) || '-' || COALESCE(WIP_ENTITY_ID, 0) AS dw_load_id,
    CURRENT_TIMESTAMP() AS dw_insert_date,
    CURRENT_TIMESTAMP() AS dw_update_date
  FROM silver_bec_ods.WIP_ENTITIES
);
UPDATE bec_etl_ctrl.batch_dw_info SET load_type = 'I', last_refresh_date = CURRENT_TIMESTAMP()
WHERE
  dw_table_name = 'dim_wip_jobs' AND batch_name = 'wip';