/* Delete Records */
DELETE FROM gold_bec_dwh.DIM_WIP_JOBS
WHERE
  (
    COALESCE(WIP_ENTITY_ID, 0)
  ) IN (
    SELECT
      ods.WIP_ENTITY_ID
    FROM gold_bec_dwh.DIM_WIP_JOBS AS dw, (
      SELECT
        COALESCE(WIP_ENTITY_ID, 0) AS WIP_ENTITY_ID
      FROM silver_bec_ods.WIP_ENTITIES
      WHERE
        1 = 1
        AND (
          kca_seq_date > (
            SELECT
              (
                executebegints - prune_days
              )
            FROM bec_etl_ctrl.batch_dw_info
            WHERE
              dw_table_name = 'dim_wip_jobs' AND batch_name = 'wip'
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
      ) || '-' || COALESCE(ods.WIP_ENTITY_ID, 0)
  );
/* Insert records */
INSERT INTO gold_bec_dwh.DIM_WIP_JOBS (
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
  is_deleted_flg,
  source_app_id,
  dw_load_id,
  dw_insert_date,
  dw_update_date
)
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
  WHERE
    1 = 1
    AND (
      kca_seq_date > (
        SELECT
          (
            executebegints - prune_days
          )
        FROM bec_etl_ctrl.batch_dw_info
        WHERE
          dw_table_name = 'dim_wip_jobs' AND batch_name = 'wip'
      )
    )
);
/* Soft delete */
UPDATE gold_bec_dwh.DIM_WIP_JOBS SET is_deleted_flg = 'Y'
WHERE
  NOT (
    COALESCE(WIP_ENTITY_ID, 0)
  ) IN (
    SELECT
      COALESCE(ext.WIP_ENTITY_ID, 0) AS WIP_ENTITY_ID
    FROM gold_bec_dwh.DIM_WIP_JOBS AS dw, silver_bec_ods.WIP_ENTITIES AS ext
    WHERE
      dw.dw_load_id = (
        SELECT
          system_id
        FROM bec_etl_ctrl.etlsourceappid
        WHERE
          source_system = 'EBS'
      ) || '-' || COALESCE(ext.WIP_ENTITY_ID, 0)
      AND ext.is_deleted_flg <> 'Y'
  );
UPDATE bec_etl_ctrl.batch_dw_info SET load_type = 'I', last_refresh_date = CURRENT_TIMESTAMP()
WHERE
  dw_table_name = 'dim_wip_jobs' AND batch_name = 'wip';