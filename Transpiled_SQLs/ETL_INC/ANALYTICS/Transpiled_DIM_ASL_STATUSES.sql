/* Delete Records */
DELETE FROM gold_bec_dwh.DIM_ASL_STATUSES
WHERE
  (
    COALESCE(status_id, 0)
  ) IN (
    SELECT
      COALESCE(ods.status_id, 0) AS status_id
    FROM gold_bec_dwh.dim_asl_statuses AS dw, (
      SELECT
        status_id
      FROM silver_bec_ods.po_asl_statuses
      WHERE
        kca_seq_date > (
          SELECT
            (
              executebegints - prune_days
            )
          FROM bec_etl_ctrl.batch_dw_info
          WHERE
            dw_table_name = 'dim_asl_statuses' AND batch_name = 'po'
        )
    ) AS ods
    WHERE
      dw.dw_load_id = (
        SELECT
          system_id
        FROM bec_etl_ctrl.etlsourceappid
        WHERE
          source_system = 'EBS'
      ) || '-' || COALESCE(ods.status_id, 0)
  );
/* Insert records */
INSERT INTO gold_bec_dwh.dim_asl_statuses (
  status_id,
  status,
  STATUS_DESCRIPTION,
  ASL_DEFAULT_FLAG,
  creation_date,
  CREATED_BY,
  LAST_UPDATE_DATE,
  LAST_UPDATED_BY,
  LAST_UPDATE_LOGIN,
  is_deleted_flg,
  SOURCE_APP_ID,
  DW_LOAD_ID,
  DW_INSERT_DATE,
  DW_UPDATE_DATE
)
(
  SELECT
    status_id,
    status,
    STATUS_DESCRIPTION,
    ASL_DEFAULT_FLAG,
    creation_date,
    CREATED_BY,
    LAST_UPDATE_DATE,
    LAST_UPDATED_BY,
    LAST_UPDATE_LOGIN,
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
    ) || '-' || COALESCE(status_id, 0) AS dw_load_id,
    CURRENT_TIMESTAMP() AS dw_insert_date,
    CURRENT_TIMESTAMP() AS dw_update_date
  FROM silver_bec_ods.po_asl_statuses
  WHERE
    kca_seq_date > (
      SELECT
        (
          executebegints - prune_days
        )
      FROM bec_etl_ctrl.batch_dw_info
      WHERE
        dw_table_name = 'dim_asl_statuses' AND batch_name = 'po'
    )
);
/* Soft delete */
UPDATE gold_bec_dwh.dim_asl_statuses SET is_deleted_flg = 'Y'
WHERE
  NOT (
    COALESCE(status_id, 0)
  ) IN (
    SELECT
      COALESCE(ods.status_id, 0) AS status_id
    FROM gold_bec_dwh.dim_asl_statuses AS dw, (
      SELECT
        *
      FROM silver_bec_ods.po_asl_statuses
    ) AS ods
    WHERE
      dw.dw_load_id = (
        SELECT
          system_id
        FROM bec_etl_ctrl.etlsourceappid
        WHERE
          source_system = 'EBS'
      ) || '-' || COALESCE(ods.status_id, 0)
      AND ods.is_deleted_flg <> 'Y'
  );
UPDATE bec_etl_ctrl.batch_dw_info SET last_refresh_date = CURRENT_TIMESTAMP()
WHERE
  dw_table_name = 'dim_asl_statuses' AND batch_name = 'po';