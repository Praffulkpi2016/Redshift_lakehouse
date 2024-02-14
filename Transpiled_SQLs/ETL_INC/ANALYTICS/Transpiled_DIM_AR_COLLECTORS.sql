/* Delete Records */
DELETE FROM gold_bec_dwh.DIM_AR_COLLECTORS
WHERE
  COALESCE(COLLECTOR_ID, 0) IN (
    SELECT
      COALESCE(ods.COLLECTOR_ID, 0)
    FROM gold_bec_dwh.DIM_AR_COLLECTORS AS dw, silver_bec_ods.AR_COLLECTORS AS ods
    WHERE
      dw.dw_load_id = (
        SELECT
          system_id
        FROM bec_etl_ctrl.etlsourceappid
        WHERE
          source_system = 'EBS'
      ) || '-' || COALESCE(ods.COLLECTOR_ID, 0)
      AND (
        ods.kca_seq_date > (
          SELECT
            (
              executebegints - prune_days
            )
          FROM bec_etl_ctrl.batch_dw_info
          WHERE
            dw_table_name = 'dim_ar_collectors' AND batch_name = 'ar'
        )
      )
  );
/* Insert records */
INSERT INTO gold_bec_dwh.DIM_AR_COLLECTORS (
  collector_id,
  `name`,
  employee_id,
  description,
  status,
  inactive_date,
  alias,
  telephone_number,
  resource_id,
  resource_type,
  last_update_date,
  is_deleted_flg,
  source_app_id,
  dw_load_id,
  dw_insert_date,
  dw_update_date
)
(
  SELECT
    COLLECTOR_ID,
    NAME,
    EMPLOYEE_ID,
    DESCRIPTION,
    STATUS,
    INACTIVE_DATE,
    ALIAS,
    TELEPHONE_NUMBER,
    RESOURCE_ID,
    RESOURCE_TYPE,
    last_update_date,
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
    ) || '-' || COALESCE(COLLECTOR_ID, 0) AS dw_load_id,
    CURRENT_TIMESTAMP() AS dw_insert_date,
    CURRENT_TIMESTAMP() AS dw_update_date
  FROM silver_bec_ods.AR_COLLECTORS
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
          dw_table_name = 'dim_ar_collectors' AND batch_name = 'ar'
      )
    )
);
/* Soft delete */
UPDATE gold_bec_dwh.DIM_AR_COLLECTORS SET is_deleted_flg = 'Y'
WHERE
  NOT COALESCE(COLLECTOR_ID, 0) IN (
    SELECT
      COALESCE(ods.COLLECTOR_ID, 0)
    FROM gold_bec_dwh.DIM_AR_COLLECTORS AS dw, silver_bec_ods.AR_COLLECTORS AS ods
    WHERE
      dw.dw_load_id = (
        SELECT
          system_id
        FROM bec_etl_ctrl.etlsourceappid
        WHERE
          source_system = 'EBS'
      ) || '-' || COALESCE(ods.COLLECTOR_ID, 0)
      AND ods.is_deleted_flg <> 'Y'
  );
UPDATE bec_etl_ctrl.batch_dw_info SET last_refresh_date = CURRENT_TIMESTAMP()
WHERE
  dw_table_name = 'dim_ar_collectors' AND batch_name = 'ar';