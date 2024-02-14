DROP table IF EXISTS gold_bec_dwh.DIM_AR_COLLECTORS;
CREATE TABLE gold_bec_dwh.DIM_AR_COLLECTORS AS
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
  FROM BEC_ODS.AR_COLLECTORS
);
UPDATE bec_etl_ctrl.batch_dw_info SET load_type = 'I', last_refresh_date = CURRENT_TIMESTAMP()
WHERE
  dw_table_name = 'dim_ar_collectors' AND batch_name = 'ar';