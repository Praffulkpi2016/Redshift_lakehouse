DROP table IF EXISTS gold_bec_dwh.DIM_AR_SALESREPS;
CREATE TABLE gold_bec_dwh.DIM_AR_SALESREPS AS
(
  SELECT
    SALESREP_ID,
    RESOURCE_ID,
    SALES_CREDIT_TYPE_ID,
    NAME,
    STATUS,
    PERSON_ID,
    SALESREP_NUMBER,
    ORG_ID,
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
    ) || '-' || COALESCE(salesrep_id, 0) || '-' || COALESCE(org_id, 0) AS dw_load_id,
    CURRENT_TIMESTAMP() AS dw_insert_date,
    CURRENT_TIMESTAMP() AS dw_update_date
  FROM silver_bec_ods.JTF_RS_SALESREPS
);
UPDATE bec_etl_ctrl.batch_dw_info SET load_type = 'I', last_refresh_date = CURRENT_TIMESTAMP()
WHERE
  dw_table_name = 'dim_ar_salesreps' AND batch_name = 'ar';