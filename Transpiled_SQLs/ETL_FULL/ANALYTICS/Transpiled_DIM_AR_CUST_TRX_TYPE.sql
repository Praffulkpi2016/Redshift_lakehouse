DROP table IF EXISTS gold_bec_dwh.DIM_AR_CUST_TRX_TYPE;
CREATE TABLE gold_bec_dwh.DIM_AR_CUST_TRX_TYPE AS
(
  SELECT
    CUST_TRX_TYPE_ID,
    LAST_UPDATE_DATE,
    STATUS,
    NAME AS CUST_TRX_TYPE_NAME,
    DESCRIPTION AS CUST_TRX_TYPE_DESCRIPTION,
    `TYPE` AS CUST_TRX_TYPE,
    SET_OF_BOOKS_ID,
    END_DATE,
    START_DATE,
    ORG_ID,
    LEGAL_ENTITY_ID,
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
    ) || '-' || COALESCE(CUST_TRX_TYPE_ID, 0) || '-' || COALESCE(ORG_ID, 0) AS dw_load_id,
    CURRENT_TIMESTAMP() AS dw_insert_date,
    CURRENT_TIMESTAMP() AS dw_update_date
  FROM silver_bec_ods.RA_CUST_TRX_TYPES_ALL
);
UPDATE bec_etl_ctrl.batch_dw_info SET load_type = 'I', last_refresh_date = CURRENT_TIMESTAMP()
WHERE
  dw_table_name = 'dim_ar_cust_trx_type' AND batch_name = 'ar';