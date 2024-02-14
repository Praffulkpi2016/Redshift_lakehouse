DROP table IF EXISTS gold_bec_dwh.DIM_AR_PAYMENT_METHODS;
CREATE TABLE gold_bec_dwh.DIM_AR_PAYMENT_METHODS AS
(
  SELECT
    ARM.RECEIPT_METHOD_ID AS RECEIPT_METHOD_ID,
    ARM.NAME AS PAYMENT_METHOD_NAME,
    ARM.RECEIPT_CLASS_ID AS RECEIPT_CLASS_ID,
    ARC.NAME AS RECEIPT_CLASS_NAME,
    ARC.CREATION_METHOD_CODE AS RECEIPT_CREATION_METHOD,
    ARC.REMIT_FLAG,
    ARC.CREATION_STATUS,
    ARC.REMIT_METHOD_CODE,
    ARM.last_update_date,
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
    ) || '-' || COALESCE(ARM.RECEIPT_METHOD_ID, 0) || '-' || COALESCE(ARM.RECEIPT_CLASS_ID, 0) AS dw_load_id,
    CURRENT_TIMESTAMP() AS dw_insert_date,
    CURRENT_TIMESTAMP() AS dw_update_date
  FROM silver_bec_ods.AR_RECEIPT_CLASSES AS ARC, silver_bec_ods.AR_RECEIPT_METHODS AS ARM
  WHERE
    1 = 1 AND ARC.RECEIPT_CLASS_ID = ARM.RECEIPT_CLASS_ID AND ARM.END_DATE IS NULL
);
UPDATE bec_etl_ctrl.batch_dw_info SET load_type = 'I', last_refresh_date = CURRENT_TIMESTAMP()
WHERE
  dw_table_name = 'dim_ar_payment_methods' AND batch_name = 'ar';