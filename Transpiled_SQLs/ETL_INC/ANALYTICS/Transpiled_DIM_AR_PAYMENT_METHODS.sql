/* Delete Records */
DELETE FROM gold_bec_dwh.DIM_AR_PAYMENT_METHODS
WHERE
  (COALESCE(RECEIPT_METHOD_ID, 0), COALESCE(RECEIPT_CLASS_ID, 0)) IN (
    SELECT
      COALESCE(ods.RECEIPT_METHOD_ID, 0) AS RECEIPT_METHOD_ID,
      COALESCE(ods.RECEIPT_CLASS_ID, 0) AS RECEIPT_CLASS_ID
    FROM gold_bec_dwh.DIM_AR_PAYMENT_METHODS AS dw, (
      SELECT
        ARM.RECEIPT_METHOD_ID,
        ARC.RECEIPT_CLASS_ID
      FROM silver_bec_ods.AR_RECEIPT_CLASSES AS ARC, silver_bec_ods.AR_RECEIPT_METHODS AS ARM
      WHERE
        ARC.RECEIPT_CLASS_ID = ARM.RECEIPT_CLASS_ID
        AND ARM.END_DATE IS NULL
        AND (
          ARM.kca_seq_date > (
            SELECT
              (
                executebegints - prune_days
              )
            FROM bec_etl_ctrl.batch_dw_info
            WHERE
              dw_table_name = 'dim_ar_payment_methods' AND batch_name = 'ar'
          )
          OR ARC.kca_seq_date > (
            SELECT
              (
                executebegints - prune_days
              )
            FROM bec_etl_ctrl.batch_dw_info
            WHERE
              dw_table_name = 'dim_ar_payment_methods' AND batch_name = 'ar'
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
      ) || '-' || COALESCE(ods.RECEIPT_METHOD_ID, 0) || '-' || COALESCE(ods.RECEIPT_CLASS_ID, 0)
  );
/* Insert records */
INSERT INTO gold_bec_dwh.DIM_AR_PAYMENT_METHODS (
  receipt_method_id,
  payment_method_name,
  receipt_class_id,
  receipt_class_name,
  receipt_creation_method,
  remit_flag,
  creation_status,
  remit_method_code,
  last_update_date,
  is_deleted_flg,
  source_app_id,
  dw_load_id,
  dw_insert_date,
  dw_update_date
)
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
    1 = 1
    AND ARC.RECEIPT_CLASS_ID = ARM.RECEIPT_CLASS_ID
    AND ARM.END_DATE IS NULL
    AND (
      ARM.kca_seq_date > (
        SELECT
          (
            executebegints - prune_days
          )
        FROM bec_etl_ctrl.batch_dw_info
        WHERE
          dw_table_name = 'dim_ar_payment_methods' AND batch_name = 'ar'
      )
      OR ARC.kca_seq_date > (
        SELECT
          (
            executebegints - prune_days
          )
        FROM bec_etl_ctrl.batch_dw_info
        WHERE
          dw_table_name = 'dim_ar_payment_methods' AND batch_name = 'ar'
      )
    )
);
/* Soft delete */
UPDATE gold_bec_dwh.DIM_AR_PAYMENT_METHODS SET is_deleted_flg = 'Y'
WHERE
  NOT (RECEIPT_METHOD_ID, RECEIPT_CLASS_ID) IN (
    SELECT
      ods.RECEIPT_METHOD_ID,
      ods.RECEIPT_CLASS_ID
    FROM gold_bec_dwh.DIM_AR_PAYMENT_METHODS AS dw, (
      SELECT
        ARM.RECEIPT_METHOD_ID,
        ARC.RECEIPT_CLASS_ID
      FROM (
        SELECT
          *
        FROM silver_bec_ods.AR_RECEIPT_CLASSES
        WHERE
          is_deleted_flg <> 'Y'
      ) AS ARC, (
        SELECT
          *
        FROM silver_bec_ods.AR_RECEIPT_METHODS
        WHERE
          is_deleted_flg <> 'Y'
      ) AS ARM
      WHERE
        ARC.RECEIPT_CLASS_ID = ARM.RECEIPT_CLASS_ID AND ARM.END_DATE IS NULL
    ) AS ods
    WHERE
      dw.dw_load_id = (
        SELECT
          system_id
        FROM bec_etl_ctrl.etlsourceappid
        WHERE
          source_system = 'EBS'
      ) || '-' || COALESCE(ods.RECEIPT_METHOD_ID, 0) || '-' || COALESCE(ods.RECEIPT_CLASS_ID, 0)
  );
UPDATE bec_etl_ctrl.batch_dw_info SET last_refresh_date = CURRENT_TIMESTAMP()
WHERE
  dw_table_name = 'dim_ar_payment_methods' AND batch_name = 'ar';