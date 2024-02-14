/* Delete Records */
DELETE FROM gold_bec_dwh.dim_ap_invoice
WHERE
  INVOICE_ID IN (
    SELECT
      ods.INVOICE_ID
    FROM gold_bec_dwh.dim_ap_invoice AS dw, silver_bec_ods.AP_INVOICES_ALL AS ods
    WHERE
      dw.dw_load_id = (
        SELECT
          system_id
        FROM bec_etl_ctrl.etlsourceappid
        WHERE
          source_system = 'EBS'
      ) || '-' || COALESCE(ods.INVOICE_ID, 0)
      AND (
        ods.kca_seq_date > (
          SELECT
            (
              executebegints - prune_days
            )
          FROM bec_etl_ctrl.batch_dw_info
          WHERE
            dw_table_name = 'dim_ap_invoice' AND batch_name = 'ap'
        )
      )
  );
/* Insert records */
INSERT INTO gold_bec_dwh.DIM_AP_INVOICE (
  INVOICE_ID,
  INVOICE_NUM,
  INVOICE_CURRENCY_CODE,
  PAYMENT_CURRENCY_CODE,
  INVOICE_DATE,
  SOURCE,
  INVOICE_TYPE_LOOKUP_CODE,
  DESCRIPTION,
  TAX_AMOUNT,
  PAYMENT_STATUS_FLAG,
  INVOICE_RECEIVED_DATE,
  CREATION_DATE,
  LAST_UPDATE_DATE,
  ORG_ID,
  GL_DATE,
  REMIT_TO_SUPPLIER_NAME,
  REMIT_TO_SUPPLIER_SITE,
  PAYMENT_METHOD_LOOKUP_CODE,
  PAY_GROUP_LOOKUP_CODE,
  ACCTS_PAY_CODE_COMBINATION_ID,
  PREPAY_FLAG,
  EXPENDITURE_TYPE,
  CREATED_BY,
  is_deleted_flg,
  source_app_id,
  dw_load_id,
  dw_insert_date,
  dw_update_date
)
(
  SELECT
    INVOICE_ID,
    INVOICE_NUM,
    INVOICE_CURRENCY_CODE,
    PAYMENT_CURRENCY_CODE,
    INVOICE_DATE,
    SOURCE,
    INVOICE_TYPE_LOOKUP_CODE,
    DESCRIPTION,
    TAX_AMOUNT,
    PAYMENT_STATUS_FLAG,
    INVOICE_RECEIVED_DATE,
    CREATION_DATE,
    LAST_UPDATE_DATE,
    ORG_ID,
    GL_DATE,
    REMIT_TO_SUPPLIER_NAME,
    REMIT_TO_SUPPLIER_SITE,
    PAYMENT_METHOD_CODE AS PAYMENT_METHOD_LOOKUP_CODE,
    PAY_GROUP_LOOKUP_CODE,
    ACCTS_PAY_CODE_COMBINATION_ID,
    PREPAY_FLAG,
    EXPENDITURE_TYPE,
    CREATED_BY,
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
    ) || '-' || COALESCE(INVOICE_ID, 0) AS dw_load_id,
    CURRENT_TIMESTAMP() AS dw_insert_date,
    CURRENT_TIMESTAMP() AS dw_update_date
  FROM silver_bec_ods.AP_INVOICES_ALL
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
          dw_table_name = 'dim_ap_invoice' AND batch_name = 'ap'
      )
    )
);
/* Soft delete */
UPDATE gold_bec_dwh.dim_ap_invoice SET is_deleted_flg = 'Y'
WHERE
  NOT INVOICE_ID IN (
    SELECT
      ods.INVOICE_ID
    FROM gold_bec_dwh.dim_ap_invoice AS dw, silver_bec_ods.AP_INVOICES_ALL AS ods
    WHERE
      dw.dw_load_id = (
        SELECT
          system_id
        FROM bec_etl_ctrl.etlsourceappid
        WHERE
          source_system = 'EBS'
      ) || '-' || COALESCE(ods.INVOICE_ID, 0)
      AND ods.is_deleted_flg <> 'Y'
  );
UPDATE bec_etl_ctrl.batch_dw_info SET last_refresh_date = CURRENT_TIMESTAMP()
WHERE
  dw_table_name = 'dim_ap_invoice' AND batch_name = 'ap';