/* Delete Records */
DELETE FROM gold_bec_dwh.DIM_AP_CHECKS
WHERE
  check_id IN (
    SELECT
      ods.check_id
    FROM gold_bec_dwh.dim_ap_checks AS dw, silver_bec_ods.ap_checks_all AS ods
    WHERE
      dw.dw_load_id = (
        SELECT
          system_id
        FROM bec_etl_ctrl.etlsourceappid
        WHERE
          source_system = 'EBS'
      ) || '-' || COALESCE(ods.CHECK_ID, 0)
      AND (
        ods.kca_seq_date > (
          SELECT
            (
              executebegints - prune_days
            )
          FROM bec_etl_ctrl.batch_dw_info
          WHERE
            dw_table_name = 'dim_ap_checks' AND batch_name = 'ap'
        )
      )
  );
/* Insert records */
INSERT INTO gold_bec_dwh.dim_ap_checks (
  amount,
  bank_account_id,
  bank_account_name,
  check_date,
  check_id,
  check_number,
  currency_code,
  payment_method_lookup_code,
  address_line1,
  address_line2,
  address_line3,
  checkrun_name,
  city,
  country,
  status_lookup_code,
  vendor_name,
  vendor_site_code,
  zip,
  supplier_bank_account,
  supplier_bank_type,
  supplier_bank_num,
  cleared_amount,
  cleared_date,
  cleared_base_amount,
  cleared_exchange_rate,
  cleared_exchange_date,
  org_id,
  vendor_id,
  vendor_site_id,
  exchange_rate,
  exchange_date,
  exchange_rate_type,
  payment_base_amount,
  checkrun_id,
  external_bank_account_id,
  void_date,
  legal_entity_id,
  remit_to_supplier_name,
  remit_to_supplier_site,
  doc_sequence_value,
  is_deleted_flg,
  source_app_id,
  dw_load_id,
  dw_insert_date,
  dw_update_date
)
(
  SELECT
    AMOUNT,
    BANK_ACCOUNT_ID,
    BANK_ACCOUNT_NAME,
    CHECK_DATE,
    CHECK_ID,
    CHECK_NUMBER,
    CURRENCY_CODE,
    PAYMENT_METHOD_CODE AS PAYMENT_METHOD_LOOKUP_CODE,
    ADDRESS_LINE1,
    ADDRESS_LINE2,
    ADDRESS_LINE3,
    CHECKRUN_NAME,
    CITY,
    COUNTRY,
    STATUS_LOOKUP_CODE,
    VENDOR_NAME,
    VENDOR_SITE_CODE,
    ZIP,
    BANK_ACCOUNT_NUM AS SUPPLIER_BANK_ACCOUNT,
    BANK_ACCOUNT_TYPE AS SUPPLIER_BANK_TYPE,
    BANK_NUM AS SUPPLIER_BANK_NUM,
    CLEARED_AMOUNT,
    CLEARED_DATE,
    CLEARED_BASE_AMOUNT,
    CLEARED_EXCHANGE_RATE,
    CLEARED_EXCHANGE_DATE,
    ORG_ID,
    VENDOR_ID,
    VENDOR_SITE_ID,
    EXCHANGE_RATE,
    EXCHANGE_DATE,
    EXCHANGE_RATE_TYPE,
    COALESCE(BASE_AMOUNT, AMOUNT) AS PAYMENT_BASE_AMOUNT,
    CHECKRUN_ID,
    EXTERNAL_BANK_ACCOUNT_ID,
    VOID_DATE,
    LEGAL_ENTITY_ID,
    REMIT_TO_SUPPLIER_NAME,
    REMIT_TO_SUPPLIER_SITE,
    DOC_SEQUENCE_VALUE,
    'N' AS is_deleted_flg, /* audit columns */
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
    ) || '-' || COALESCE(CHECK_ID, 0) AS dw_load_id,
    CURRENT_TIMESTAMP() AS dw_insert_date,
    CURRENT_TIMESTAMP() AS dw_update_date
  FROM silver_bec_ods.AP_CHECKS_ALL
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
          dw_table_name = 'dim_ap_checks' AND batch_name = 'ap'
      )
    )
);
/* Soft delete */
UPDATE gold_bec_dwh.dim_ap_checks SET is_deleted_flg = 'Y'
WHERE
  NOT check_id IN (
    SELECT
      ods.check_id
    FROM gold_bec_dwh.dim_ap_checks AS dw, silver_bec_ods.ap_checks_all AS ods
    WHERE
      dw.dw_load_id = (
        SELECT
          system_id
        FROM bec_etl_ctrl.etlsourceappid
        WHERE
          source_system = 'EBS'
      ) || '-' || COALESCE(ods.CHECK_ID, 0)
      AND ods.is_deleted_flg <> 'Y'
  );
UPDATE bec_etl_ctrl.batch_dw_info SET last_refresh_date = CURRENT_TIMESTAMP()
WHERE
  dw_table_name = 'dim_ap_checks' AND batch_name = 'ap';