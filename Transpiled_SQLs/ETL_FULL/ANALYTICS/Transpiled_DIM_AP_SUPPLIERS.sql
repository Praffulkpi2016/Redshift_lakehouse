DROP table IF EXISTS gold_bec_dwh.DIM_AP_SUPPLIERS;
CREATE TABLE gold_bec_dwh.DIM_AP_SUPPLIERS AS
(
  SELECT
    vendor_id,
    vendor_name,
    segment1 AS vendor_number,
    vendor_name_alt,
    vendor_type_lookup_code,
    enabled_flag,
    employee_id,
    terms_id,
    invoice_currency_code,
    payment_currency_code,
    start_date_active,
    end_date_active,
    party_id,
    TRIM(attribute2) AS vendor_category,
    created_by,
    last_updated_by,
    creation_date,
    last_update_date,
    num_1099,
    type_1099,
    RECEIPT_REQUIRED_FLAG,
    INSPECTION_REQUIRED_FLAG,
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
    ) || '-' || COALESCE(vendor_id, 0) AS dw_load_id,
    CURRENT_TIMESTAMP() AS dw_insert_date,
    CURRENT_TIMESTAMP() AS dw_update_date
  FROM silver_bec_ods.ap_suppliers
);
UPDATE bec_etl_ctrl.batch_dw_info SET load_type = 'I', last_refresh_date = CURRENT_TIMESTAMP()
WHERE
  dw_table_name = 'dim_ap_suppliers' AND batch_name = 'ap';