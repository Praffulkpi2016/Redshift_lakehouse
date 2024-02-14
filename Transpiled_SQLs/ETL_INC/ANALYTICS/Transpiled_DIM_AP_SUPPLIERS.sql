/* Delete Records */
DELETE FROM gold_bec_dwh.dim_ap_suppliers
WHERE
  COALESCE(vendor_id, 0) IN (
    SELECT
      COALESCE(ods.vendor_id, 0)
    FROM gold_bec_dwh.dim_ap_suppliers AS dw, silver_bec_ods.ap_suppliers AS ods
    WHERE
      dw.dw_load_id = (
        SELECT
          system_id
        FROM bec_etl_ctrl.etlsourceappid
        WHERE
          source_system = 'EBS'
      ) || '-' || COALESCE(ods.vendor_id, 0)
      AND (
        ods.kca_seq_date > (
          SELECT
            (
              executebegints - prune_days
            )
          FROM bec_etl_ctrl.batch_dw_info
          WHERE
            dw_table_name = 'dim_ap_suppliers' AND batch_name = 'ap'
        )
      )
  );
/* Insert records */
INSERT INTO gold_bec_dwh.DIM_AP_SUPPLIERS (
  vendor_id,
  vendor_name,
  vendor_number,
  vendor_name_alt,
  vendor_category,
  vendor_type_lookup_code,
  enabled_flag,
  employee_id,
  terms_id,
  invoice_currency_code,
  payment_currency_code,
  start_date_active,
  end_date_active,
  party_id,
  created_by,
  last_updated_by,
  creation_date,
  last_update_date,
  num_1099,
  type_1099,
  RECEIPT_REQUIRED_FLAG,
  INSPECTION_REQUIRED_FLAG,
  is_deleted_flg,
  source_app_id,
  dw_load_id,
  dw_insert_date,
  dw_update_date
)
(
  SELECT
    vendor_id,
    vendor_name,
    segment1 AS vendor_number,
    vendor_name_alt,
    TRIM(attribute2) AS vendor_category,
    vendor_type_lookup_code,
    enabled_flag,
    employee_id,
    terms_id,
    invoice_currency_code,
    payment_currency_code,
    start_date_active,
    end_date_active,
    party_id,
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
          dw_table_name = 'dim_ap_suppliers' AND batch_name = 'ap'
      )
    )
);
/* Soft delete */
UPDATE gold_bec_dwh.dim_ap_suppliers SET is_deleted_flg = 'Y'
WHERE
  NOT COALESCE(vendor_id, 0) IN (
    SELECT
      COALESCE(ods.vendor_id, 0)
    FROM gold_bec_dwh.dim_ap_suppliers AS dw, silver_bec_ods.ap_suppliers AS ods
    WHERE
      dw.dw_load_id = (
        SELECT
          system_id
        FROM bec_etl_ctrl.etlsourceappid
        WHERE
          source_system = 'EBS'
      ) || '-' || COALESCE(ods.vendor_id, 0)
      AND ods.is_deleted_flg <> 'Y'
  );
UPDATE bec_etl_ctrl.batch_dw_info SET last_refresh_date = CURRENT_TIMESTAMP()
WHERE
  dw_table_name = 'dim_ap_suppliers' AND batch_name = 'ap';