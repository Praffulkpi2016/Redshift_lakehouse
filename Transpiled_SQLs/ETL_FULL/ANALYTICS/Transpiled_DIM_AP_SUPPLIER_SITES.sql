DROP table IF EXISTS gold_bec_dwh.DIM_AP_SUPPLIER_SITES;
CREATE TABLE gold_bec_dwh.DIM_AP_SUPPLIER_SITES AS
(
  SELECT
    vendor_site_id,
    vendor_site_code,
    vendor_id,
    purchasing_site_flag,
    rfq_only_site_flag,
    pay_site_flag,
    attention_ar_flag,
    address_line1 AS `VENDOR_ADDRESS1`,
    address_line2 AS `VENDOR_ADDRESS2`,
    address_line3 AS `VENDOR_ADDRESS3`,
    address_line4 AS `VENDOR_ADDRESS4`,
    city AS `VENDOR_SITE_CITY`,
    zip,
    COALESCE(province, state) AS `VENDOR_SITE_STATE`,
    country AS `VENDOR_SITE_COUNTRY`,
    area_code || '-' || phone AS `VENDOR_SITE_PHONE`,
    fax_area_code || '-' || fax AS `VENDOR_SITE_FAX`,
    payment_currency_code,
    vendor_site_code_alt,
    ship_to_location_id,
    bill_to_location_id,
    org_id,
    location_id,
    party_site_id,
    tca_sync_state,
    freight_terms_lookup_code,
    inactive_date,
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
    ) || '-' || COALESCE(vendor_site_id, 0) AS dw_load_id,
    CURRENT_TIMESTAMP() AS dw_insert_date,
    CURRENT_TIMESTAMP() AS dw_update_date
  FROM silver_bec_ods.ap_supplier_sites_all
);
UPDATE bec_etl_ctrl.batch_dw_info SET load_type = 'I', last_refresh_date = CURRENT_TIMESTAMP()
WHERE
  dw_table_name = 'dim_ap_supplier_sites' AND batch_name = 'ap';