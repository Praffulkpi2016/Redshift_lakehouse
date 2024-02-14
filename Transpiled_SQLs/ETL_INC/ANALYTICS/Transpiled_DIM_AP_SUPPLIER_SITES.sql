/* Delete Records */
DELETE FROM gold_bec_dwh.dim_ap_supplier_sites
WHERE
  COALESCE(vendor_site_id, 0) IN (
    SELECT
      COALESCE(ods.vendor_site_id, 0)
    FROM gold_bec_dwh.dim_ap_supplier_sites AS dw, silver_bec_ods.ap_supplier_sites_all AS ods
    WHERE
      dw.dw_load_id = (
        SELECT
          system_id
        FROM bec_etl_ctrl.etlsourceappid
        WHERE
          source_system = 'EBS'
      ) || '-' || COALESCE(ods.vendor_site_id, 0)
      AND (
        ods.kca_seq_date > (
          SELECT
            (
              executebegints - prune_days
            )
          FROM bec_etl_ctrl.batch_dw_info
          WHERE
            dw_table_name = 'dim_ap_supplier_sites' AND batch_name = 'ap'
        )
      )
  );
/* Insert records */
INSERT INTO gold_bec_dwh.DIM_AP_SUPPLIER_SITES (
  vendor_site_id,
  vendor_site_code,
  vendor_id,
  purchasing_site_flag,
  rfq_only_site_flag,
  pay_site_flag,
  attention_ar_flag,
  vendor_address1,
  vendor_address2,
  vendor_address3,
  vendor_address4,
  vendor_site_city,
  vendor_site_state,
  vendor_site_country,
  ZIP,
  vendor_site_phone,
  vendor_site_fax,
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
  is_deleted_flg,
  source_app_id,
  dw_load_id,
  dw_insert_date,
  dw_update_date
)
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
    COALESCE(province, state) AS `VENDOR_SITE_STATE`,
    country AS `VENDOR_SITE_COUNTRY`,
    ZIP,
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
          dw_table_name = 'dim_ap_supplier_sites' AND batch_name = 'ap'
      )
    )
);
/* Soft delete */
UPDATE gold_bec_dwh.dim_ap_supplier_sites SET is_deleted_flg = 'Y'
WHERE
  NOT COALESCE(vendor_site_id, 0) IN (
    SELECT
      COALESCE(ods.vendor_site_id, 0)
    FROM gold_bec_dwh.dim_ap_supplier_sites AS dw, silver_bec_ods.ap_supplier_sites_all AS ods
    WHERE
      dw.dw_load_id = (
        SELECT
          system_id
        FROM bec_etl_ctrl.etlsourceappid
        WHERE
          source_system = 'EBS'
      ) || '-' || COALESCE(ods.vendor_site_id, 0)
      AND ods.is_deleted_flg <> 'Y'
  );
UPDATE bec_etl_ctrl.batch_dw_info SET last_refresh_date = CURRENT_TIMESTAMP()
WHERE
  dw_table_name = 'dim_ap_supplier_sites' AND batch_name = 'ap';