DROP table IF EXISTS gold_bec_dwh.DIM_AR_CUST_ADDRESS;
CREATE TABLE gold_bec_dwh.dim_ar_cust_address AS
SELECT DISTINCT
  CT.customer_trx_id,
  ct.trx_number,
  hca_shipto.party_id,
  ct.bill_to_site_use_id,
  ct.ship_to_site_use_id,
  ct.bill_to_customer_id,
  ct.ship_to_customer_id,
  bhcsua_shipto.cust_acct_site_id AS bill_to_cust_acct_site_id,
  hcsua_shipto.cust_acct_site_id AS ship_to_cust_acct_site_id,
  bhcasa_shipto.party_site_id AS bill_to_party_site_id,
  hcasa_shipto.party_site_id AS ship_to_party_site_id,
  bhps.location_id AS bill_to_location_id,
  hps.location_id AS ship_to_location_id,
  hca_shipto.account_number AS ship_to_account,
  hz_shipto.party_name AS ship_to_customer_name,
  hl.address1 AS ship_to_address1,
  hl.address2 AS ship_to_address2,
  hl.address3 AS ship_to_address3,
  hl.country AS ship_to_country,
  hl.state AS ship_state,
  hl.city AS ship_city,
  hl.postal_code AS ship_zip,
  hca_billto.account_number AS bill_to_account,
  hz_billto.party_name AS bill_to_customer_name,
  Bhl.address1 AS Bill_to_address1,
  Bhl.address2 AS Bill_to_address2,
  Bhl.address3 AS Bill_to_address3,
  Bhl.country AS Bill_to_country,
  Bhl.state AS Bill_State,
  Bhl.city AS Bill_City,
  Bhl.postal_code AS Bill_Zip,
  COALESCE(hz_shipto.party_name, hz_billto.party_name) AS customer_name,
  COALESCE(hz_shipto.party_number, hz_billto.party_number) AS customer_number,
  COALESCE(hca_shipto.account_number, hca_billto.account_number) AS customer_account_number,
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
  ) || '-' || COALESCE(ct.customer_trx_id, 0) || '-' || COALESCE(ct.bill_to_customer_id, 0) || '-' || COALESCE(ct.ship_to_customer_id, 0) || '-' || COALESCE(hca_shipto.party_id, 0) AS dw_load_id,
  CURRENT_TIMESTAMP() AS dw_insert_date,
  CURRENT_TIMESTAMP() AS dw_update_date
FROM silver_bec_ods.ra_customer_trx_all AS ct, silver_bec_ods.hz_cust_accounts AS hca_billto, silver_bec_ods.hz_parties AS hz_billto, silver_bec_ods.hz_cust_accounts AS hca_shipto, silver_bec_ods.hz_cust_acct_sites_all AS hcasa_shipto, silver_bec_ods.hz_cust_site_uses_all AS hcsua_shipto, silver_bec_ods.hz_parties AS hz_shipto, silver_bec_ods.hz_cust_accounts AS lhca_shipto, silver_bec_ods.hz_party_sites AS hps, silver_bec_ods.hz_locations AS hl, silver_bec_ods.hz_cust_acct_sites_all AS bhcasa_shipto, silver_bec_ods.hz_cust_site_uses_all AS bhcsua_shipto, silver_bec_ods.hz_party_sites AS bhps, silver_bec_ods.hz_locations AS bhl
WHERE
  1 = 1
  AND ct.bill_to_customer_id = hca_billto.cust_account_id
  AND hca_billto.party_id = hz_billto.party_id
  AND ct.ship_to_customer_id = hca_shipto.CUST_ACCOUNT_ID()
  AND hca_shipto.party_id = hz_shipto.PARTY_ID()
  AND ct.ship_to_site_use_id = hcsua_shipto.SITE_USE_ID()
  AND hcsua_shipto.SITE_USE_CODE() = 'SHIP_TO'
  AND hcsua_shipto.cust_acct_site_id = hcasa_shipto.CUST_ACCT_SITE_ID()
  AND hcasa_shipto.party_site_id = hps.PARTY_SITE_ID()
  AND hps.location_id = hl.LOCATION_ID()
  AND ct.bill_to_site_use_id = bhcsua_shipto.SITE_USE_ID()
  AND Bhcsua_shipto.SITE_USE_CODE() = 'BILL_TO'
  AND bhcsua_shipto.cust_acct_site_id = bhcasa_shipto.CUST_ACCT_SITE_ID()
  AND bhcasa_shipto.party_site_id = bhps.PARTY_SITE_ID()
  AND bhps.location_id = bhl.LOCATION_ID();
UPDATE bec_etl_ctrl.batch_dw_info SET load_type = 'I', last_refresh_date = CURRENT_TIMESTAMP()
WHERE
  dw_table_name = 'dim_ar_cust_address' AND batch_name = 'ar';