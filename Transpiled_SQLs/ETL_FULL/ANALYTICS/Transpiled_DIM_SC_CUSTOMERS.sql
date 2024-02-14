DROP table IF EXISTS gold_bec_dwh.DIM_SC_CUSTOMERS;
CREATE TABLE gold_bec_dwh.DIM_SC_CUSTOMERS AS
(
  SELECT
    HP.PARTY_NAME AS Customer_name,
    hl.address1 AS Site_id,
    COALESCE(hl.address1, 'NA') || ' ' || COALESCE(hl.address2, 'NA') || ' ' || COALESCE(hl.city, 'NA') || ' ' || COALESCE(hl.county, 'NA') || ' ' || COALESCE(hl.state, 'NA') || ' ' || COALESCE(hl.country, 'NA') || ' ' || COALESCE(hl.postal_code, 'NA') AS Site_Address,
    hl.address_lines_phonetic AS geo_code,
    hcsu.site_use_id,
    hcas.org_id,
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
    ) || '-' || COALESCE(site_use_id, 0) AS dw_load_id,
    CURRENT_TIMESTAMP() AS dw_insert_date,
    CURRENT_TIMESTAMP() AS dw_update_date
  FROM silver_bec_ods.hz_parties AS hp, silver_bec_ods.hz_party_sites AS hps, silver_bec_ods.hz_cust_accounts AS hca, silver_bec_ods.hz_cust_acct_sites_all AS hcas, silver_bec_ods.hz_cust_site_uses_all AS hcsu, silver_bec_ods.hz_locations AS hl
  WHERE
    1 = 1
    AND hp.party_id = hca.party_id
    AND hca.cust_account_id = hcas.CUST_ACCOUNT_ID()
    AND hps.PARTY_SITE_ID() = hcas.party_site_id
    AND hcas.cust_acct_site_id = hcsu.cust_acct_site_id
    AND hps.location_id = hl.LOCATION_ID()
    AND hp.party_type = 'ORGANIZATION'
    AND hp.status = 'A'
);
UPDATE bec_etl_ctrl.batch_dw_info SET load_type = 'I', last_refresh_date = CURRENT_TIMESTAMP()
WHERE
  dw_table_name = 'dim_sc_customers' AND batch_name = 'sc';