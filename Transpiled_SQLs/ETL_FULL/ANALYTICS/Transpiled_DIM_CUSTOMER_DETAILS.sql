DROP table IF EXISTS gold_bec_dwh.DIM_CUSTOMER_DETAILS;
CREATE TABLE gold_bec_dwh.DIM_CUSTOMER_DETAILS AS
(
  SELECT
    HP.PARTY_NAME,
    HP.PARTY_NUMBER,
    HPS.PARTY_SITE_NUMBER,
    HPS.PARTY_SITE_ID,
    HL.ADDRESS1,
    HL.ADDRESS2,
    HL.ADDRESS3,
    HL.ADDRESS4,
    CASE WHEN HL.CITY IS NULL THEN NULL ELSE HL.CITY || ', ' END || CASE WHEN HL.STATE IS NULL THEN HL.PROVINCE || ', ' ELSE HL.STATE || ', ' END || CASE WHEN HL.POSTAL_CODE IS NULL THEN NULL ELSE HL.POSTAL_CODE || ', ' END || CASE WHEN HL.COUNTRY IS NULL THEN NULL ELSE HL.COUNTRY END AS ADDRESS5,
    HL.COUNTY,
    HL.CITY,
    HL.COUNTRY,
    HL.STATE,
    HL.POSTAL_CODE,
    HCA.CUST_ACCOUNT_ID,
    HCSU.SITE_USE_ID,
    HCA.ACCOUNT_NUMBER,
    HCSU.LOCATION,
    HCSU.SITE_USE_CODE,
    HCSU.ORIG_SYSTEM_REFERENCE,
    HCS.CUST_ACCT_SITE_ID,
    'N' AS IS_DELETED_FLG,
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
    ) || '-' || COALESCE(HCS.CUST_ACCT_SITE_ID, 0) || '-' || COALESCE(HCSU.SITE_USE_ID, 0) AS dw_load_id,
    CURRENT_TIMESTAMP() AS dw_insert_date,
    CURRENT_TIMESTAMP() AS dw_update_date
  FROM BEC_ODS.HZ_PARTIES AS HP, BEC_ODS.HZ_PARTY_SITES AS HPS, BEC_ODS.HZ_LOCATIONS AS HL, BEC_ODS.HZ_CUST_ACCOUNTS AS HCA, BEC_ODS.HZ_CUST_ACCT_SITES_ALL AS HCS, BEC_ODS.HZ_CUST_SITE_USES_ALL AS HCSU
  WHERE
    HP.PARTY_ID = HPS.PARTY_ID
    AND HPS.LOCATION_ID = HL.LOCATION_ID
    AND HP.PARTY_ID = HCA.PARTY_ID
    AND HCA.CUST_ACCOUNT_ID = HCS.CUST_ACCOUNT_ID
    AND HCS.CUST_ACCT_SITE_ID = HCSU.CUST_ACCT_SITE_ID
    AND HCS.PARTY_SITE_ID = HPS.PARTY_SITE_ID
    AND HPS.STATUS = 'A'
    AND HCSU.STATUS = 'A'
);
UPDATE bec_etl_ctrl.batch_dw_info SET load_type = 'I', last_refresh_date = CURRENT_TIMESTAMP()
WHERE
  dw_table_name = 'dim_customer_details' AND batch_name = 'om';