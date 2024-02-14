DROP table IF EXISTS gold_bec_dwh.FACT_OM_CUSTOMER;
CREATE TABLE gold_bec_dwh.FACT_OM_CUSTOMER AS
(
  SELECT
    BCDV.PARTY_SITE_ID,
    BCDV.CUST_ACCOUNT_ID,
    HCSU.ORG_ID,
    BCDV.PARTY_NAME AS CUSTOMER_NAME,
    BCDV.ACCOUNT_NUMBER AS CUSTOMER_NUMBER,
    BCDV.ADDRESS2 AS SITE_ADDRESS2,
    CASE WHEN HCAL.STATUS = 'A' THEN 'Active' WHEN HCAL.STATUS = 'I' THEN 'Inactive' END AS CUSTOMER_STATUS,
    HCPC.NAME AS CUSTOMER_PROFILE_CLASS,
    HPS.PARTY_SITE_NUMBER AS SITE_NUMBER,
    HCSU.SITE_USE_CODE AS BUSINESS_PURPOSE,
    HL.ADDRESS1 AS SITE_ID,
    HCSU.PRIMARY_FLAG AS PRIMARY_SITE,
    COALESCE(HL.ADDRESS1, 'NA') || '-' || COALESCE(HL.ADDRESS2, 'NA') || '-' || COALESCE(HL.CITY, 'NA') || '-' || COALESCE(HL.STATE, 'NA') || '-' || COALESCE(HL.POSTAL_CODE, 'NA') || '-' || COALESCE(HL.COUNTRY, 'NA') AS SITE_ADDRESS,
    HL.STATE,
    HL.COUNTRY,
    HL.COUNTY,
    HL.POSTAL_CODE, /* ,HCSU.STATUS			SITE_STATUS */
    CASE WHEN HCSU.STATUS = 'A' THEN 'Active' WHEN HCSU.STATUS = 'I' THEN 'Inactive' END AS SITE_STATUS,
    HP.CREATION_DATE AS CREATION_DATE,
    HP.LAST_UPDATE_DATE,
    HP.LAST_UPDATED_BY,
    HP.CREATED_BY AS CUSTOMER_CREATED_BY,
    HP.PARTY_TYPE AS CUSTOMER_TYPE, /* added columns for quicksite */
    BCDV.SITE_USE_ID,
    BCDV.party_number,
    BCDV.party_site_number,
    BCDV.address3,
    BCDV.address4,
    BCDV.address5,
    BCDV.city,
    BCDV.location,
    (
      SELECT
        system_id
      FROM bec_etl_ctrl.etlsourceappid
      WHERE
        source_system = 'EBS'
    ) || '-' || BCDV.PARTY_SITE_ID AS PARTY_SITE_ID_KEY,
    (
      SELECT
        system_id
      FROM bec_etl_ctrl.etlsourceappid
      WHERE
        source_system = 'EBS'
    ) || '-' || BCDV.CUST_ACCOUNT_ID AS CUST_ACCOUNT_ID_KEY,
    (
      SELECT
        system_id
      FROM bec_etl_ctrl.etlsourceappid
      WHERE
        source_system = 'EBS'
    ) || '-' || HCSU.ORG_ID AS ORG_ID_KEY,
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
    ) || '-' || COALESCE(BCDV.DW_LOAD_ID, 'NA') AS dw_load_id,
    CURRENT_TIMESTAMP() AS dw_insert_date,
    CURRENT_TIMESTAMP() AS dw_update_date
  FROM (
    SELECT
      *
    FROM gold_bec_dwh.DIM_CUSTOMER_DETAILS
    WHERE
      is_deleted_flg <> 'Y'
  ) AS BCDV, (
    SELECT
      *
    FROM silver_bec_ods.HZ_PARTY_SITES
    WHERE
      is_deleted_flg <> 'Y'
  ) AS HPS, (
    SELECT
      *
    FROM silver_bec_ods.HZ_LOCATIONS
    WHERE
      is_deleted_flg <> 'Y'
  ) AS HL, (
    SELECT
      *
    FROM silver_bec_ods.HZ_CUST_ACCOUNTS
    WHERE
      is_deleted_flg <> 'Y'
  ) AS HCAL, (
    SELECT
      *
    FROM silver_bec_ods.HZ_CUST_ACCOUNTS
    WHERE
      is_deleted_flg <> 'Y'
  ) AS HCA, (
    SELECT
      *
    FROM silver_bec_ods.HZ_PARTIES
    WHERE
      is_deleted_flg <> 'Y'
  ) AS HP, (
    SELECT
      *
    FROM silver_bec_ods.HZ_CUST_SITE_USES_ALL
    WHERE
      is_deleted_flg <> 'Y'
  ) AS HCSU, (
    SELECT
      *
    FROM silver_bec_ods.HZ_CUSTOMER_PROFILES
    WHERE
      is_deleted_flg <> 'Y'
  ) AS HCP, (
    SELECT
      *
    FROM silver_bec_ods.HZ_CUST_PROFILE_CLASSES
    WHERE
      is_deleted_flg <> 'Y'
  ) AS HCPC
  WHERE
    1 = 1
    AND BCDV.PARTY_SITE_ID = HPS.PARTY_SITE_ID()
    AND BCDV.PARTY_SITE_NUMBER = HPS.PARTY_SITE_NUMBER()
    AND HPS.LOCATION_ID = HL.LOCATION_ID
    AND BCDV.CUST_ACCOUNT_ID = HCAL.CUST_ACCOUNT_ID()
    AND BCDV.CUST_ACCOUNT_ID = HCA.CUST_ACCOUNT_ID()
    AND HCA.PARTY_ID = HP.PARTY_ID()
    AND BCDV.SITE_USE_ID = HCSU.SITE_USE_ID()
    AND HCA.PARTY_ID = HCP.PARTY_ID
    AND BCDV.CUST_ACCOUNT_ID = HCP.CUST_ACCOUNT_ID()
    AND HCP.SITE_USE_ID() IS NULL
    AND HCP.PROFILE_CLASS_ID = HCPC.PROFILE_CLASS_ID()
);
UPDATE bec_etl_ctrl.batch_dw_info SET load_type = 'I', last_refresh_date = CURRENT_TIMESTAMP()
WHERE
  dw_table_name = 'fact_om_customer' AND batch_name = 'om';