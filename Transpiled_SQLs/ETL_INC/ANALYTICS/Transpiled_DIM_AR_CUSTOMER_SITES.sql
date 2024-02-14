/* Delete Records */
DELETE FROM gold_bec_dwh.DIM_AR_CUSTOMER_SITES
WHERE
  COALESCE(SITE_USE_ID, 0) IN (
    SELECT
      COALESCE(ods.SITE_USE_ID, 0) AS SITE_USE_ID
    FROM gold_bec_dwh.DIM_AR_CUSTOMER_SITES AS dw, silver_bec_ods.HZ_CUST_SITE_USES_ALL AS ods
    WHERE
      dw.dw_load_id = (
        SELECT
          system_id
        FROM bec_etl_ctrl.etlsourceappid
        WHERE
          source_system = 'EBS'
      ) || '-' || COALESCE(ods.SITE_USE_ID, 0)
      AND (
        ods.kca_seq_date > (
          SELECT
            (
              executebegints - prune_days
            )
          FROM bec_etl_ctrl.batch_dw_info
          WHERE
            dw_table_name = 'dim_ar_customer_sites' AND batch_name = 'ar'
        )
      )
  );
/* Insert records */
INSERT INTO gold_bec_dwh.DIM_AR_CUSTOMER_SITES (
  salesrep_id,
  resource_id,
  salesrep_name,
  salesrep_status,
  start_date_active,
  end_date_active,
  salesrep_number,
  org_id,
  person_id,
  party_id,
  party_number,
  party_name,
  party_type,
  country,
  address1,
  address2,
  address3,
  address4,
  city,
  postal_code,
  state,
  province,
  parties_status,
  county,
  category_code,
  cust_account_id,
  account_number,
  cust_account_status,
  customer_type,
  customer_class_code,
  warehouse_id,
  account_name,
  cust_acct_site_id,
  site_use_id,
  site_use_code,
  primary_flag,
  cust_site_acct_status,
  site_location,
  cust_site_use_warehouse_id,
  cust_site_use_org_id,
  last_update_date,
  customer_name,
  customer_number,
  customer_category,
  customer_class,
  customer_channel,
  profile_class_id,
  collector_id,
  collector_name,
  employee_id,
  description,
  collector_staus,
  inactive_date,
  collector_alias,
  is_deleted_flg,
  source_app_id,
  dw_load_id,
  dw_insert_date,
  dw_update_date
)
(
  SELECT
    RS.SALESREP_ID,
    RS.RESOURCE_ID,
    RS.NAME,
    RS.STATUS,
    RS.START_DATE_ACTIVE,
    RS.END_DATE_ACTIVE,
    RS.SALESREP_NUMBER,
    RS.ORG_ID,
    RS.PERSON_ID,
    PARTIES.PARTY_ID,
    PARTIES.PARTY_NUMBER,
    PARTIES.PARTY_NAME,
    PARTIES.PARTY_TYPE,
    PARTIES.COUNTRY,
    PARTIES.ADDRESS1,
    PARTIES.ADDRESS2,
    PARTIES.ADDRESS3,
    PARTIES.ADDRESS4,
    PARTIES.CITY,
    PARTIES.POSTAL_CODE,
    PARTIES.STATE,
    PARTIES.PROVINCE,
    PARTIES.STATUS AS PARTIES_STATUS,
    PARTIES.COUNTY,
    PARTIES.CATEGORY_CODE,
    HZCA.CUST_ACCOUNT_ID,
    HZCA.ACCOUNT_NUMBER,
    HZCA.STATUS AS CUST_ACCOUNT_STATUS,
    HZCA.CUSTOMER_TYPE,
    HZCA.CUSTOMER_CLASS_CODE,
    HZCA.WAREHOUSE_ID,
    HZCA.ACCOUNT_NAME,
    HCAS.CUST_ACCT_SITE_ID,
    HCSS.SITE_USE_ID,
    HCSS.SITE_USE_CODE,
    HCSS.PRIMARY_FLAG,
    HCSS.STATUS AS CUST_ACCT_STATUS,
    HCSS.LOCATION,
    HCSS.WAREHOUSE_ID AS CUST_SITE_USE_WAREHOUSE_ID,
    HCSS.ORG_ID AS CUST_SITE_USE_ORG_ID,
    HCSS.last_update_date,
    RC.CUSTOMER_NAME AS CUSTOMER_NAME,
    RC.CUSTOMER_NUMBER AS CUSTOMER_NUMBER,
    RC.CUSTOMER_CATEGORY_CODE AS CUSTOMER_CATEGORY,
    RC.CUSTOMER_CLASS_CODE AS CUSTOMER_CLASS,
    RC.SALES_CHANNEL_CODE AS CUSTOMER_CHANNEL,
    PRO.PROFILE_CLASS_ID,
    COLL.COLLECTOR_ID,
    COLL.NAME AS COLLECTOR_NAME,
    COLL.EMPLOYEE_ID,
    COLL.DESCRIPTION,
    COLL.STATUS AS COLLECTOR_STAUS,
    COLL.INACTIVE_DATE,
    COLL.ALIAS,
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
    ) || '-' || COALESCE(HCSS.SITE_USE_ID, 0) AS dw_load_id,
    CURRENT_TIMESTAMP() AS dw_insert_date,
    CURRENT_TIMESTAMP() AS dw_update_date
  FROM silver_bec_ods.JTF_RS_SALESREPS AS RS, silver_bec_ods.HZ_PARTIES AS PARTIES, silver_bec_ods.HZ_CUST_ACCOUNTS AS HZCA, silver_bec_ods.HZ_CUST_ACCT_SITES_ALL AS HCAS, silver_bec_ods.HZ_CUST_SITE_USES_ALL AS HCSS, silver_bec_ods.AR_CUSTOMERS AS RC, silver_bec_ods.HZ_CUSTOMER_PROFILES AS PRO, silver_bec_ods.HZ_CUST_PROFILE_CLASSES AS PRC, silver_bec_ods.AR_COLLECTORS AS COLL
  WHERE
    HCSS.CUST_ACCT_SITE_ID = HCAS.CUST_ACCT_SITE_ID
    AND HCAS.CUST_ACCOUNT_ID = HZCA.CUST_ACCOUNT_ID
    AND HZCA.PARTY_ID = PARTIES.PARTY_ID
    AND HZCA.CUST_ACCOUNT_ID = RC.CUSTOMER_ID
    AND HCSS.PRIMARY_SALESREP_ID = RS.SALESREP_ID()
    AND HCSS.SITE_USE_ID = PRO.SITE_USE_ID()
    AND PRO.PROFILE_CLASS_ID = PRC.PROFILE_CLASS_ID()
    AND PRO.COLLECTOR_ID = COLL.COLLECTOR_ID()
    AND HCSS.ORG_ID = RS.ORG_ID()
    AND (
      HCSS.kca_seq_date > (
        SELECT
          (
            executebegints - prune_days
          )
        FROM bec_etl_ctrl.batch_dw_info
        WHERE
          dw_table_name = 'dim_ar_customer_sites' AND batch_name = 'ar'
      )
    )
);
/* Soft delete */
UPDATE gold_bec_dwh.DIM_AR_CUSTOMER_SITES SET is_deleted_flg = 'Y'
WHERE
  NOT (
    COALESCE(site_use_id, 0)
  ) IN (
    SELECT
      COALESCE(ods.site_use_id, 0) AS site_use_id
    FROM gold_bec_dwh.DIM_AR_CUSTOMER_SITES AS dw, silver_bec_ods.HZ_CUST_SITE_USES_ALL AS ods
    WHERE
      dw.dw_load_id = (
        SELECT
          system_id
        FROM bec_etl_ctrl.etlsourceappid
        WHERE
          source_system = 'EBS'
      ) || '-' || COALESCE(ods.site_use_id, 0)
      AND ods.is_deleted_flg <> 'Y'
  );
UPDATE bec_etl_ctrl.batch_dw_info SET last_refresh_date = CURRENT_TIMESTAMP()
WHERE
  dw_table_name = 'dim_ar_customer_sites' AND batch_name = 'ar';