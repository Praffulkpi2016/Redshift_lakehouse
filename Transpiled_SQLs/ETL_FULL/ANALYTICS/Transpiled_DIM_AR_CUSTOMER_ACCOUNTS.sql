DROP table IF EXISTS gold_bec_dwh.DIM_AR_CUSTOMER_ACCOUNTS;
CREATE TABLE gold_bec_dwh.DIM_AR_CUSTOMER_ACCOUNTS AS
(
  SELECT
    RC.CUST_ACCOUNT_ID AS `CUSTOMER_ID`,
    HZP.CATEGORY_CODE AS `CUSTOMER_CATEGORY_CODE`,
    rc.customer_class_code,
    'A' AS `CUSTOMER_GROUP_CODE`,
    COALESCE(RC.ACCOUNT_NAME, HZP.PARTY_NAME) AS `CUSTOMER_NAME`,
    rc.ACCOUNT_NUMBER AS `CUSTOMER_NUMBER`,
    rc.customer_type,
    hzp.party_id AS `PARTY_ID`,
    hzp.party_number,
    hzp.party_type,
    rc.sales_channel_code,
    rc.status AS `CUSTOMER_STATUS`,
    hzp.category_code,
    hzp.address1,
    hzp.address2,
    hzp.address3,
    hzp.address4,
    hzp.city,
    hzp.country,
    hzp.county,
    hzp.postal_code,
    COALESCE(hzp.province, hzp.state) AS `STATE_PROVINCE`,
    rc.creation_date,
    rc.last_update_date,
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
    ) || '-' || COALESCE(RC.CUST_ACCOUNT_ID, 0) || '-' || COALESCE(RC.PARTY_ID, 0) AS dw_load_id,
    CURRENT_TIMESTAMP() AS dw_insert_date,
    CURRENT_TIMESTAMP() AS dw_update_date
  FROM silver_bec_ods.HZ_PARTIES AS HZP, silver_bec_ods.HZ_CUST_ACCOUNTS AS RC
  WHERE
    1 = 1 AND HZP.PARTY_ID = RC.PARTY_ID
);
UPDATE bec_etl_ctrl.batch_dw_info SET load_type = 'I', last_refresh_date = CURRENT_TIMESTAMP()
WHERE
  dw_table_name = 'dim_ar_customer_accounts' AND batch_name = 'ar';