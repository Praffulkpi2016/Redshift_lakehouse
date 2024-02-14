/* Delete Records */
DELETE FROM gold_bec_dwh.DIM_AR_CUSTOMER_ACCOUNTS
WHERE
  (COALESCE(CUSTOMER_ID, 0), COALESCE(PARTY_ID, 0)) IN (
    SELECT
      COALESCE(ods.CUSTOMER_ID, 0),
      COALESCE(ods.PARTY_ID, 0)
    FROM gold_bec_dwh.DIM_AR_CUSTOMER_ACCOUNTS AS dw, (
      SELECT
        RC.CUST_ACCOUNT_ID AS `CUSTOMER_ID`,
        hzp.party_id AS `PARTY_ID`
      FROM silver_bec_ods.HZ_PARTIES AS HZP, silver_bec_ods.HZ_CUST_ACCOUNTS AS RC
      WHERE
        1 = 1
        AND HZP.PARTY_ID = RC.PARTY_ID
        AND (
          RC.kca_seq_date > (
            SELECT
              (
                executebegints - prune_days
              )
            FROM bec_etl_ctrl.batch_dw_info
            WHERE
              dw_table_name = 'dim_ar_customer_accounts' AND batch_name = 'ar'
          )
          OR hzp.kca_seq_date > (
            SELECT
              (
                executebegints - prune_days
              )
            FROM bec_etl_ctrl.batch_dw_info
            WHERE
              dw_table_name = 'dim_ar_customer_accounts' AND batch_name = 'ar'
          )
        )
    ) AS ods
    WHERE
      dw.dw_load_id = (
        SELECT
          system_id
        FROM bec_etl_ctrl.etlsourceappid
        WHERE
          source_system = 'EBS'
      ) || '-' || COALESCE(ods.CUSTOMER_ID, 0) || '-' || COALESCE(ods.PARTY_ID, 0)
  );
/* Insert records */
INSERT INTO gold_bec_dwh.DIM_AR_CUSTOMER_ACCOUNTS (
  customer_id,
  customer_category_code,
  customer_class_code,
  customer_group_code,
  customer_name,
  customer_number,
  customer_type,
  party_id,
  party_number,
  party_type,
  sales_channel_code,
  customer_status,
  category_code,
  address1,
  address2,
  address3,
  address4,
  city,
  country,
  county,
  postal_code,
  state_province,
  creation_date,
  last_update_date,
  is_deleted_flg,
  source_app_id,
  dw_load_id,
  dw_insert_date,
  dw_update_date
)
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
    1 = 1
    AND HZP.PARTY_ID = RC.PARTY_ID
    AND (
      RC.kca_seq_date > (
        SELECT
          (
            executebegints - prune_days
          )
        FROM bec_etl_ctrl.batch_dw_info
        WHERE
          dw_table_name = 'dim_ar_customer_accounts' AND batch_name = 'ar'
      )
      OR hzp.kca_seq_date > (
        SELECT
          (
            executebegints - prune_days
          )
        FROM bec_etl_ctrl.batch_dw_info
        WHERE
          dw_table_name = 'dim_ar_customer_accounts' AND batch_name = 'ar'
      )
    )
);
/* Soft delete */
UPDATE gold_bec_dwh.DIM_AR_CUSTOMER_ACCOUNTS SET is_deleted_flg = 'Y'
WHERE
  NOT (COALESCE(CUSTOMER_ID, 0), COALESCE(PARTY_ID, 0)) IN (
    SELECT
      COALESCE(ods.CUSTOMER_ID, 0),
      COALESCE(ods.PARTY_ID, 0)
    FROM gold_bec_dwh.DIM_AR_CUSTOMER_ACCOUNTS AS dw, (
      SELECT
        RC.CUST_ACCOUNT_ID AS `CUSTOMER_ID`,
        hzp.party_id AS `PARTY_ID`,
        RC.last_update_date,
        RC.kca_operation
      FROM (
        SELECT
          *
        FROM silver_bec_ods.HZ_PARTIES
        WHERE
          is_deleted_flg <> 'Y'
      ) AS HZP, (
        SELECT
          *
        FROM silver_bec_ods.HZ_CUST_ACCOUNTS
        WHERE
          is_deleted_flg <> 'Y'
      ) AS RC
      WHERE
        1 = 1 AND HZP.PARTY_ID = RC.PARTY_ID
    ) AS ods
    WHERE
      dw.dw_load_id = (
        SELECT
          system_id
        FROM bec_etl_ctrl.etlsourceappid
        WHERE
          source_system = 'EBS'
      ) || '-' || COALESCE(ods.CUSTOMER_ID, 0) || '-' || COALESCE(ods.PARTY_ID, 0)
  );
UPDATE bec_etl_ctrl.batch_dw_info SET last_refresh_date = CURRENT_TIMESTAMP()
WHERE
  dw_table_name = 'dim_ar_customer_accounts' AND batch_name = 'ar';