/* delete */
DELETE FROM gold_bec_dwh.DIM_CUSTOMER_DETAILS
WHERE
  EXISTS(
    SELECT
      1
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
      AND (
        COALESCE(HCS.kca_seq_date) > (
          SELECT
            (
              executebegints - prune_days
            )
          FROM bec_etl_ctrl.batch_dw_info
          WHERE
            dw_table_name = 'dim_customer_details' AND batch_name = 'om'
        )
        OR COALESCE(HCSU.kca_seq_date) > (
          SELECT
            (
              executebegints - prune_days
            )
          FROM bec_etl_ctrl.batch_dw_info
          WHERE
            dw_table_name = 'dim_customer_details' AND batch_name = 'om'
        )
      )
      AND COALESCE(DIM_CUSTOMER_DETAILS.CUST_ACCT_SITE_ID, 0) = COALESCE(HCS.CUST_ACCT_SITE_ID, 0)
      AND COALESCE(DIM_CUSTOMER_DETAILS.SITE_USE_ID, 0) = COALESCE(HCSU.SITE_USE_ID, 0)
  );
INSERT INTO gold_bec_dwh.dim_customer_details
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
    AND (
      COALESCE(HCS.kca_seq_date) > (
        SELECT
          (
            executebegints - prune_days
          )
        FROM bec_etl_ctrl.batch_dw_info
        WHERE
          dw_table_name = 'dim_customer_details' AND batch_name = 'om'
      )
      OR COALESCE(HCSU.kca_seq_date) > (
        SELECT
          (
            executebegints - prune_days
          )
        FROM bec_etl_ctrl.batch_dw_info
        WHERE
          dw_table_name = 'dim_customer_details' AND batch_name = 'om'
      )
    )
);
/* soft delete */
UPDATE gold_bec_dwh.DIM_CUSTOMER_DETAILS SET is_deleted_flg = 'Y'
WHERE
  NOT EXISTS(
    SELECT
      1
    FROM (
      SELECT
        *
      FROM BEC_ODS.HZ_PARTIES
      WHERE
        is_deleted_flg <> 'Y'
    ) AS HP, (
      SELECT
        *
      FROM BEC_ODS.HZ_PARTY_SITES
      WHERE
        is_deleted_flg <> 'Y'
    ) AS HPS, (
      SELECT
        *
      FROM BEC_ODS.HZ_LOCATIONS
      WHERE
        is_deleted_flg <> 'Y'
    ) AS HL, (
      SELECT
        *
      FROM BEC_ODS.HZ_CUST_ACCOUNTS
      WHERE
        is_deleted_flg <> 'Y'
    ) AS HCA, (
      SELECT
        *
      FROM BEC_ODS.HZ_CUST_ACCT_SITES_ALL
      WHERE
        is_deleted_flg <> 'Y'
    ) AS HCS, (
      SELECT
        *
      FROM BEC_ODS.HZ_CUST_SITE_USES_ALL
      WHERE
        is_deleted_flg <> 'Y'
    ) AS HCSU
    WHERE
      HP.PARTY_ID = HPS.PARTY_ID
      AND HPS.LOCATION_ID = HL.LOCATION_ID
      AND HP.PARTY_ID = HCA.PARTY_ID
      AND HCA.CUST_ACCOUNT_ID = HCS.CUST_ACCOUNT_ID
      AND HCS.CUST_ACCT_SITE_ID = HCSU.CUST_ACCT_SITE_ID
      AND HCS.PARTY_SITE_ID = HPS.PARTY_SITE_ID
      AND HPS.STATUS = 'A'
      AND HCSU.STATUS = 'A'
      AND COALESCE(DIM_CUSTOMER_DETAILS.CUST_ACCT_SITE_ID, 0) = COALESCE(HCS.CUST_ACCT_SITE_ID, 0)
      AND COALESCE(DIM_CUSTOMER_DETAILS.SITE_USE_ID, 0) = COALESCE(HCSU.SITE_USE_ID, 0)
  );
UPDATE bec_etl_ctrl.batch_dw_info SET load_type = 'I', last_refresh_date = CURRENT_TIMESTAMP()
WHERE
  dw_table_name = 'dim_customer_details' AND batch_name = 'om';