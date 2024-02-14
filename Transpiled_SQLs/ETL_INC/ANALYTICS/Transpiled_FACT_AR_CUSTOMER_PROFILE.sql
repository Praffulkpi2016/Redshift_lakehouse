/* Delete Records */
DELETE FROM gold_bec_dwh.FACT_AR_CUSTOMER_PROFILE
WHERE
  (COALESCE(CUST_ACCOUNT_PROFILE_ID, 0), COALESCE(PARTY_ID, 0), COALESCE(CUST_ACCT_PROFILE_AMT_ID, 0)) IN (
    SELECT
      COALESCE(ods.CUST_ACCOUNT_PROFILE_ID, 0) AS CUST_ACCOUNT_PROFILE_ID,
      COALESCE(ods.PARTY_ID, 0) AS PARTY_ID,
      COALESCE(ods.CUST_ACCT_PROFILE_AMT_ID, 0) AS CUST_ACCT_PROFILE_AMT_ID
    FROM gold_bec_dwh.FACT_AR_CUSTOMER_PROFILE AS dw, (
      SELECT
        HCP.CUST_ACCOUNT_PROFILE_ID,
        HCP.PARTY_ID,
        HPA.CUST_ACCT_PROFILE_AMT_ID
      FROM silver_bec_ods.HZ_CUST_PROFILE_AMTS AS HPA
      INNER JOIN BEC_ODS.HZ_CUSTOMER_PROFILES AS HCP
        ON HPA.cust_account_profile_id = HCP.cust_account_profile_id
      LEFT OUTER JOIN (
        SELECT
          B.RESOURCE_ID AS RESOURCE_ID,
          T.RESOURCE_NAME AS RESOURCE_NAME,
          B.END_DATE_ACTIVE AS END_DATE_ACTIVE,
          B.is_deleted_flg,
          T.is_deleted_flg AS is_deleted_flg1
        FROM BEC_ODS.JTF_RS_RESOURCE_EXTNS AS B
        INNER JOIN BEC_ODS.JTF_RS_RESOURCE_EXTNS_TL AS T
          ON B.RESOURCE_ID = T.RESOURCE_ID AND B.CATEGORY = T.CATEGORY AND T.LANGUAGE = 'US'
      ) AS RES
        ON HCP.CREDIT_ANALYST_ID = RES.RESOURCE_ID
      WHERE
        1 = 1
        AND HCP.STATUS = 'A'
        AND HCP.CUST_ACCOUNT_ID <> '-1'
        AND (
          HPA.kca_seq_date > (
            SELECT
              (
                executebegints - prune_days
              )
            FROM bec_etl_ctrl.batch_dw_info
            WHERE
              dw_table_name = 'fact_ar_customer_profile' AND batch_name = 'ar'
          )
          OR HPA.is_deleted_flg = 'Y'
          OR HCP.is_deleted_flg = 'Y'
          OR RES.is_deleted_flg = 'Y'
          OR RES.is_deleted_flg1 = 'Y'
        )
    ) AS ods
    WHERE
      dw.dw_load_id = (
        SELECT
          system_id
        FROM bec_etl_ctrl.etlsourceappid
        WHERE
          source_system = 'EBS'
      ) || '-' || COALESCE(ODS.CUST_ACCOUNT_PROFILE_ID, 0) || '-' || COALESCE(ODS.PARTY_ID, 0) || '-' || COALESCE(ODS.CUST_ACCT_PROFILE_AMT_ID, 0)
  );
/* Insert records */
INSERT INTO gold_bec_dwh.FACT_AR_CUSTOMER_PROFILE (
  cust_acct_profile_amt_id,
  cust_account_profile_id,
  currency_code,
  trx_credit_limit,
  overall_credit_limit,
  min_dunning_amount,
  min_dunning_invoice_amount,
  max_interest_charge,
  min_statement_amount,
  interest_rate,
  attribute_category,
  min_fc_balance_amount,
  min_fc_invoice_amount,
  cust_account_id_key,
  cust_account_id,
  customer_site_use_id,
  customer_site_use_id_key,
  expiration_date,
  exchange_rate_type,
  interest_type,
  interest_fixed_amount,
  interest_schedule_id,
  penalty_type,
  penalty_rate,
  min_interest_charge,
  penalty_fixed_amount,
  last_update_date,
  cust_prof_account_id,
  collector_id_key,
  credit_checking,
  next_credit_review_date,
  credit_hold,
  site_use_id,
  cust_prof_site_id_key,
  profile_class_id,
  credit_rating,
  standard_terms,
  last_credit_review_date,
  review_cycle,
  late_charge_calculation_trx,
  credit_analyst_name,
  revolve_credit_flag,
  credit_classification,
  party_id,
  is_deleted_flg,
  source_app_id,
  dw_load_id,
  dw_insert_date,
  dw_update_date
)
(
  SELECT
    HPA.CUST_ACCT_PROFILE_AMT_ID AS CUST_ACCT_PROFILE_AMT_ID,
    HPA.CUST_ACCOUNT_PROFILE_ID AS CUST_ACCOUNT_PROFILE_ID,
    HPA.CURRENCY_CODE AS CURRENCY_CODE,
    HPA.TRX_CREDIT_LIMIT AS TRX_CREDIT_LIMIT,
    HPA.OVERALL_CREDIT_LIMIT AS OVERALL_CREDIT_LIMIT,
    HPA.MIN_DUNNING_AMOUNT AS MIN_DUNNING_AMOUNT,
    HPA.MIN_DUNNING_INVOICE_AMOUNT AS MIN_DUNNING_INVOICE_AMOUNT,
    HPA.MAX_INTEREST_CHARGE AS MAX_INTEREST_CHARGE,
    HPA.MIN_STATEMENT_AMOUNT AS MIN_STATEMENT_AMOUNT,
    HPA.INTEREST_RATE AS INTEREST_RATE,
    HPA.ATTRIBUTE_CATEGORY AS ATTRIBUTE_CATEGORY,
    HPA.MIN_FC_BALANCE_AMOUNT AS MIN_FC_BALANCE_AMOUNT,
    HPA.MIN_FC_INVOICE_AMOUNT AS MIN_FC_INVOICE_AMOUNT,
    (
      SELECT
        system_id
      FROM bec_etl_ctrl.etlsourceappid
      WHERE
        source_system = 'EBS'
    ) || '-' || HPA.CUST_ACCOUNT_ID AS CUST_ACCOUNT_ID_KEY,
    HPA.CUST_ACCOUNT_ID AS CUST_ACCOUNT_ID,
    HPA.SITE_USE_ID AS CUSTOMER_SITE_USE_ID,
    (
      SELECT
        system_id
      FROM bec_etl_ctrl.etlsourceappid
      WHERE
        source_system = 'EBS'
    ) || '-' || HPA.SITE_USE_ID AS CUSTOMER_SITE_USE_ID_KEY,
    HPA.EXPIRATION_DATE AS EXPIRATION_DATE,
    HPA.EXCHANGE_RATE_TYPE AS EXCHANGE_RATE_TYPE,
    HPA.INTEREST_TYPE AS INTEREST_TYPE,
    HPA.INTEREST_FIXED_AMOUNT AS INTEREST_FIXED_AMOUNT,
    HPA.INTEREST_SCHEDULE_ID AS INTEREST_SCHEDULE_ID,
    HPA.PENALTY_TYPE AS PENALTY_TYPE,
    HPA.PENALTY_RATE AS PENALTY_RATE,
    HPA.MIN_INTEREST_CHARGE AS MIN_INTEREST_CHARGE,
    HPA.PENALTY_FIXED_AMOUNT AS PENALTY_FIXED_AMOUNT,
    HPA.last_update_date AS last_update_date,
    HCP.CUST_ACCOUNT_ID AS CUST_PROF_ACCOUNT_ID,
    (
      SELECT
        system_id
      FROM bec_etl_ctrl.etlsourceappid
      WHERE
        source_system = 'EBS'
    ) || '-' || HCP.COLLECTOR_ID AS COLLECTOR_ID_KEY,
    HCP.CREDIT_CHECKING AS CREDIT_CHECKING,
    HCP.NEXT_CREDIT_REVIEW_DATE AS NEXT_CREDIT_REVIEW_DATE,
    HCP.CREDIT_HOLD AS CREDIT_HOLD,
    HCP.SITE_USE_ID,
    (
      SELECT
        system_id
      FROM bec_etl_ctrl.etlsourceappid
      WHERE
        source_system = 'EBS'
    ) || '-' || HCP.SITE_USE_ID AS CUST_PROF_SITE_ID_KEY,
    HCP.PROFILE_CLASS_ID,
    HCP.CREDIT_RATING AS CREDIT_RATING,
    HCP.STANDARD_TERMS AS STANDARD_TERMS,
    HCP.LAST_CREDIT_REVIEW_DATE AS LAST_CREDIT_REVIEW_DATE,
    HCP.REVIEW_CYCLE AS REVIEW_CYCLE,
    HCP.LATE_CHARGE_CALCULATION_TRX AS LATE_CHARGE_CALCULATION_TRX,
    RES.RESOURCE_NAME AS CREDIT_ANALYST_NAME,
    HCP.ATTRIBUTE1 AS REVOLVE_CREDIT_FLAG,
    HCP.CREDIT_CLASSIFICATION AS CREDIT_CLASSIFICATION,
    HCP.PARTY_ID,
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
    ) || '-' || COALESCE(HCP.CUST_ACCOUNT_PROFILE_ID, 0) || '-' || COALESCE(HCP.PARTY_ID, 0) || '-' || COALESCE(HPA.CUST_ACCT_PROFILE_AMT_ID, 0) AS dw_load_id,
    CURRENT_TIMESTAMP() AS dw_insert_date,
    CURRENT_TIMESTAMP() AS dw_update_date
  FROM (
    SELECT
      *
    FROM silver_bec_ods.HZ_CUST_PROFILE_AMTS
    WHERE
      is_deleted_flg <> 'Y'
  ) AS HPA
  INNER JOIN (
    SELECT
      *
    FROM BEC_ODS.HZ_CUSTOMER_PROFILES
    WHERE
      is_deleted_flg <> 'Y'
  ) AS HCP
    ON HPA.cust_account_profile_id = HCP.cust_account_profile_id
  LEFT OUTER JOIN (
    SELECT
      B.RESOURCE_ID AS RESOURCE_ID,
      T.RESOURCE_NAME AS RESOURCE_NAME,
      B.END_DATE_ACTIVE AS END_DATE_ACTIVE
    FROM (
      SELECT
        *
      FROM BEC_ODS.JTF_RS_RESOURCE_EXTNS
      WHERE
        is_deleted_flg <> 'Y'
    ) AS B
    INNER JOIN (
      SELECT
        *
      FROM BEC_ODS.JTF_RS_RESOURCE_EXTNS_TL
      WHERE
        is_deleted_flg <> 'Y'
    ) AS T
      ON B.RESOURCE_ID = T.RESOURCE_ID AND B.CATEGORY = T.CATEGORY AND T.LANGUAGE = 'US'
  ) AS RES
    ON HCP.CREDIT_ANALYST_ID = RES.RESOURCE_ID
  WHERE
    1 = 1
    AND HCP.STATUS = 'A'
    AND HCP.CUST_ACCOUNT_ID <> '-1'
    AND (
      HPA.kca_seq_date > (
        SELECT
          (
            executebegints - prune_days
          )
        FROM bec_etl_ctrl.batch_dw_info
        WHERE
          dw_table_name = 'fact_ar_customer_profile' AND batch_name = 'ar'
      )
    )
);
UPDATE bec_etl_ctrl.batch_dw_info SET last_refresh_date = CURRENT_TIMESTAMP()
WHERE
  dw_table_name = 'fact_ar_customer_profile' AND batch_name = 'ar';