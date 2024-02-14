DROP table IF EXISTS gold_bec_dwh.DIM_AP_PAYMENT_BANKS;
CREATE TABLE gold_bec_dwh.DIM_AP_PAYMENT_BANKS AS
(
  SELECT
    aba.bank_account_id,
    bv.party_id,
    aba.bank_account_name,
    aba.bank_account_num,
    aba.currency_code,
    aba.description AS bank_acc_desc,
    'BANK CONTACT' AS bank_acc_contact,
    'BANK PHONE' AS contact_phone,
    aba.bank_account_type,
    'ACCT TYPE' AS account_type,
    'BRANCH' AS bank_branch_name,
    bv.party_name AS bank_name,
    'BRANCH DESC' AS description,
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
    ) || '-' || COALESCE(aba.bank_account_id, 0) || '-' || COALESCE(bv.party_id, 0) AS dw_load_id,
    CURRENT_TIMESTAMP() AS dw_insert_date,
    CURRENT_TIMESTAMP() AS dw_update_date
  FROM silver_bec_ods.ce_bank_accounts AS aba, silver_bec_ods.hz_parties AS bv
  WHERE
    aba.bank_id = bv.party_id
);
UPDATE bec_etl_ctrl.batch_dw_info SET load_type = 'I', last_refresh_date = CURRENT_TIMESTAMP()
WHERE
  dw_table_name = 'dim_ap_payment_banks' AND batch_name = 'ap';