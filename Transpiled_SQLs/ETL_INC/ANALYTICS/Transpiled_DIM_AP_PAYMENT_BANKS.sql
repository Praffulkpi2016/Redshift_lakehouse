/* Delete Records */
DELETE FROM gold_bec_dwh.dim_ap_payment_banks
WHERE
  (COALESCE(bank_account_id, 0), COALESCE(party_id, 0)) IN (
    SELECT
      COALESCE(ods.bank_account_id, 0),
      COALESCE(ods.party_id, 0)
    FROM gold_bec_dwh.dim_ap_payment_banks AS dw, (
      SELECT
        FLOOR(aba.bank_account_id) AS bank_account_id,
        bv.party_id
      FROM silver_bec_ods.ce_bank_accounts AS aba, silver_bec_ods.hz_parties AS bv
      WHERE
        aba.bank_id = bv.party_id
        AND (
          aba.kca_seq_date > (
            SELECT
              (
                executebegints - prune_days
              )
            FROM bec_etl_ctrl.batch_dw_info
            WHERE
              dw_table_name = 'dim_ap_payment_banks' AND batch_name = 'ap'
          )
          OR bv.kca_seq_date > (
            SELECT
              (
                executebegints - prune_days
              )
            FROM bec_etl_ctrl.batch_dw_info
            WHERE
              dw_table_name = 'dim_ap_payment_banks' AND batch_name = 'ap'
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
      ) || '-' || COALESCE(ods.bank_account_id, 0) || '-' || COALESCE(ods.party_id, 0)
  );
/* Insert records */
INSERT INTO gold_bec_dwh.DIM_AP_PAYMENT_BANKS (
  bank_account_id,
  party_id,
  bank_account_name,
  bank_account_num,
  currency_code,
  bank_acc_desc,
  bank_acc_contact,
  contact_phone,
  bank_account_type,
  account_type,
  bank_branch_name,
  bank_name,
  description,
  is_deleted_flg,
  source_app_id,
  dw_load_id,
  dw_insert_date,
  dw_update_date
)
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
    AND (
      aba.kca_seq_date > (
        SELECT
          (
            executebegints - prune_days
          )
        FROM bec_etl_ctrl.batch_dw_info
        WHERE
          dw_table_name = 'dim_ap_payment_banks' AND batch_name = 'ap'
      )
      OR bv.kca_seq_date > (
        SELECT
          (
            executebegints - prune_days
          )
        FROM bec_etl_ctrl.batch_dw_info
        WHERE
          dw_table_name = 'dim_ap_payment_banks' AND batch_name = 'ap'
      )
    )
);
/* Soft delete */
UPDATE gold_bec_dwh.dim_ap_payment_banks SET is_deleted_flg = 'Y'
WHERE
  NOT (COALESCE(bank_account_id, 0), COALESCE(party_id, 0)) IN (
    SELECT
      COALESCE(ods.bank_account_id, 0),
      COALESCE(ods.party_id, 0)
    FROM gold_bec_dwh.dim_ap_payment_banks AS dw, (
      SELECT
        FLOOR(aba.bank_account_id) AS bank_account_id,
        bv.party_id
      FROM (
        SELECT
          *
        FROM silver_bec_ods.ce_bank_accounts
        WHERE
          is_deleted_flg <> 'Y'
      ) AS aba, (
        SELECT
          *
        FROM silver_bec_ods.hz_parties
        WHERE
          is_deleted_flg <> 'Y'
      ) AS bv
      WHERE
        aba.bank_id = bv.party_id
    ) AS ods
    WHERE
      dw.dw_load_id = (
        SELECT
          system_id
        FROM bec_etl_ctrl.etlsourceappid
        WHERE
          source_system = 'EBS'
      ) || '-' || COALESCE(ods.bank_account_id, 0) || '-' || COALESCE(ods.party_id, 0)
  );
UPDATE bec_etl_ctrl.batch_dw_info SET last_refresh_date = CURRENT_TIMESTAMP()
WHERE
  dw_table_name = 'dim_ap_payment_banks' AND batch_name = 'ap';