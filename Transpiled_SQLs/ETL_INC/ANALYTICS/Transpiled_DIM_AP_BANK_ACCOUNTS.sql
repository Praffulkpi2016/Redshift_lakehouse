/* delete */
WITH ValidAccounts AS (
  SELECT
    FLOOR(aba.bank_account_id) AS bank_account_id,
    bv.party_id AS party_id
  FROM silver_bec_ods.ce_bank_accounts AS aba
  JOIN silver_bec_ods.hz_parties AS bv
    ON aba.bank_id = bv.party_id
  WHERE
    bv.kca_seq_date > (
      SELECT
        (
          executebegints - prune_days
        )
      FROM bec_etl_ctrl.batch_dw_info
      WHERE
        dw_table_name = 'dim_ap_bank_accounts' AND batch_name = 'ap'
    )
  UNION
  SELECT
    bank_ac.ext_bank_account_id AS bank_account_id,
    hps.party_id AS party_id
  FROM silver_bec_ods.hz_party_sites AS hps, silver_bec_ods.iby_external_payees_all AS ib_xt_payee, silver_bec_ods.iby_pmt_instr_uses_all AS ib_pmt_instr, silver_bec_ods.iby_ext_bank_accounts AS bank_ac, silver_bec_ods.ce_bank_branches_v AS bank_branch
  WHERE
    1 = 1
    AND ib_xt_payee.payee_party_id = hps.party_id
    AND ib_pmt_instr.ext_pmt_party_id = ib_xt_payee.ext_payee_id
    AND ib_pmt_instr.instrument_type = 'BANKACCOUNT'
    AND ib_pmt_instr.instrument_id = bank_ac.ext_bank_account_id
    AND bank_ac.branch_id = bank_branch.BRANCH_PARTY_ID()
    AND ib_pmt_instr.END_DATE IS NULL
    AND (
      bank_ac.kca_seq_date > (
        SELECT
          (
            executebegints - prune_days
          )
        FROM bec_etl_ctrl.batch_dw_info
        WHERE
          dw_table_name = 'dim_ap_bank_accounts' AND batch_name = 'ap'
      )
      OR hps.kca_seq_date > (
        SELECT
          (
            executebegints - prune_days
          )
        FROM bec_etl_ctrl.batch_dw_info
        WHERE
          dw_table_name = 'dim_ap_bank_accounts' AND batch_name = 'ap'
      )
    )
)
DELETE FROM gold_bec_dwh.DIM_AP_BANK_ACCOUNTS USING ValidAccounts AS ods
WHERE
  DIM_AP_BANK_ACCOUNTS.dw_load_id = (
    SELECT
      system_id
    FROM bec_etl_ctrl.etlsourceappid
    WHERE
      source_system = 'EBS'
  ) || '-' || COALESCE(ods.bank_account_id, 0) || '-' || COALESCE(ods.party_id, 0);
/* INSERT */
INSERT INTO gold_bec_dwh.DIM_AP_BANK_ACCOUNTS
(
  SELECT
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
    bank_branch_num,
    bank_num,
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
    ) || '-' || COALESCE(bank_account_id, 0) || '-' || COALESCE(party_id, 0) AS dw_load_id,
    CURRENT_TIMESTAMP() AS dw_insert_date,
    CURRENT_TIMESTAMP() AS dw_update_date
  FROM (
    SELECT
      FLOOR(aba.bank_account_id) AS bank_account_id,
      bv.party_id AS party_id,
      aba.bank_account_name AS bank_account_name,
      aba.bank_account_num AS bank_account_num,
      aba.currency_code AS currency_code,
      aba.description AS bank_acc_desc,
      'BANK CONTACT' AS bank_acc_contact,
      'BANK PHONE' AS contact_phone,
      aba.bank_account_type AS bank_account_type,
      'ACCT TYPE' AS account_type,
      'BRANCH' AS bank_branch_name,
      bv.party_name AS bank_name,
      'BRANCH DESC' AS description,
      'BRANCH NUM' AS bank_branch_num,
      'BANK BRANCH NUM' AS bank_num
    FROM silver_bec_ods.ce_bank_accounts AS aba, silver_bec_ods.hz_parties AS bv
    WHERE
      aba.bank_id = bv.party_id
      AND (
        bv.kca_seq_date > (
          SELECT
            (
              executebegints - prune_days
            )
          FROM bec_etl_ctrl.batch_dw_info
          WHERE
            dw_table_name = 'dim_ap_bank_accounts' AND batch_name = 'ap'
        )
        OR aba.kca_seq_date > (
          SELECT
            (
              executebegints - prune_days
            )
          FROM bec_etl_ctrl.batch_dw_info
          WHERE
            dw_table_name = 'dim_ap_bank_accounts' AND batch_name = 'ap'
        )
      )
    UNION
    SELECT
      bank_ac.ext_bank_account_id AS bank_account_id,
      hps.party_id AS party_id,
      bank_ac.bank_account_name AS bank_account_name,
      bank_ac.bank_account_num AS bank_account_num,
      bank_ac.currency_code AS currency_code,
      bank_ac.description AS bank_acc_desc,
      'BANK CONTACT' AS bank_acc_contact,
      'BANK PHONE' AS contact_phone,
      bank_ac.bank_account_type AS bank_account_type,
      'ACCT TYPE' AS account_type,
      bank_branch.bank_branch_name,
      bank_branch.BANK_NAME AS bank_name,
      bank_branch.description,
      bank_branch.branch_number AS bank_branch_num,
      bank_branch.bank_number AS bank_num
    FROM silver_bec_ods.hz_party_sites AS hps, silver_bec_ods.iby_external_payees_all AS ib_xt_payee, silver_bec_ods.iby_pmt_instr_uses_all AS ib_pmt_instr, silver_bec_ods.iby_ext_bank_accounts AS bank_ac, silver_bec_ods.ce_bank_branches_v AS bank_branch
    WHERE
      1 = 1
      AND ib_xt_payee.payee_party_id = hps.party_id
      AND ib_pmt_instr.ext_pmt_party_id = ib_xt_payee.ext_payee_id
      AND ib_pmt_instr.instrument_type = 'BANKACCOUNT'
      AND ib_pmt_instr.instrument_id = bank_ac.ext_bank_account_id
      AND bank_ac.branch_id = bank_branch.BRANCH_PARTY_ID()
      AND ib_pmt_instr.END_DATE IS NULL
      AND (
        bank_ac.kca_seq_date > (
          SELECT
            (
              executebegints - prune_days
            )
          FROM bec_etl_ctrl.batch_dw_info
          WHERE
            dw_table_name = 'dim_ap_bank_accounts' AND batch_name = 'ap'
        )
        OR hps.kca_seq_date > (
          SELECT
            (
              executebegints - prune_days
            )
          FROM bec_etl_ctrl.batch_dw_info
          WHERE
            dw_table_name = 'dim_ap_bank_accounts' AND batch_name = 'ap'
        )
      )
  )
);
/* soft delete */
WITH ValidAccounts AS (
  SELECT
    FLOOR(aba.bank_account_id) AS bank_account_id,
    bv.party_id AS party_id
  FROM silver_bec_ods.ce_bank_accounts AS aba
  JOIN silver_bec_ods.hz_parties AS bv
    ON aba.bank_id = bv.party_id
  WHERE
    aba.is_deleted_flg <> 'Y'
  UNION
  SELECT
    bank_ac.ext_bank_account_id AS bank_account_id,
    hps.party_id AS party_id
  FROM silver_bec_ods.hz_party_sites AS hps, silver_bec_ods.iby_external_payees_all AS ib_xt_payee, silver_bec_ods.iby_pmt_instr_uses_all AS ib_pmt_instr, silver_bec_ods.iby_ext_bank_accounts AS bank_ac, silver_bec_ods.ce_bank_branches_v AS bank_branch
  WHERE
    1 = 1
    AND bank_ac.is_deleted_flg <> 'Y'
    AND ib_xt_payee.payee_party_id = hps.party_id
    AND ib_pmt_instr.ext_pmt_party_id = ib_xt_payee.ext_payee_id
    AND ib_pmt_instr.instrument_type = 'BANKACCOUNT'
    AND ib_pmt_instr.instrument_id = bank_ac.ext_bank_account_id
    AND bank_ac.branch_id = bank_branch.BRANCH_PARTY_ID()
    AND ib_pmt_instr.END_DATE IS NULL
)
UPDATE gold_bec_dwh.DIM_AP_BANK_ACCOUNTS SET is_deleted_flg = 'Y'
WHERE
  NOT EXISTS(
    SELECT
      1
    FROM ValidAccounts AS ods
    WHERE
      DIM_AP_BANK_ACCOUNTS.dw_load_id = (
        SELECT
          system_id
        FROM bec_etl_ctrl.etlsourceappid
        WHERE
          source_system = 'EBS'
      ) || '-' || COALESCE(ods.bank_account_id, 0) || '-' || COALESCE(ods.party_id, 0)
  );
UPDATE bec_etl_ctrl.batch_dw_info SET last_refresh_date = CURRENT_TIMESTAMP()
WHERE
  dw_table_name = 'dim_ap_bank_accounts' AND batch_name = 'ap';