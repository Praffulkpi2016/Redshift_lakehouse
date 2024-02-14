/* Delete Records */
DELETE FROM silver_bec_ods.IBY_EXTERNAL_PAYEES_ALL
WHERE
  (
    COALESCE(EXT_PAYEE_ID, 0)
  ) IN (
    SELECT
      COALESCE(stg.EXT_PAYEE_ID, 0) AS EXT_PAYEE_ID
    FROM silver_bec_ods.IBY_EXTERNAL_PAYEES_ALL AS ods, bronze_bec_ods_stg.IBY_EXTERNAL_PAYEES_ALL AS stg
    WHERE
      COALESCE(ods.EXT_PAYEE_ID, 0) = COALESCE(stg.EXT_PAYEE_ID, 0)
      AND stg.kca_operation IN ('INSERT', 'UPDATE')
  );
/* Insert records */
INSERT INTO silver_bec_ods.IBY_EXTERNAL_PAYEES_ALL (
  ext_payee_id,
  payee_party_id,
  payment_function,
  exclusive_payment_flag,
  created_by,
  creation_date,
  last_updated_by,
  last_update_date,
  last_update_login,
  object_version_number,
  party_site_id,
  supplier_site_id,
  org_id,
  org_type,
  default_payment_method_code,
  ece_tp_location_code,
  bank_charge_bearer,
  bank_instruction1_code,
  bank_instruction2_code,
  bank_instruction_details,
  payment_reason_code,
  payment_reason_comments,
  inactive_date,
  payment_text_message1,
  payment_text_message2,
  payment_text_message3,
  delivery_channel_code,
  payment_format_code,
  settlement_priority,
  remit_advice_delivery_method,
  remit_advice_email,
  remit_advice_fax,
  service_level_code,
  KCA_OPERATION,
  IS_DELETED_FLG,
  kca_seq_id,
  kca_seq_date
)
(
  SELECT
    ext_payee_id,
    payee_party_id,
    payment_function,
    exclusive_payment_flag,
    created_by,
    creation_date,
    last_updated_by,
    last_update_date,
    last_update_login,
    object_version_number,
    party_site_id,
    supplier_site_id,
    org_id,
    org_type,
    default_payment_method_code,
    ece_tp_location_code,
    bank_charge_bearer,
    bank_instruction1_code,
    bank_instruction2_code,
    bank_instruction_details,
    payment_reason_code,
    payment_reason_comments,
    inactive_date,
    payment_text_message1,
    payment_text_message2,
    payment_text_message3,
    delivery_channel_code,
    payment_format_code,
    settlement_priority,
    remit_advice_delivery_method,
    remit_advice_email,
    remit_advice_fax,
    service_level_code,
    KCA_OPERATION,
    'N' AS IS_DELETED_FLG,
    CAST(NULLIF(KCA_SEQ_ID, '') AS DECIMAL(36, 0)) AS KCA_SEQ_ID,
    kca_seq_date
  FROM bronze_bec_ods_stg.IBY_EXTERNAL_PAYEES_ALL
  WHERE
    kca_operation IN ('INSERT', 'UPDATE')
    AND (COALESCE(EXT_PAYEE_ID, 0), kca_seq_id) IN (
      SELECT
        COALESCE(EXT_PAYEE_ID, 0) AS EXT_PAYEE_ID,
        MAX(kca_seq_id)
      FROM bronze_bec_ods_stg.IBY_EXTERNAL_PAYEES_ALL
      WHERE
        kca_operation IN ('INSERT', 'UPDATE')
      GROUP BY
        COALESCE(EXT_PAYEE_ID, 0)
    )
);
/* Soft delete */
UPDATE silver_bec_ods.IBY_EXTERNAL_PAYEES_ALL SET IS_DELETED_FLG = 'N';
UPDATE silver_bec_ods.IBY_EXTERNAL_PAYEES_ALL SET IS_DELETED_FLG = 'Y'
WHERE
  (
    EXT_PAYEE_ID
  ) IN (
    SELECT
      EXT_PAYEE_ID
    FROM bec_raw_dl_ext.IBY_EXTERNAL_PAYEES_ALL
    WHERE
      (EXT_PAYEE_ID, KCA_SEQ_ID) IN (
        SELECT
          EXT_PAYEE_ID,
          MAX(KCA_SEQ_ID) AS KCA_SEQ_ID
        FROM bec_raw_dl_ext.IBY_EXTERNAL_PAYEES_ALL
        GROUP BY
          EXT_PAYEE_ID
      )
      AND kca_operation = 'DELETE'
  );
UPDATE bec_etl_ctrl.batch_ods_info SET last_refresh_date = CURRENT_TIMESTAMP()
WHERE
  ods_table_name = 'iby_external_payees_all';