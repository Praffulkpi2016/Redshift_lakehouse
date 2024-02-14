TRUNCATE table bronze_bec_ods_stg.IBY_EXTERNAL_PAYEES_ALL;
INSERT INTO bronze_bec_ods_stg.IBY_EXTERNAL_PAYEES_ALL (
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
    kca_seq_id,
    kca_seq_date
  FROM bec_raw_dl_ext.IBY_EXTERNAL_PAYEES_ALL
  WHERE
    kca_operation <> 'DELETE'
    AND COALESCE(kca_seq_id, '') <> ''
    AND (COALESCE(EXT_PAYEE_ID, 0), kca_seq_id) IN (
      SELECT
        COALESCE(EXT_PAYEE_ID, 0) AS EXT_PAYEE_ID,
        MAX(kca_seq_id)
      FROM bec_raw_dl_ext.IBY_EXTERNAL_PAYEES_ALL
      WHERE
        kca_operation <> 'DELETE' AND COALESCE(kca_seq_id, '') <> ''
      GROUP BY
        COALESCE(EXT_PAYEE_ID, 0)
    )
    AND (
      kca_seq_date > (
        SELECT
          (
            executebegints - prune_days
          )
        FROM bec_etl_ctrl.batch_ods_info
        WHERE
          ods_table_name = 'iby_external_payees_all'
      )
    )
);