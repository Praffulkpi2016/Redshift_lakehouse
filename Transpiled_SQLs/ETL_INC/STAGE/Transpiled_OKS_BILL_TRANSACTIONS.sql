TRUNCATE table bronze_bec_ods_stg.OKS_BILL_TRANSACTIONS;
INSERT INTO bronze_bec_ods_stg.oks_bill_transactions (
  id,
  currency_code,
  object_version_number,
  created_by,
  creation_date,
  last_updated_by,
  last_update_date,
  trx_date,
  trx_number,
  trx_amount,
  trx_class,
  last_update_login,
  security_group_id,
  KCA_OPERATION,
  kca_seq_id,
  kca_seq_date
)
(
  SELECT
    id,
    currency_code,
    object_version_number,
    created_by,
    creation_date,
    last_updated_by,
    last_update_date,
    trx_date,
    trx_number,
    trx_amount,
    trx_class,
    last_update_login,
    security_group_id,
    KCA_OPERATION,
    kca_seq_id,
    kca_seq_date
  FROM bec_raw_dl_ext.oks_bill_transactions
  WHERE
    kca_operation <> 'DELETE'
    AND COALESCE(kca_seq_id, '') <> ''
    AND (ID, kca_seq_id) IN (
      SELECT
        ID,
        MAX(kca_seq_id)
      FROM bec_raw_dl_ext.oks_bill_transactions
      WHERE
        kca_operation <> 'DELETE' AND COALESCE(kca_seq_id, '') <> ''
      GROUP BY
        ID
    )
    AND kca_seq_date > (
      SELECT
        (
          executebegints - prune_days
        )
      FROM bec_etl_ctrl.batch_ods_info
      WHERE
        ods_table_name = 'oks_bill_transactions'
    )
);