TRUNCATE table bronze_bec_ods_stg.OKS_BILL_TXN_LINES;
INSERT INTO bronze_bec_ods_stg.oks_bill_txn_lines (
  id,
  btn_id,
  bsl_id,
  bcl_id,
  object_version_number,
  created_by,
  creation_date,
  last_updated_by,
  last_update_date,
  bill_instance_number,
  trx_line_amount,
  trx_line_tax_amount,
  last_update_login,
  attribute_category,
  attribute1,
  attribute2,
  attribute3,
  attribute4,
  attribute5,
  attribute6,
  attribute7,
  attribute8,
  attribute9,
  attribute10,
  attribute11,
  attribute12,
  attribute13,
  attribute14,
  attribute15,
  security_group_id,
  trx_date,
  trx_number,
  trx_class,
  manual_credit,
  trx_amount,
  split_flag,
  cycle_refrence,
  KCA_OPERATION,
  kca_seq_id,
  kca_seq_date
)
(
  SELECT
    id,
    btn_id,
    bsl_id,
    bcl_id,
    object_version_number,
    created_by,
    creation_date,
    last_updated_by,
    last_update_date,
    bill_instance_number,
    trx_line_amount,
    trx_line_tax_amount,
    last_update_login,
    attribute_category,
    attribute1,
    attribute2,
    attribute3,
    attribute4,
    attribute5,
    attribute6,
    attribute7,
    attribute8,
    attribute9,
    attribute10,
    attribute11,
    attribute12,
    attribute13,
    attribute14,
    attribute15,
    security_group_id,
    trx_date,
    trx_number,
    trx_class,
    manual_credit,
    trx_amount,
    split_flag,
    cycle_refrence,
    KCA_OPERATION,
    kca_seq_id,
    kca_seq_date
  FROM bec_raw_dl_ext.oks_bill_txn_lines
  WHERE
    kca_operation <> 'DELETE'
    AND COALESCE(kca_seq_id, '') <> ''
    AND (ID, kca_seq_id) IN (
      SELECT
        ID,
        MAX(kca_seq_id)
      FROM bec_raw_dl_ext.oks_bill_txn_lines
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
        ods_table_name = 'oks_bill_txn_lines'
    )
);