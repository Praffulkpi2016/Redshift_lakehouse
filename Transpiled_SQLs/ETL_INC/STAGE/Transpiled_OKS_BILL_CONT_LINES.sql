TRUNCATE table bronze_bec_ods_stg.OKS_BILL_CONT_LINES;
INSERT INTO bronze_bec_ods_stg.oks_bill_cont_lines (
  id,
  cle_id,
  btn_id,
  date_billed_from,
  date_billed_to,
  sent_yn,
  object_version_number,
  created_by,
  creation_date,
  last_updated_by,
  last_update_date,
  amount,
  bill_action,
  date_next_invoice,
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
  currency_code,
  averaged_yn,
  settled_yn,
  KCA_OPERATION,
  kca_seq_id,
  kca_seq_date
)
(
  SELECT
    id,
    cle_id,
    btn_id,
    date_billed_from,
    date_billed_to,
    sent_yn,
    object_version_number,
    created_by,
    creation_date,
    last_updated_by,
    last_update_date,
    amount,
    bill_action,
    date_next_invoice,
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
    currency_code,
    averaged_yn,
    settled_yn,
    KCA_OPERATION,
    kca_seq_id,
    kca_seq_date
  FROM bec_raw_dl_ext.oks_bill_cont_lines
  WHERE
    kca_operation <> 'DELETE'
    AND COALESCE(kca_seq_id, '') <> ''
    AND (ID, kca_seq_id) IN (
      SELECT
        ID,
        MAX(kca_seq_id)
      FROM bec_raw_dl_ext.oks_bill_cont_lines
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
        ods_table_name = 'oks_bill_cont_lines'
    )
);