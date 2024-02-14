/* Delete Records */
DELETE FROM silver_bec_ods.OKS_BILL_TXN_LINES
WHERE
  COALESCE(ID, 'NA') IN (
    SELECT
      COALESCE(stg.ID, 'NA')
    FROM silver_bec_ods.oks_bill_txn_lines AS ods, bronze_bec_ods_stg.oks_bill_txn_lines AS stg
    WHERE
      ods.ID = stg.ID AND stg.kca_operation IN ('INSERT', 'UPDATE')
  );
/* Insert records */
INSERT INTO silver_bec_ods.oks_bill_txn_lines (
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
  IS_DELETED_FLG,
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
    'N' AS IS_DELETED_FLG,
    CAST(NULLIF(KCA_SEQ_ID, '') AS DECIMAL(36, 0)) AS KCA_SEQ_ID,
    kca_seq_date
  FROM bronze_bec_ods_stg.oks_bill_txn_lines
  WHERE
    kca_operation IN ('INSERT', 'UPDATE')
    AND (ID, kca_seq_id) IN (
      SELECT
        ID,
        MAX(kca_seq_id)
      FROM bronze_bec_ods_stg.oks_bill_txn_lines
      WHERE
        kca_operation IN ('INSERT', 'UPDATE')
      GROUP BY
        ID
    )
);
/* Soft delete */
UPDATE silver_bec_ods.oks_bill_txn_lines SET IS_DELETED_FLG = 'N';
UPDATE silver_bec_ods.oks_bill_txn_lines SET IS_DELETED_FLG = 'Y'
WHERE
  (
    COALESCE(ID, 'NA')
  ) IN (
    SELECT
      COALESCE(ID, 'NA')
    FROM bec_raw_dl_ext.oks_bill_txn_lines
    WHERE
      (COALESCE(ID, 'NA'), KCA_SEQ_ID) IN (
        SELECT
          COALESCE(ID, 'NA'),
          MAX(KCA_SEQ_ID) AS KCA_SEQ_ID
        FROM bec_raw_dl_ext.oks_bill_txn_lines
        GROUP BY
          COALESCE(ID, 'NA')
      )
      AND kca_operation = 'DELETE'
  );
UPDATE bec_etl_ctrl.batch_ods_info SET last_refresh_date = CURRENT_TIMESTAMP()
WHERE
  ods_table_name = 'oks_bill_txn_lines';