TRUNCATE table bronze_bec_ods_stg.RA_TERMS_B;
INSERT INTO bronze_bec_ods_stg.RA_TERMS_B (
  term_id,
  last_update_date,
  last_updated_by,
  creation_date,
  created_by,
  last_update_login,
  credit_check_flag,
  due_cutoff_day,
  printing_lead_days,
  start_date_active,
  end_date_active,
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
  base_amount,
  calc_discount_on_lines_flag,
  first_installment_code,
  in_use,
  partial_discount_flag,
  attribute11,
  attribute12,
  attribute13,
  attribute14,
  attribute15,
  prepayment_flag,
  billing_cycle_id,
  zd_edition_name,
  zd_sync,
  kca_operation,
  kca_seq_id,
  KCA_SEQ_DATE
)
(
  SELECT
    term_id,
    last_update_date,
    last_updated_by,
    creation_date,
    created_by,
    last_update_login,
    credit_check_flag, /* "name", */
    due_cutoff_day,
    printing_lead_days,
    start_date_active, /* description, */
    end_date_active,
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
    base_amount,
    calc_discount_on_lines_flag,
    first_installment_code,
    in_use,
    partial_discount_flag,
    attribute11,
    attribute12,
    attribute13,
    attribute14,
    attribute15,
    prepayment_flag,
    billing_cycle_id,
    zd_edition_name,
    zd_sync,
    kca_operation,
    kca_seq_id,
    KCA_SEQ_DATE
  FROM bec_raw_dl_ext.RA_TERMS_B
  WHERE
    kca_operation <> 'DELETE'
    AND COALESCE(kca_seq_id, '') <> ''
    AND (TERM_ID, kca_seq_id) IN (
      SELECT
        TERM_ID,
        MAX(kca_seq_id)
      FROM bec_raw_dl_ext.RA_TERMS_B
      WHERE
        kca_operation <> 'DELETE' AND COALESCE(kca_seq_id, '') <> ''
      GROUP BY
        TERM_ID
    )
    AND (
      KCA_SEQ_DATE > (
        SELECT
          (
            executebegints - prune_days
          )
        FROM bec_etl_ctrl.batch_ods_info
        WHERE
          ods_table_name = 'ra_terms_b'
      )
    )
);