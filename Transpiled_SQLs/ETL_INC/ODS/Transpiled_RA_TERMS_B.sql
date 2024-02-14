/* Delete Records */
DELETE FROM silver_bec_ods.RA_TERMS_B
WHERE
  (
    TERM_ID
  ) IN (
    SELECT
      stg.TERM_ID
    FROM silver_bec_ods.RA_TERMS_B AS ods, bronze_bec_ods_stg.RA_TERMS_B AS stg
    WHERE
      ods.TERM_ID = stg.TERM_ID AND stg.kca_operation IN ('INSERT', 'UPDATE')
  );
/* Insert records */
INSERT INTO silver_bec_ods.RA_TERMS_B (
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
  is_deleted_flg,
  kca_seq_id,
  kca_seq_date
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
    KCA_OPERATION,
    'N' AS IS_DELETED_FLG,
    CAST(NULLIF(KCA_SEQ_ID, '') AS DECIMAL(36, 0)) AS KCA_SEQ_ID,
    KCA_SEQ_DATE
  FROM bronze_bec_ods_stg.RA_TERMS_B
  WHERE
    kca_operation IN ('INSERT', 'UPDATE')
    AND (TERM_ID, kca_seq_id) IN (
      SELECT
        TERM_ID,
        MAX(kca_seq_id)
      FROM bronze_bec_ods_stg.RA_TERMS_B
      WHERE
        kca_operation IN ('INSERT', 'UPDATE')
      GROUP BY
        TERM_ID
    )
);
/* Soft delete */
UPDATE silver_bec_ods.RA_TERMS_B SET IS_DELETED_FLG = 'N';
UPDATE silver_bec_ods.RA_TERMS_B SET IS_DELETED_FLG = 'Y'
WHERE
  (
    TERM_ID
  ) IN (
    SELECT
      TERM_ID
    FROM bec_raw_dl_ext.RA_TERMS_B
    WHERE
      (TERM_ID, KCA_SEQ_ID) IN (
        SELECT
          TERM_ID,
          MAX(KCA_SEQ_ID) AS KCA_SEQ_ID
        FROM bec_raw_dl_ext.RA_TERMS_B
        GROUP BY
          TERM_ID
      )
      AND kca_operation = 'DELETE'
  );
UPDATE bec_etl_ctrl.batch_ods_info SET last_refresh_date = CURRENT_TIMESTAMP()
WHERE
  ods_table_name = 'ra_terms_b';