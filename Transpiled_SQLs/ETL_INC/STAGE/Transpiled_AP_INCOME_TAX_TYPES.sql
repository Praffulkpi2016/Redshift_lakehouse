TRUNCATE table bronze_bec_ods_stg.ap_income_tax_types;
INSERT INTO bronze_bec_ods_stg.ap_income_tax_types (
  income_tax_type,
  description,
  inactive_date,
  last_update_date,
  last_updated_by,
  last_update_login,
  creation_date,
  created_by,
  zd_edition_name,
  zd_sync,
  KCA_OPERATION,
  kca_seq_id,
  kca_seq_date
)
(
  SELECT
    income_tax_type,
    description,
    inactive_date,
    last_update_date,
    last_updated_by,
    last_update_login,
    creation_date,
    created_by,
    zd_edition_name,
    zd_sync,
    KCA_OPERATION,
    kca_seq_id,
    kca_seq_date
  FROM bec_raw_dl_ext.ap_income_tax_types
  WHERE
    kca_operation <> 'DELETE'
    AND COALESCE(kca_seq_id, '') <> ''
    AND (income_tax_type, KCA_SEQ_ID) IN (
      SELECT
        income_tax_type,
        MAX(KCA_SEQ_ID)
      FROM bec_raw_dl_ext.ap_income_tax_types
      WHERE
        kca_operation <> 'DELETE' AND COALESCE(kca_seq_id, '') <> ''
      GROUP BY
        income_tax_type
    )
    AND kca_seq_date > (
      SELECT
        (
          executebegints - prune_days
        )
      FROM bec_etl_ctrl.batch_ods_info
      WHERE
        ods_table_name = 'ap_income_tax_types'
    )
);