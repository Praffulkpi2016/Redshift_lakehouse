TRUNCATE table bronze_bec_ods_stg.AP_DISTRIBUTION_SET_LINES_ALL;
INSERT INTO bronze_bec_ods_stg.ap_distribution_set_lines_all (
  distribution_set_id,
  dist_code_combination_id,
  last_update_date,
  last_updated_by,
  set_of_books_id,
  percent_distribution,
  type_1099,
  vat_code,
  description,
  last_update_login,
  creation_date,
  created_by,
  distribution_set_line_number,
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
  project_accounting_context,
  task_id,
  project_id,
  expenditure_organization_id,
  expenditure_type,
  org_id,
  award_id,
  KCA_OPERATION,
  kca_seq_id,
  kca_seq_date
)
(
  SELECT
    distribution_set_id,
    dist_code_combination_id,
    last_update_date,
    last_updated_by,
    set_of_books_id,
    percent_distribution,
    type_1099,
    vat_code,
    description,
    last_update_login,
    creation_date,
    created_by,
    distribution_set_line_number,
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
    project_accounting_context,
    task_id,
    project_id,
    expenditure_organization_id,
    expenditure_type,
    org_id,
    award_id,
    KCA_OPERATION,
    kca_seq_id,
    kca_seq_date
  FROM bec_raw_dl_ext.AP_DISTRIBUTION_SET_LINES_ALL
  WHERE
    kca_operation <> 'DELETE'
    AND COALESCE(kca_seq_id, '') <> ''
    AND (distribution_set_id, distribution_set_line_number, KCA_SEQ_ID) IN (
      SELECT
        distribution_set_id,
        distribution_set_line_number,
        MAX(KCA_SEQ_ID)
      FROM bec_raw_dl_ext.AP_DISTRIBUTION_SET_LINES_ALL
      WHERE
        kca_operation <> 'DELETE' AND COALESCE(kca_seq_id, '') <> ''
      GROUP BY
        distribution_set_id,
        distribution_set_line_number
    )
    AND kca_seq_date > (
      SELECT
        (
          executebegints - prune_days
        )
      FROM bec_etl_ctrl.batch_ods_info
      WHERE
        ods_table_name = 'ap_distribution_set_lines_all'
    )
);