/* Delete Records */
DELETE FROM silver_bec_ods.ap_distribution_set_lines_all
WHERE
  (distribution_set_id, distribution_set_line_number) IN (
    SELECT
      stg.distribution_set_id,
      stg.distribution_set_line_number
    FROM silver_bec_ods.ap_distribution_set_lines_all AS ods, bronze_bec_ods_stg.ap_distribution_set_lines_all AS stg
    WHERE
      ods.distribution_set_id = stg.distribution_set_id
      AND ods.distribution_set_line_number = stg.distribution_set_line_number
      AND stg.kca_operation IN ('INSERT', 'UPDATE')
  );
/* Insert records		*/
INSERT INTO silver_bec_ods.ap_distribution_set_lines_all (
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
  IS_DELETED_FLG,
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
    'N' AS IS_DELETED_FLG,
    CAST(NULLIF(KCA_SEQ_ID, '') AS DECIMAL(36, 0)) AS KCA_SEQ_ID,
    kca_seq_date
  FROM bronze_bec_ods_stg.ap_distribution_set_lines_all
  WHERE
    kca_operation IN ('INSERT', 'UPDATE')
    AND (distribution_set_id, distribution_set_line_number, KCA_SEQ_ID) IN (
      SELECT
        distribution_set_id,
        distribution_set_line_number,
        MAX(KCA_SEQ_ID)
      FROM bronze_bec_ods_stg.ap_distribution_set_lines_all
      WHERE
        kca_operation IN ('INSERT', 'UPDATE')
      GROUP BY
        distribution_set_id,
        distribution_set_line_number
    )
);
/* Soft delete */
UPDATE silver_bec_ods.ap_distribution_set_lines_all SET IS_DELETED_FLG = 'N';
UPDATE silver_bec_ods.ap_distribution_set_lines_all SET IS_DELETED_FLG = 'Y'
WHERE
  (distribution_set_id, distribution_set_line_number) IN (
    SELECT
      distribution_set_id,
      distribution_set_line_number
    FROM bec_raw_dl_ext.ap_distribution_set_lines_all
    WHERE
      (distribution_set_id, distribution_set_line_number, KCA_SEQ_ID) IN (
        SELECT
          distribution_set_id,
          distribution_set_line_number,
          MAX(KCA_SEQ_ID) AS KCA_SEQ_ID
        FROM bec_raw_dl_ext.ap_distribution_set_lines_all
        GROUP BY
          distribution_set_id,
          distribution_set_line_number
      )
      AND kca_operation = 'DELETE'
  );
UPDATE bec_etl_ctrl.batch_ods_info SET last_refresh_date = CURRENT_TIMESTAMP()
WHERE
  ods_table_name = 'ap_distribution_set_lines_all';