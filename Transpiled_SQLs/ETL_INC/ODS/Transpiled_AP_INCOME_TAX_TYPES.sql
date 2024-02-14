/* Delete Records */
DELETE FROM silver_bec_ods.ap_income_tax_types
WHERE
  income_tax_type IN (
    SELECT
      stg.income_tax_type
    FROM silver_bec_ods.ap_income_tax_types AS ods, bronze_bec_ods_stg.ap_income_tax_types AS stg
    WHERE
      ods.income_tax_type = stg.income_tax_type
      AND stg.kca_operation IN ('INSERT', 'UPDATE')
  );
/* Insert records */
INSERT INTO silver_bec_ods.ap_income_tax_types (
  income_tax_type,
  description,
  inactive_date,
  last_update_date,
  last_updated_by,
  last_update_login,
  creation_date,
  created_by,
  ZD_EDITION_NAME,
  ZD_SYNC,
  KCA_OPERATION,
  IS_DELETED_FLG,
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
    ZD_EDITION_NAME,
    ZD_SYNC,
    KCA_OPERATION,
    'N' AS IS_DELETED_FLG,
    CAST(NULLIF(KCA_SEQ_ID, '') AS DECIMAL(36, 0)) AS KCA_SEQ_ID,
    kca_seq_date
  FROM bronze_bec_ods_stg.ap_income_tax_types
  WHERE
    kca_operation IN ('INSERT', 'UPDATE')
    AND (income_tax_type, kca_seq_id) IN (
      SELECT
        income_tax_type,
        MAX(kca_seq_id)
      FROM bronze_bec_ods_stg.ap_income_tax_types
      WHERE
        kca_operation IN ('INSERT', 'UPDATE')
      GROUP BY
        income_tax_type
    )
);
/* Soft delete */
UPDATE silver_bec_ods.ap_income_tax_types SET IS_DELETED_FLG = 'N';
UPDATE silver_bec_ods.ap_income_tax_types SET IS_DELETED_FLG = 'Y'
WHERE
  (
    income_tax_type
  ) IN (
    SELECT
      income_tax_type
    FROM bec_raw_dl_ext.ap_income_tax_types
    WHERE
      (income_tax_type, KCA_SEQ_ID) IN (
        SELECT
          income_tax_type,
          MAX(KCA_SEQ_ID) AS KCA_SEQ_ID
        FROM bec_raw_dl_ext.ap_income_tax_types
        GROUP BY
          income_tax_type
      )
      AND kca_operation = 'DELETE'
  );
UPDATE bec_etl_ctrl.batch_ods_info SET last_refresh_date = CURRENT_TIMESTAMP()
WHERE
  ods_table_name = 'ap_income_tax_types';