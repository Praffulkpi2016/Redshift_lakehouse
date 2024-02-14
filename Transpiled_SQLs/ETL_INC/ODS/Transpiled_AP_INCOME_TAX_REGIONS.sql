/* Delete Records */
DELETE FROM silver_bec_ods.ap_income_tax_regions
WHERE
  region_short_name IN (
    SELECT
      stg.region_short_name
    FROM silver_bec_ods.ap_income_tax_regions AS ods, bronze_bec_ods_stg.ap_income_tax_regions AS stg
    WHERE
      ods.region_short_name = stg.region_short_name
      AND stg.kca_operation IN ('INSERT', 'UPDATE')
  );
/* Insert records */
INSERT INTO silver_bec_ods.ap_income_tax_regions (
  region_short_name,
  region_long_name,
  region_code,
  reporting_limit,
  num_of_payees,
  control_total1,
  control_total2,
  control_total3,
  control_total4,
  control_total5,
  control_total6,
  control_total7,
  control_total8,
  control_total9,
  control_total10,
  active_date,
  inactive_date,
  last_update_date,
  last_updated_by,
  last_update_login,
  creation_date,
  created_by,
  reporting_limit_method_code,
  control_total13,
  control_total13e,
  control_total14,
  control_total15a,
  control_total15b,
  KCA_OPERATION,
  IS_DELETED_FLG,
  kca_seq_id,
  kca_seq_date
)
(
  SELECT
    region_short_name,
    region_long_name,
    region_code,
    reporting_limit,
    num_of_payees,
    control_total1,
    control_total2,
    control_total3,
    control_total4,
    control_total5,
    control_total6,
    control_total7,
    control_total8,
    control_total9,
    control_total10,
    active_date,
    inactive_date,
    last_update_date,
    last_updated_by,
    last_update_login,
    creation_date,
    created_by,
    reporting_limit_method_code,
    control_total13,
    control_total13e,
    control_total14,
    control_total15a,
    control_total15b,
    KCA_OPERATION,
    'N' AS IS_DELETED_FLG,
    CAST(NULLIF(KCA_SEQ_ID, '') AS DECIMAL(36, 0)) AS KCA_SEQ_ID,
    kca_seq_date
  FROM bronze_bec_ods_stg.ap_income_tax_regions
  WHERE
    kca_operation IN ('INSERT', 'UPDATE')
    AND (region_short_name, KCA_SEQ_ID) IN (
      SELECT
        region_short_name,
        MAX(KCA_SEQ_ID)
      FROM bronze_bec_ods_stg.ap_income_tax_regions
      WHERE
        kca_operation IN ('INSERT', 'UPDATE')
      GROUP BY
        region_short_name
    )
);
/* Soft delete */
UPDATE silver_bec_ods.ap_income_tax_regions SET IS_DELETED_FLG = 'N';
UPDATE silver_bec_ods.ap_income_tax_regions SET IS_DELETED_FLG = 'Y'
WHERE
  (
    region_short_name
  ) IN (
    SELECT
      region_short_name
    FROM bec_raw_dl_ext.ap_income_tax_regions
    WHERE
      (region_short_name, KCA_SEQ_ID) IN (
        SELECT
          region_short_name,
          MAX(KCA_SEQ_ID) AS KCA_SEQ_ID
        FROM bec_raw_dl_ext.ap_income_tax_regions
        GROUP BY
          region_short_name
      )
      AND kca_operation = 'DELETE'
  );
UPDATE bec_etl_ctrl.batch_ods_info SET last_refresh_date = CURRENT_TIMESTAMP()
WHERE
  ods_table_name = 'ap_income_tax_regions';