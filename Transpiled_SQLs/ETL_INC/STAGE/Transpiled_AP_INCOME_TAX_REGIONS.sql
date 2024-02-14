TRUNCATE table bronze_bec_ods_stg.ap_income_tax_regions;
INSERT INTO bronze_bec_ods_stg.ap_income_tax_regions (
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
    kca_seq_id,
    kca_seq_date
  FROM bec_raw_dl_ext.ap_income_tax_regions
  WHERE
    kca_operation <> 'DELETE'
    AND COALESCE(kca_seq_id, '') <> ''
    AND (REGION_SHORT_NAME, KCA_SEQ_ID) IN (
      SELECT
        REGION_SHORT_NAME,
        MAX(KCA_SEQ_ID)
      FROM bec_raw_dl_ext.ap_income_tax_regions
      WHERE
        kca_operation <> 'DELETE' AND COALESCE(kca_seq_id, '') <> ''
      GROUP BY
        REGION_SHORT_NAME
    )
    AND kca_seq_date > (
      SELECT
        (
          executebegints - prune_days
        )
      FROM bec_etl_ctrl.batch_ods_info
      WHERE
        ods_table_name = 'ap_income_tax_regions'
    )
);