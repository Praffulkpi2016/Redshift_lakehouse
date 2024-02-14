TRUNCATE table
	table bronze_bec_ods_stg.WIP_PERIOD_BALANCES;
INSERT INTO bronze_bec_ods_stg.WIP_PERIOD_BALANCES (
  acct_period_id,
  wip_entity_id,
  repetitive_schedule_id,
  last_update_date,
  last_updated_by,
  creation_date,
  created_by,
  last_update_login,
  organization_id,
  class_type,
  tl_resource_in,
  tl_overhead_in,
  tl_outside_processing_in,
  pl_material_in,
  pl_material_overhead_in,
  pl_resource_in,
  pl_overhead_in,
  pl_outside_processing_in,
  tl_material_out,
  tl_material_overhead_out,
  tl_resource_out,
  tl_overhead_out,
  tl_outside_processing_out,
  pl_material_out,
  pl_material_overhead_out,
  pl_resource_out,
  pl_overhead_out,
  pl_outside_processing_out,
  request_id,
  program_application_id,
  program_id,
  program_update_date,
  tl_material_var,
  tl_material_overhead_var,
  tl_resource_var,
  tl_outside_processing_var,
  tl_overhead_var,
  pl_material_var,
  pl_material_overhead_var,
  pl_resource_var,
  pl_overhead_var,
  pl_outside_processing_var,
  tl_scrap_in,
  tl_scrap_out,
  tl_scrap_var,
  kca_operation,
  kca_seq_id,
  KCA_SEQ_DATE
)
(
  SELECT
    acct_period_id,
    wip_entity_id,
    repetitive_schedule_id,
    last_update_date,
    last_updated_by,
    creation_date,
    created_by,
    last_update_login,
    organization_id,
    class_type,
    tl_resource_in,
    tl_overhead_in,
    tl_outside_processing_in,
    pl_material_in,
    pl_material_overhead_in,
    pl_resource_in,
    pl_overhead_in,
    pl_outside_processing_in,
    tl_material_out,
    tl_material_overhead_out,
    tl_resource_out,
    tl_overhead_out,
    tl_outside_processing_out,
    pl_material_out,
    pl_material_overhead_out,
    pl_resource_out,
    pl_overhead_out,
    pl_outside_processing_out,
    request_id,
    program_application_id,
    program_id,
    program_update_date,
    tl_material_var,
    tl_material_overhead_var,
    tl_resource_var,
    tl_outside_processing_var,
    tl_overhead_var,
    pl_material_var,
    pl_material_overhead_var,
    pl_resource_var,
    pl_overhead_var,
    pl_outside_processing_var,
    tl_scrap_in,
    tl_scrap_out,
    tl_scrap_var,
    kca_operation,
    kca_seq_id,
    KCA_SEQ_DATE
  FROM bec_raw_dl_ext.WIP_PERIOD_BALANCES
  WHERE
    kca_operation <> 'DELETE'
    AND COALESCE(kca_seq_id, '') <> ''
    AND (COALESCE(WIP_ENTITY_ID, 0), COALESCE(REPETITIVE_SCHEDULE_ID, 0), COALESCE(ACCT_PERIOD_ID, 0), kca_seq_id) IN (
      SELECT
        COALESCE(WIP_ENTITY_ID, 0) AS WIP_ENTITY_ID,
        COALESCE(REPETITIVE_SCHEDULE_ID, 0) AS REPETITIVE_SCHEDULE_ID,
        COALESCE(ACCT_PERIOD_ID, 0) AS ACCT_PERIOD_ID,
        MAX(kca_seq_id)
      FROM bec_raw_dl_ext.WIP_PERIOD_BALANCES
      WHERE
        kca_operation <> 'DELETE' AND COALESCE(kca_seq_id, '') <> ''
      GROUP BY
        WIP_ENTITY_ID,
        REPETITIVE_SCHEDULE_ID,
        ACCT_PERIOD_ID
    )
    AND (
      KCA_SEQ_DATE > (
        SELECT
          (
            executebegints - prune_days
          )
        FROM bec_etl_ctrl.batch_ods_info
        WHERE
          ods_table_name = 'wip_period_balances'
      )
    )
);