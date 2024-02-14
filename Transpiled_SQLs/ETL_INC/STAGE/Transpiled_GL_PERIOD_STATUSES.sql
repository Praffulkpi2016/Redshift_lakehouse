TRUNCATE table bronze_bec_ods_stg.GL_PERIOD_STATUSES;
INSERT INTO bronze_bec_ods_stg.GL_PERIOD_STATUSES (
  application_id,
  set_of_books_id,
  period_name,
  last_update_date,
  last_updated_by,
  closing_status,
  start_date,
  end_date,
  year_start_date,
  quarter_num,
  quarter_start_date,
  period_type,
  period_year,
  effective_period_num,
  period_num,
  adjustment_period_flag,
  creation_date,
  created_by,
  last_update_login,
  elimination_confirmed_flag,
  attribute1,
  attribute2,
  attribute3,
  attribute4,
  attribute5,
  context,
  chronological_seq_status_code,
  ledger_id,
  migration_status_code,
  track_bc_ytd_flag,
  KCA_OPERATION,
  kca_seq_id,
  kca_seq_date
)
(
  SELECT
    application_id,
    set_of_books_id,
    period_name,
    last_update_date,
    last_updated_by,
    closing_status,
    start_date,
    end_date,
    year_start_date,
    quarter_num,
    quarter_start_date,
    period_type,
    period_year,
    effective_period_num,
    period_num,
    adjustment_period_flag,
    creation_date,
    created_by,
    last_update_login,
    elimination_confirmed_flag,
    attribute1,
    attribute2,
    attribute3,
    attribute4,
    attribute5,
    context,
    chronological_seq_status_code,
    ledger_id,
    migration_status_code,
    track_bc_ytd_flag,
    KCA_OPERATION,
    kca_seq_id,
    kca_seq_date
  FROM bec_raw_dl_ext.gl_period_statuses
  WHERE
    kca_operation <> 'DELETE'
    AND COALESCE(kca_seq_id, '') <> ''
    AND (APPLICATION_ID, LEDGER_ID, PERIOD_NAME, kca_seq_id) IN (
      SELECT
        APPLICATION_ID,
        LEDGER_ID,
        PERIOD_NAME,
        MAX(kca_seq_id)
      FROM bec_raw_dl_ext.gl_period_statuses
      WHERE
        kca_operation <> 'DELETE' AND COALESCE(kca_seq_id, '') <> ''
      GROUP BY
        APPLICATION_ID,
        LEDGER_ID,
        PERIOD_NAME
    )
    AND (
      kca_seq_date > (
        SELECT
          (
            executebegints - prune_days
          )
        FROM bec_etl_ctrl.batch_ods_info
        WHERE
          ods_table_name = 'gl_period_statuses'
      )
    )
);