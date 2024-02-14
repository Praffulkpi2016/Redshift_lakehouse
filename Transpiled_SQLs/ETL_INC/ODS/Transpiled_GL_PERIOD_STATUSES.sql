/* Delete Records */
DELETE FROM silver_bec_ods.gl_period_statuses
WHERE
  (APPLICATION_ID, LEDGER_ID, PERIOD_NAME) IN (
    SELECT
      STG.APPLICATION_ID,
      STG.LEDGER_ID,
      STG.PERIOD_NAME
    FROM silver_bec_ods.gl_period_statuses AS ods, bronze_bec_ods_stg.gl_period_statuses AS stg
    WHERE
      ods.APPLICATION_ID = stg.APPLICATION_ID
      AND ods.LEDGER_ID = stg.LEDGER_ID
      AND ods.PERIOD_NAME = stg.PERIOD_NAME
      AND stg.kca_operation IN ('INSERT', 'UPDATE')
  );
/* Insert records */
INSERT INTO silver_bec_ods.GL_PERIOD_STATUSES (
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
  IS_DELETED_FLG,
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
    'N' AS IS_DELETED_FLG,
    CAST(NULLIF(KCA_SEQ_ID, '') AS DECIMAL(36, 0)) AS KCA_SEQ_ID,
    kca_seq_date
  FROM bronze_bec_ods_stg.gl_period_statuses
  WHERE
    kca_operation IN ('INSERT', 'UPDATE')
    AND (APPLICATION_ID, LEDGER_ID, PERIOD_NAME, kca_seq_id) IN (
      SELECT
        APPLICATION_ID,
        LEDGER_ID,
        PERIOD_NAME,
        MAX(kca_seq_id)
      FROM bronze_bec_ods_stg.gl_period_statuses
      WHERE
        kca_operation IN ('INSERT', 'UPDATE')
      GROUP BY
        APPLICATION_ID,
        LEDGER_ID,
        PERIOD_NAME
    )
);
/* Soft delete */
UPDATE silver_bec_ods.gl_period_statuses SET IS_DELETED_FLG = 'N';
UPDATE silver_bec_ods.gl_period_statuses SET IS_DELETED_FLG = 'Y'
WHERE
  (APPLICATION_ID, LEDGER_ID, PERIOD_NAME) IN (
    SELECT
      APPLICATION_ID,
      LEDGER_ID,
      PERIOD_NAME
    FROM bec_raw_dl_ext.gl_period_statuses
    WHERE
      (APPLICATION_ID, LEDGER_ID, PERIOD_NAME, KCA_SEQ_ID) IN (
        SELECT
          APPLICATION_ID,
          LEDGER_ID,
          PERIOD_NAME,
          MAX(KCA_SEQ_ID) AS KCA_SEQ_ID
        FROM bec_raw_dl_ext.gl_period_statuses
        GROUP BY
          APPLICATION_ID,
          LEDGER_ID,
          PERIOD_NAME
      )
      AND kca_operation = 'DELETE'
  );
UPDATE bec_etl_ctrl.batch_ods_info SET load_type = 'I', last_refresh_date = CURRENT_TIMESTAMP()
WHERE
  ods_table_name = 'gl_period_statuses';