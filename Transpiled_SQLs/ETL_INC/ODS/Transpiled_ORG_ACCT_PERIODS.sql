/* Delete Records */
DELETE FROM silver_bec_ods.ORG_ACCT_PERIODS
WHERE
  (COALESCE(ACCT_PERIOD_ID, 0), COALESCE(ORGANIZATION_ID, 0)) IN (
    SELECT
      COALESCE(stg.ACCT_PERIOD_ID, 0) AS ACCT_PERIOD_ID,
      COALESCE(stg.ORGANIZATION_ID, 0) AS ORGANIZATION_ID
    FROM silver_bec_ods.ORG_ACCT_PERIODS AS ods, bronze_bec_ods_stg.ORG_ACCT_PERIODS AS stg
    WHERE
      COALESCE(ods.ACCT_PERIOD_ID, 0) = COALESCE(stg.ACCT_PERIOD_ID, 0)
      AND COALESCE(ods.ORGANIZATION_ID, 0) = COALESCE(stg.ORGANIZATION_ID, 0)
      AND stg.kca_operation IN ('INSERT', 'UPDATE')
  );
/* Insert records */
INSERT INTO silver_bec_ods.ORG_ACCT_PERIODS (
  acct_period_id,
  organization_id,
  last_update_date,
  last_updated_by,
  creation_date,
  created_by,
  last_update_login,
  period_set_name,
  period_year,
  period_num,
  period_name,
  description,
  period_start_date,
  schedule_close_date,
  period_close_date,
  open_flag,
  global_attribute_category,
  global_attribute1,
  global_attribute2,
  global_attribute3,
  global_attribute4,
  global_attribute5,
  global_attribute6,
  global_attribute7,
  global_attribute8,
  global_attribute9,
  global_attribute10,
  global_attribute11,
  global_attribute12,
  global_attribute13,
  global_attribute14,
  global_attribute15,
  global_attribute16,
  global_attribute17,
  global_attribute18,
  global_attribute19,
  global_attribute20,
  summarized_flag,
  kca_operation,
  IS_DELETED_FLG,
  kca_seq_id,
  kca_seq_date
)
(
  SELECT
    acct_period_id,
    organization_id,
    last_update_date,
    last_updated_by,
    creation_date,
    created_by,
    last_update_login,
    period_set_name,
    period_year,
    period_num,
    period_name,
    description,
    period_start_date,
    schedule_close_date,
    period_close_date,
    open_flag,
    global_attribute_category,
    global_attribute1,
    global_attribute2,
    global_attribute3,
    global_attribute4,
    global_attribute5,
    global_attribute6,
    global_attribute7,
    global_attribute8,
    global_attribute9,
    global_attribute10,
    global_attribute11,
    global_attribute12,
    global_attribute13,
    global_attribute14,
    global_attribute15,
    global_attribute16,
    global_attribute17,
    global_attribute18,
    global_attribute19,
    global_attribute20,
    summarized_flag,
    kca_operation,
    'N' AS IS_DELETED_FLG,
    CAST(NULLIF(KCA_SEQ_ID, '') AS DECIMAL(36, 0)) AS KCA_SEQ_ID,
    kca_seq_date
  FROM bronze_bec_ods_stg.ORG_ACCT_PERIODS
  WHERE
    kca_operation IN ('INSERT', 'UPDATE')
    AND (COALESCE(ACCT_PERIOD_ID, 0), COALESCE(ORGANIZATION_ID, 0), KCA_SEQ_ID) IN (
      SELECT
        COALESCE(ACCT_PERIOD_ID, 0) AS ACCT_PERIOD_ID,
        COALESCE(ORGANIZATION_ID, 0) AS ORGANIZATION_ID,
        MAX(KCA_SEQ_ID)
      FROM bronze_bec_ods_stg.ORG_ACCT_PERIODS
      WHERE
        kca_operation IN ('INSERT', 'UPDATE')
      GROUP BY
        ACCT_PERIOD_ID,
        ORGANIZATION_ID
    )
);
/* Soft delete */
UPDATE silver_bec_ods.ORG_ACCT_PERIODS SET IS_DELETED_FLG = 'N';
UPDATE silver_bec_ods.ORG_ACCT_PERIODS SET IS_DELETED_FLG = 'Y'
WHERE
  (COALESCE(ACCT_PERIOD_ID, 0), COALESCE(ORGANIZATION_ID, 0)) IN (
    SELECT
      COALESCE(ACCT_PERIOD_ID, 0),
      COALESCE(ORGANIZATION_ID, 0)
    FROM bec_raw_dl_ext.ORG_ACCT_PERIODS
    WHERE
      (COALESCE(ACCT_PERIOD_ID, 0), COALESCE(ORGANIZATION_ID, 0), KCA_SEQ_ID) IN (
        SELECT
          COALESCE(ACCT_PERIOD_ID, 0),
          COALESCE(ORGANIZATION_ID, 0),
          MAX(KCA_SEQ_ID) AS KCA_SEQ_ID
        FROM bec_raw_dl_ext.ORG_ACCT_PERIODS
        GROUP BY
          COALESCE(ACCT_PERIOD_ID, 0),
          COALESCE(ORGANIZATION_ID, 0)
      )
      AND kca_operation = 'DELETE'
  );
UPDATE bec_etl_ctrl.batch_ods_info SET last_refresh_date = CURRENT_TIMESTAMP()
WHERE
  ods_table_name = 'org_acct_periods';