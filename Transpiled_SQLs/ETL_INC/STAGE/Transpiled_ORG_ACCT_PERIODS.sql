TRUNCATE table
	table bronze_bec_ods_stg.ORG_ACCT_PERIODS;
INSERT INTO bronze_bec_ods_stg.ORG_ACCT_PERIODS (
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
  KCA_OPERATION,
  KCA_SEQ_ID,
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
    KCA_OPERATION,
    KCA_SEQ_ID,
    kca_seq_date
  FROM bec_raw_dl_ext.ORG_ACCT_PERIODS
  WHERE
    kca_operation <> 'DELETE'
    AND (COALESCE(ACCT_PERIOD_ID, 0), COALESCE(ORGANIZATION_ID, 0), KCA_SEQ_ID) IN (
      SELECT
        COALESCE(ACCT_PERIOD_ID, 0) AS ACCT_PERIOD_ID,
        COALESCE(ORGANIZATION_ID, 0) AS ORGANIZATION_ID,
        MAX(KCA_SEQ_ID)
      FROM bec_raw_dl_ext.ORG_ACCT_PERIODS
      WHERE
        kca_operation <> 'DELETE' AND COALESCE(kca_seq_id, '') <> ''
      GROUP BY
        COALESCE(ACCT_PERIOD_ID, 0),
        COALESCE(ORGANIZATION_ID, 0)
    )
    AND kca_seq_date > (
      SELECT
        (
          executebegints - prune_days
        )
      FROM bec_etl_ctrl.batch_ods_info
      WHERE
        ods_table_name = 'org_acct_periods'
    )
);