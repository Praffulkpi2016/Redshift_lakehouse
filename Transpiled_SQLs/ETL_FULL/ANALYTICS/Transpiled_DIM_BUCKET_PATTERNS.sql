DROP table IF EXISTS gold_bec_dwh.DIM_BUCKET_PATTERNS;
CREATE TABLE gold_bec_dwh.DIM_BUCKET_PATTERNS AS
(
  SELECT
    BUCKET_PATTERN_ID,
    BUCKET_PATTERN_NAME,
    NUMBER_DAILY_BUCKETS,
    NUMBER_WEEKLY_BUCKETS,
    NUMBER_MONTHLY_BUCKETS,
    NUMBER_QUARTERLY_BUCKETS,
    LAST_UPDATE_DATE,
    LAST_UPDATED_BY,
    CREATION_DATE,
    CREATED_BY,
    WEEK_START_DAY,
    DESCRIPTION,
    INACTIVE_DATE,
    'N' AS is_deleted_flg, /* Audit columns */
    (
      SELECT
        system_id
      FROM bec_etl_ctrl.etlsourceappid
      WHERE
        source_system = 'EBS'
    ) AS source_app_id,
    (
      SELECT
        system_id
      FROM bec_etl_ctrl.etlsourceappid
      WHERE
        source_system = 'EBS'
    ) || '-' || COALESCE(BUCKET_PATTERN_ID, 0) AS dw_load_id,
    CURRENT_TIMESTAMP() AS dw_insert_date,
    CURRENT_TIMESTAMP() AS dw_update_date
  FROM silver_bec_ods.CHV_BUCKET_PATTERNS
);
UPDATE bec_etl_ctrl.batch_dw_info SET load_type = 'I', last_refresh_date = CURRENT_TIMESTAMP()
WHERE
  dw_table_name = 'dim_bucket_patterns' AND batch_name = 'po';