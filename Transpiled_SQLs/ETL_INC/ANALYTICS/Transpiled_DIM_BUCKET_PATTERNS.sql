/* Delete Records */
DELETE FROM gold_bec_dwh.dim_bucket_patterns
WHERE
  BUCKET_PATTERN_ID IN (
    SELECT
      ods.BUCKET_PATTERN_ID
    FROM gold_bec_dwh.dim_bucket_patterns AS dw, silver_bec_ods.CHV_BUCKET_PATTERNS AS ods
    WHERE
      dw.dw_load_id = (
        SELECT
          system_id
        FROM bec_etl_ctrl.etlsourceappid
        WHERE
          source_system = 'EBS'
      ) || '-' || COALESCE(ods.BUCKET_PATTERN_ID, 0)
      AND (
        ods.kca_seq_date > (
          SELECT
            (
              executebegints - prune_days
            )
          FROM bec_etl_ctrl.batch_dw_info
          WHERE
            dw_table_name = 'dim_bucket_patterns' AND batch_name = 'po'
        )
      )
  );
/* Insert records */
INSERT INTO gold_bec_dwh.dim_bucket_patterns (
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
  is_deleted_flg,
  source_app_id,
  dw_load_id,
  dw_insert_date,
  dw_update_date
)
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
  WHERE
    (
      kca_seq_date > (
        SELECT
          (
            executebegints - prune_days
          )
        FROM bec_etl_ctrl.batch_dw_info
        WHERE
          dw_table_name = 'dim_bucket_patterns' AND batch_name = 'ap'
      )
    )
);
/* Soft delete */
UPDATE gold_bec_dwh.dim_bucket_patterns SET is_deleted_flg = 'Y'
WHERE
  NOT BUCKET_PATTERN_ID IN (
    SELECT
      ods.BUCKET_PATTERN_ID
    FROM gold_bec_dwh.dim_bucket_patterns AS dw, silver_bec_ods.CHV_BUCKET_PATTERNS AS ods
    WHERE
      dw.dw_load_id = (
        SELECT
          system_id
        FROM bec_etl_ctrl.etlsourceappid
        WHERE
          source_system = 'EBS'
      ) || '-' || COALESCE(ods.BUCKET_PATTERN_ID, 0)
      AND ods.is_deleted_flg <> 'Y'
  );
UPDATE bec_etl_ctrl.batch_dw_info SET last_refresh_date = CURRENT_TIMESTAMP()
WHERE
  dw_table_name = 'dim_bucket_patterns' AND batch_name = 'po';