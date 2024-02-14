/* Delete Records */
DELETE FROM gold_bec_dwh.DIM_AR_AGING_BUCKETS
WHERE
  (COALESCE(AGING_BUCKET_ID, 0), COALESCE(BUCKET_NAME, 'NA'), COALESCE(BUCKET_SEQUENCE_NUM, 0)) IN (
    SELECT
      COALESCE(ods.AGING_BUCKET_ID, 0) AS AGING_BUCKET_ID,
      COALESCE(ods.BUCKET_NAME, 'NA') AS BUCKET_NAME,
      COALESCE(ods.BUCKET_SEQUENCE_NUM, 0) AS BUCKET_SEQUENCE_NUM
    FROM gold_bec_dwh.DIM_AR_AGING_BUCKETS AS dw, (
      SELECT
        a.AGING_BUCKET_ID,
        a.BUCKET_NAME,
        b.BUCKET_SEQUENCE_NUM
      FROM silver_bec_ods.AR_AGING_BUCKETS AS a, silver_bec_ods.AR_AGING_BUCKET_LINES_B AS b
      WHERE
        a.AGING_BUCKET_ID = b.AGING_BUCKET_ID
        AND (
          a.kca_seq_date > (
            SELECT
              (
                executebegints - prune_days
              )
            FROM bec_etl_ctrl.batch_dw_info
            WHERE
              dw_table_name = 'dim_ar_aging_buckets' AND batch_name = 'ar'
          )
          OR b.kca_seq_date > (
            SELECT
              (
                executebegints - prune_days
              )
            FROM bec_etl_ctrl.batch_dw_info
            WHERE
              dw_table_name = 'dim_ar_aging_buckets' AND batch_name = 'ar'
          )
        )
    ) AS ods
    WHERE
      dw.dw_load_id = (
        SELECT
          system_id
        FROM bec_etl_ctrl.etlsourceappid
        WHERE
          source_system = 'EBS'
      ) || '-' || COALESCE(ods.AGING_BUCKET_ID, 0) || '-' || COALESCE(ods.BUCKET_NAME, 'NA') || '-' || COALESCE(ods.BUCKET_SEQUENCE_NUM, 0)
  );
/* Insert records */
INSERT INTO gold_bec_dwh.DIM_AR_AGING_BUCKETS (
  aging_bucket_id,
  bucket_name,
  bucket_sequence_num,
  days_start,
  days_to,
  `type`,
  last_update_date,
  is_deleted_flg,
  source_app_id,
  dw_load_id,
  dw_insert_date,
  dw_update_date
)
(
  SELECT
    a.AGING_BUCKET_ID,
    a.BUCKET_NAME,
    b.BUCKET_SEQUENCE_NUM,
    b.DAYS_START,
    b.DAYS_TO,
    b.`TYPE`,
    b.last_update_date,
    'N' AS is_deleted_flg,
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
    ) || '-' || COALESCE(a.AGING_BUCKET_ID, 0) || '-' || COALESCE(a.BUCKET_NAME, 'NA') || '-' || COALESCE(b.BUCKET_SEQUENCE_NUM, 0) AS dw_load_id,
    CURRENT_TIMESTAMP() AS dw_insert_date,
    CURRENT_TIMESTAMP() AS dw_update_date
  FROM silver_bec_ods.AR_AGING_BUCKETS AS a, silver_bec_ods.AR_AGING_BUCKET_LINES_B AS b
  WHERE
    a.AGING_BUCKET_ID = b.AGING_BUCKET_ID
    AND (
      a.kca_seq_date > (
        SELECT
          (
            executebegints - prune_days
          )
        FROM bec_etl_ctrl.batch_dw_info
        WHERE
          dw_table_name = 'dim_ar_aging_buckets' AND batch_name = 'ar'
      )
      OR b.kca_seq_date > (
        SELECT
          (
            executebegints - prune_days
          )
        FROM bec_etl_ctrl.batch_dw_info
        WHERE
          dw_table_name = 'dim_ar_aging_buckets' AND batch_name = 'ar'
      )
    )
);
/* Soft delete */
UPDATE gold_bec_dwh.DIM_AR_AGING_BUCKETS SET is_deleted_flg = 'Y'
WHERE
  NOT (COALESCE(AGING_BUCKET_ID, 0), COALESCE(BUCKET_NAME, 'NA'), COALESCE(BUCKET_SEQUENCE_NUM, 0)) IN (
    SELECT
      COALESCE(ods.AGING_BUCKET_ID, 0) AS AGING_BUCKET_ID,
      COALESCE(ods.BUCKET_NAME, 'NA') AS BUCKET_NAME,
      COALESCE(ods.BUCKET_SEQUENCE_NUM, 0) AS BUCKET_SEQUENCE_NUM
    FROM gold_bec_dwh.DIM_AR_AGING_BUCKETS AS dw, (
      SELECT
        a.AGING_BUCKET_ID,
        a.BUCKET_NAME,
        b.BUCKET_SEQUENCE_NUM,
        b.last_update_date,
        b.kca_operation
      FROM (
        SELECT
          *
        FROM silver_bec_ods.AR_AGING_BUCKETS
        WHERE
          is_deleted_flg <> 'Y'
      ) AS a, (
        SELECT
          *
        FROM silver_bec_ods.AR_AGING_BUCKET_LINES_B
        WHERE
          is_deleted_flg <> 'Y'
      ) AS b
      WHERE
        a.AGING_BUCKET_ID = b.AGING_BUCKET_ID
    ) AS ods
    WHERE
      dw.dw_load_id = (
        SELECT
          system_id
        FROM bec_etl_ctrl.etlsourceappid
        WHERE
          source_system = 'EBS'
      ) || '-' || COALESCE(ods.AGING_BUCKET_ID, 0) || '-' || COALESCE(ods.BUCKET_NAME, 'NA') || '-' || COALESCE(ods.BUCKET_SEQUENCE_NUM, 0)
  );
UPDATE bec_etl_ctrl.batch_dw_info SET last_refresh_date = CURRENT_TIMESTAMP()
WHERE
  dw_table_name = 'dim_ar_aging_buckets' AND batch_name = 'ar';