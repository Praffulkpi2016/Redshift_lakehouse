DROP table IF EXISTS gold_bec_dwh.DIM_AR_AGING_BUCKETS;
CREATE TABLE gold_bec_dwh.DIM_AR_AGING_BUCKETS AS
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
  ORDER BY
    a.AGING_BUCKET_ID NULLS LAST,
    a.BUCKET_NAME NULLS LAST,
    b.BUCKET_SEQUENCE_NUM NULLS LAST
);
UPDATE bec_etl_ctrl.batch_dw_info SET load_type = 'I', last_refresh_date = CURRENT_TIMESTAMP()
WHERE
  dw_table_name = 'dim_ar_aging_buckets' AND batch_name = 'ar';