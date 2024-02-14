/* Delete Records */
DELETE FROM gold_bec_dwh.DIM_AP_CHECK_FORMATS
WHERE
  EXISTS(
    SELECT
      1
    FROM silver_bec_ods.ap_check_formats AS ods
    WHERE
      ods.check_format_id = dim_ap_check_formats.check_format_id
      AND ods.kca_seq_date > (
        SELECT
          executebegints - prune_days
        FROM bec_etl_ctrl.batch_dw_info
        WHERE
          dw_table_name = 'dim_ap_check_formats' AND batch_name = 'ap'
      )
  );
/* Insert records */
INSERT INTO gold_bec_dwh.DIM_AP_CHECK_FORMATS (
  check_format_id,
  check_format,
  invoices_per_stub,
  payment_method,
  currency_code,
  is_deleted_flg,
  source_app_id,
  dw_load_id,
  dw_insert_date,
  dw_update_date
)
(
  SELECT
    check_format_id,
    name AS `CHECK_FORMAT`,
    invoices_per_stub,
    payment_method_lookup_code AS `PAYMENT_METHOD`,
    currency_code,
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
    ) || '-' || COALESCE(check_format_id, 0) AS dw_load_id,
    CURRENT_TIMESTAMP() AS dw_insert_date,
    CURRENT_TIMESTAMP() AS dw_update_date
  FROM silver_bec_ods.ap_check_formats
  WHERE
    1 = 1
    AND (
      kca_seq_date > (
        SELECT
          (
            executebegints - prune_days
          )
        FROM bec_etl_ctrl.batch_dw_info
        WHERE
          dw_table_name = 'dim_ap_check_formats' AND batch_name = 'ap'
      )
    )
);
/* Soft delete */
UPDATE gold_bec_dwh.dim_ap_check_formats SET is_deleted_flg = 'Y'
WHERE
  NOT EXISTS(
    SELECT
      1
    FROM silver_bec_ods.ap_check_formats AS ods
    WHERE
      ods.check_format_id = dim_ap_check_formats.check_format_id
      AND ods.is_deleted_flg <> 'Y'
  );
UPDATE bec_etl_ctrl.batch_dw_info SET last_refresh_date = CURRENT_TIMESTAMP()
WHERE
  dw_table_name = 'dim_ap_check_formats' AND batch_name = 'ap';