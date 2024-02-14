DROP table IF EXISTS gold_bec_dwh.DIM_AP_TERMS;
CREATE TABLE gold_bec_dwh.DIM_AP_TERMS AS
(
  SELECT
    term_id,
    type AS `AP_TERM_TYPE`,
    start_date_active,
    end_date_active,
    enabled_flag,
    name AS `AP_TERM`,
    description AS `AP_TERM_DESC`,
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
    ) || '-' || COALESCE(term_id, 0) AS dw_load_id,
    CURRENT_TIMESTAMP() AS dw_insert_date,
    CURRENT_TIMESTAMP() AS dw_update_date
  FROM silver_bec_ods.ap_terms_tl
  WHERE
    language = 'US'
);
UPDATE bec_etl_ctrl.batch_dw_info SET load_type = 'I', last_refresh_date = CURRENT_TIMESTAMP()
WHERE
  dw_table_name = 'dim_ap_terms' AND batch_name = 'ap';