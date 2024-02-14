DROP table IF EXISTS gold_bec_dwh.DIM_LOOKUPS;
CREATE TABLE gold_bec_dwh.DIM_LOOKUPS AS
(
  SELECT
    fnd.lookup_code AS lookup_code,
    fnd.description AS description,
    fnd.lookup_type AS lookup_type,
    fnd.language AS language,
    fnd.meaning AS meaning,
    fnd.enabled_flag AS enabled_flag,
    fnd.view_application_id AS view_application_id,
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
    ) || '-' || COALESCE(fnd.lookup_type, 'NA') || '-' || COALESCE(fnd.lookup_code, 'NA') || '-' || COALESCE(fnd.language, 'NA') || '-' || COALESCE(fnd.view_application_id, 0) AS dw_load_id,
    CURRENT_TIMESTAMP() AS dw_insert_date,
    CURRENT_TIMESTAMP() AS dw_update_date
  FROM silver_bec_ods.fnd_lookup_values AS fnd
  WHERE
    1 = 1 AND language = 'US' AND enabled_flag = 'Y' AND is_deleted_flg <> 'Y'
);
UPDATE bec_etl_ctrl.batch_dw_info SET load_type = 'I', last_refresh_date = CURRENT_TIMESTAMP()
WHERE
  dw_table_name = 'dim_lookups' AND batch_name = 'ap';