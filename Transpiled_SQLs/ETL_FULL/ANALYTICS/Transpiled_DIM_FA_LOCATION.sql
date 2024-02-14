DROP table IF EXISTS gold_bec_dwh.DIM_FA_LOCATION;
CREATE TABLE gold_bec_dwh.dim_fa_location AS
(
  SELECT
    fl.location_id,
    fl.segment1,
    fl.segment2,
    fl.segment3,
    fl.segment4,
    fl.segment5,
    fl.segment6,
    fl.segment7,
    fl.enabled_flag,
    fl.end_date_active,
    fl.last_update_date,
    fl.last_updated_by,
    '0' AS x_custom,
    'N' AS is_deleted_flg, /* audit columns */
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
    ) || '-' || COALESCE(fl.location_id, 0) AS dw_load_id,
    CURRENT_TIMESTAMP() AS dw_insert_date,
    CURRENT_TIMESTAMP() AS dw_update_date
  FROM silver_bec_ods.fa_locations AS fl
);
UPDATE bec_etl_ctrl.batch_dw_info SET load_type = 'I', last_refresh_date = CURRENT_TIMESTAMP()
WHERE
  dw_table_name = 'dim_fa_location' AND batch_name = 'fa';