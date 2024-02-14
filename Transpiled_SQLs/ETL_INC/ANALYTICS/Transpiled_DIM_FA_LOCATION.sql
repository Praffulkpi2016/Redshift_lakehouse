/* Delete Records */
DELETE FROM gold_bec_dwh.dim_fa_location
WHERE
  COALESCE(location_id, 0) IN (
    SELECT
      COALESCE(ods.location_id, 0) AS location_id
    FROM gold_bec_dwh.dim_fa_location AS dw, (
      SELECT
        fl.location_id
      FROM silver_bec_ods.fa_locations AS fl
      WHERE
        (
          fl.kca_seq_date > (
            SELECT
              (
                executebegints - prune_days
              )
            FROM bec_etl_ctrl.batch_dw_info
            WHERE
              dw_table_name = 'dim_fa_location' AND batch_name = 'fa'
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
      ) || '-' || COALESCE(ods.location_id, 0)
  );
/* Insert records */
INSERT INTO gold_bec_dwh.dim_fa_location (
  location_id,
  segment1,
  segment2,
  segment3,
  segment4,
  segment5,
  segment6,
  segment7,
  enabled_flag,
  end_date_active,
  last_update_date,
  last_updated_by,
  x_custom,
  is_deleted_flg,
  source_app_id,
  dw_load_id,
  dw_insert_date,
  dw_update_date
)
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
  WHERE
    (
      fl.kca_seq_date > (
        SELECT
          (
            executebegints - prune_days
          )
        FROM bec_etl_ctrl.batch_dw_info
        WHERE
          dw_table_name = 'dim_fa_location' AND batch_name = 'fa'
      )
    )
);
/* Soft delete */
UPDATE gold_bec_dwh.dim_fa_location SET is_deleted_flg = 'Y'
WHERE
  NOT COALESCE(location_id, 0) IN (
    SELECT
      COALESCE(ods.location_id, 0) AS location_id
    FROM gold_bec_dwh.dim_fa_location AS dw, (
      SELECT
        fl.location_id
      FROM silver_bec_ods.fa_locations AS fl
      WHERE
        is_deleted_flg <> 'Y'
    ) AS ods
    WHERE
      dw.dw_load_id = (
        SELECT
          system_id
        FROM bec_etl_ctrl.etlsourceappid
        WHERE
          source_system = 'EBS'
      ) || '-' || COALESCE(ods.location_id, 0)
  );
UPDATE bec_etl_ctrl.batch_dw_info SET last_refresh_date = CURRENT_TIMESTAMP()
WHERE
  dw_table_name = 'dim_fa_location' AND batch_name = 'fa';