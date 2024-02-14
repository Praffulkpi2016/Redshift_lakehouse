/* Delete Records */
DELETE FROM gold_bec_dwh.dim_lookups
WHERE
  EXISTS(
    SELECT
      1
    FROM silver_bec_ods.fnd_lookup_values AS ods
    WHERE
      COALESCE(dim_lookups.lookup_type, 'NA') = COALESCE(ods.lookup_type, 'NA')
      AND COALESCE(dim_lookups.lookup_code, 'NA') = COALESCE(ods.lookup_code, 'NA')
      AND COALESCE(dim_lookups.language, 'NA') = COALESCE(ods.language, 'NA')
      AND COALESCE(dim_lookups.view_application_id, 0) = COALESCE(ods.view_application_id, 0)
      AND ods.language = 'US'
      AND ods.enabled_flag = 'Y'
      AND (
        ods.kca_seq_date > (
          SELECT
            (
              executebegints - prune_days
            )
          FROM bec_etl_ctrl.batch_dw_info
          WHERE
            dw_table_name = 'dim_lookups' AND batch_name = 'ap'
        )
      )
  );
/* Insert records */
INSERT INTO gold_bec_dwh.dim_lookups (
  lookup_code,
  description,
  lookup_type,
  language,
  meaning,
  enabled_flag,
  view_application_id,
  is_deleted_flg,
  source_app_id,
  dw_load_id,
  dw_insert_date,
  dw_update_date
)
(
  SELECT DISTINCT
    ods.lookup_code AS lookup_code,
    ods.description AS description,
    ods.lookup_type AS lookup_type,
    ods.language AS language,
    ods.meaning AS meaning,
    ods.enabled_flag AS enabled_flag,
    ods.view_application_id AS view_application_id,
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
    ) || '-' || COALESCE(ods.lookup_type, 'NA') || '-' || COALESCE(ods.lookup_code, 'NA') || '-' || COALESCE(ods.language, 'NA') || '-' || COALESCE(ods.view_application_id, 0) AS dw_load_id,
    CURRENT_TIMESTAMP() AS dw_insert_date,
    CURRENT_TIMESTAMP() AS dw_update_date
  FROM silver_bec_ods.fnd_lookup_values AS ods
  WHERE
    1 = 1
    AND ods.language = 'US'
    AND ods.enabled_flag = 'Y'
    AND ods.is_deleted_flg <> 'Y'
    AND (
      ods.kca_seq_date > (
        SELECT
          (
            executebegints - prune_days
          )
        FROM bec_etl_ctrl.batch_dw_info
        WHERE
          dw_table_name = 'dim_lookups' AND batch_name = 'ap'
      )
    )
);
/* Soft Delete Records */
UPDATE gold_bec_dwh.dim_lookups SET is_deleted_flg = 'Y'
WHERE
  NOT EXISTS(
    SELECT
      1
    FROM silver_bec_ods.fnd_lookup_values AS ods
    WHERE
      COALESCE(dim_lookups.lookup_type, 'NA') = COALESCE(ods.lookup_type, 'NA')
      AND COALESCE(dim_lookups.lookup_code, 'NA') = COALESCE(ods.lookup_code, 'NA')
      AND COALESCE(dim_lookups.language, 'NA') = COALESCE(ods.language, 'NA')
      AND COALESCE(dim_lookups.view_application_id, 0) = COALESCE(ods.view_application_id, 0)
      AND ods.language = 'US'
      AND ods.enabled_flag = 'Y'
      AND ods.is_deleted_flg <> 'Y'
  );
UPDATE bec_etl_ctrl.batch_dw_info SET last_refresh_date = CURRENT_TIMESTAMP()
WHERE
  dw_table_name = 'dim_lookups' AND batch_name = 'ap';