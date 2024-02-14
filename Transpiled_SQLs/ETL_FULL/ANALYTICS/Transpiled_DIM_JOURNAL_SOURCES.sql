DROP table IF EXISTS gold_bec_dwh.dim_journal_sources;
CREATE TABLE gold_bec_dwh.dim_journal_sources AS
(
  SELECT
    je_source_name AS je_source_name,
    description AS description,
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
    ) || '-' || COALESCE(je_source_name, 'NA') AS dw_load_id,
    CURRENT_TIMESTAMP() AS dw_insert_date,
    CURRENT_TIMESTAMP() AS dw_update_date
  FROM silver_bec_ods.gl_je_sources_tl
  WHERE
    1 = 1 AND language = 'US'
);
UPDATE bec_etl_ctrl.batch_dw_info SET load_type = 'I', last_refresh_date = CURRENT_TIMESTAMP()
WHERE
  dw_table_name = 'dim_journal_sources' AND batch_name = 'gl';