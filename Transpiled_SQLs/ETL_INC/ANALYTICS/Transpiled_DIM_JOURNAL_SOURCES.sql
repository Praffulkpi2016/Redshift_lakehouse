/* Delete Records */
DELETE FROM gold_bec_dwh.dim_journal_sources
WHERE
  (
    COALESCE(je_source_name, 'NA')
  ) IN (
    SELECT
      COALESCE(ods.je_source_name, 'NA') AS je_source_name
    FROM gold_bec_dwh.dim_journal_sources AS dw, silver_bec_ods.gl_je_sources_tl AS ods
    WHERE
      dw.dw_load_id = (
        SELECT
          system_id
        FROM bec_etl_ctrl.etlsourceappid
        WHERE
          source_system = 'EBS'
      ) || '-' || COALESCE(ods.je_source_name, 'NA')
      AND ods.language = 'US'
      AND (
        ods.kca_seq_date > (
          SELECT
            (
              executebegints - prune_days
            )
          FROM bec_etl_ctrl.batch_dw_info
          WHERE
            dw_table_name = 'dim_journal_sources' AND batch_name = 'gl'
        )
      )
  );
/* Insert records */
INSERT INTO gold_bec_dwh.dim_journal_sources (
  je_source_name,
  description,
  is_deleted_flg,
  source_app_id,
  dw_load_id,
  dw_insert_date,
  dw_update_date
)
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
    1 = 1
    AND language = 'US'
    AND (
      kca_seq_date > (
        SELECT
          (
            executebegints - prune_days
          )
        FROM bec_etl_ctrl.batch_dw_info
        WHERE
          dw_table_name = 'dim_journal_sources' AND batch_name = 'gl'
      )
    )
);
/* Soft delete */
UPDATE gold_bec_dwh.dim_journal_sources SET is_deleted_flg = 'Y'
WHERE
  NOT (
    COALESCE(je_source_name, 'NA')
  ) IN (
    SELECT
      COALESCE(ods.je_source_name, 'NA') AS je_source_name
    FROM gold_bec_dwh.dim_journal_sources AS dw, silver_bec_ods.gl_je_sources_tl AS ods
    WHERE
      dw.dw_load_id = (
        SELECT
          system_id
        FROM bec_etl_ctrl.etlsourceappid
        WHERE
          source_system = 'EBS'
      ) || '-' || COALESCE(ods.je_source_name, 'NA')
      AND ods.language = 'US'
      AND ods.is_deleted_flg <> 'Y'
  );
UPDATE bec_etl_ctrl.batch_dw_info SET last_refresh_date = CURRENT_TIMESTAMP()
WHERE
  dw_table_name = 'dim_journal_sources' AND batch_name = 'gl';