/* Delete Records */
DELETE FROM gold_bec_dwh.dim_ap_terms
WHERE
  COALESCE(term_id, 0) IN (
    SELECT
      COALESCE(ods.term_id, 0)
    FROM gold_bec_dwh.dim_ap_terms AS dw, silver_bec_ods.ap_terms_tl AS ods
    WHERE
      dw.dw_load_id = (
        SELECT
          system_id
        FROM bec_etl_ctrl.etlsourceappid
        WHERE
          source_system = 'EBS'
      ) || '-' || COALESCE(ods.term_id, 0)
      AND (
        ods.kca_seq_date > (
          SELECT
            (
              executebegints - prune_days
            )
          FROM bec_etl_ctrl.batch_dw_info
          WHERE
            dw_table_name = 'dim_ap_terms' AND batch_name = 'ap'
        )
      )
  );
/* Insert records */
INSERT INTO gold_bec_dwh.DIM_AP_TERMS (
  term_id,
  ap_term_type,
  start_date_active,
  end_date_active,
  enabled_flag,
  ap_term,
  ap_term_desc,
  is_deleted_flg,
  source_app_id,
  dw_load_id,
  dw_insert_date,
  dw_update_date
)
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
          dw_table_name = 'dim_ap_terms' AND batch_name = 'ap'
      )
    )
);
/* Soft delete */
UPDATE gold_bec_dwh.dim_ap_terms SET is_deleted_flg = 'Y'
WHERE
  NOT COALESCE(term_id, 0) IN (
    SELECT
      COALESCE(ods.term_id, 0)
    FROM gold_bec_dwh.dim_ap_terms AS dw, silver_bec_ods.ap_terms_tl AS ods
    WHERE
      dw.dw_load_id = (
        SELECT
          system_id
        FROM bec_etl_ctrl.etlsourceappid
        WHERE
          source_system = 'EBS'
      ) || '-' || COALESCE(ods.term_id, 0)
      AND ods.is_deleted_flg <> 'Y'
  );
UPDATE bec_etl_ctrl.batch_dw_info SET last_refresh_date = CURRENT_TIMESTAMP()
WHERE
  dw_table_name = 'dim_ap_terms' AND batch_name = 'ap';