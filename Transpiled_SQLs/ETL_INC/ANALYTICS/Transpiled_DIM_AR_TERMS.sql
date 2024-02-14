/* Delete Records */
DELETE FROM gold_bec_dwh.DIM_AR_TERMS
WHERE
  (
    COALESCE(TERM_ID, 0)
  ) IN (
    SELECT
      COALESCE(ods.TERM_ID, 0) AS TERM_ID
    FROM gold_bec_dwh.DIM_AR_TERMS AS dw, silver_bec_ods.RA_TERMS_TL AS ods
    WHERE
      dw.dw_load_id = (
        SELECT
          system_id
        FROM bec_etl_ctrl.etlsourceappid
        WHERE
          source_system = 'EBS'
      ) || '-' || COALESCE(ods.TERM_ID, 0)
      AND (
        ods.kca_seq_date > (
          SELECT
            (
              executebegints - prune_days
            )
          FROM bec_etl_ctrl.batch_dw_info
          WHERE
            dw_table_name = 'dim_ar_terms' AND batch_name = 'ar'
        )
      )
  );
/* Insert records */
INSERT INTO gold_bec_dwh.DIM_AR_TERMS (
  term_id,
  description,
  `name`,
  `language`,
  last_update_date,
  is_deleted_flg,
  source_app_id,
  dw_load_id,
  dw_insert_date,
  dw_update_date
)
(
  SELECT
    TERM_ID,
    DESCRIPTION,
    NAME,
    `LANGUAGE`,
    last_update_date,
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
    ) || '-' || COALESCE(TERM_ID, 0) AS dw_load_id,
    CURRENT_TIMESTAMP() AS dw_insert_date,
    CURRENT_TIMESTAMP() AS dw_update_date
  FROM silver_bec_ods.RA_TERMS_TL
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
          dw_table_name = 'dim_ar_terms' AND batch_name = 'ar'
      )
    )
);
/* Soft delete */
UPDATE gold_bec_dwh.DIM_AR_TERMS SET is_deleted_flg = 'Y'
WHERE
  NOT (
    COALESCE(TERM_ID, 0)
  ) IN (
    SELECT
      COALESCE(ods.TERM_ID, 0) AS TERM_ID
    FROM gold_bec_dwh.DIM_AR_TERMS AS dw, silver_bec_ods.RA_TERMS_TL AS ods
    WHERE
      dw.dw_load_id = (
        SELECT
          system_id
        FROM bec_etl_ctrl.etlsourceappid
        WHERE
          source_system = 'EBS'
      ) || '-' || COALESCE(ods.TERM_ID, 0)
      AND ods.is_deleted_flg <> 'Y'
  );
UPDATE bec_etl_ctrl.batch_dw_info SET last_refresh_date = CURRENT_TIMESTAMP()
WHERE
  dw_table_name = 'dim_ar_terms' AND batch_name = 'ar';