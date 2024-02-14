/* Delete Records */
DELETE FROM gold_bec_dwh.DIM_RA_PAYMENT_TERMS
WHERE
  COALESCE(TERM_ID, 0) IN (
    SELECT
      COALESCE(ods.TERM_ID, 0) AS TERM_ID
    FROM gold_bec_dwh.DIM_RA_PAYMENT_TERMS AS dw, (
      SELECT
        *
      FROM silver_bec_ods.RA_TERMS_TL
      WHERE
        LANGUAGE = 'US'
        AND (
          kca_seq_date > (
            SELECT
              (
                executebegints - prune_days
              )
            FROM bec_etl_ctrl.batch_dw_info
            WHERE
              dw_table_name = 'dim_ra_payment_terms' AND batch_name = 'po'
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
      ) || '-' || COALESCE(ods.TERM_ID, 0)
  );
/* Insert records */
INSERT INTO gold_bec_dwh.DIM_RA_PAYMENT_TERMS (
  term_id,
  `name`,
  description,
  last_update_date,
  is_deleted_flg,
  source_app_id,
  dw_load_id,
  dw_insert_date,
  dw_update_date
)
(
  SELECT
    term_id AS `TERM_ID`,
    `name`,
    description,
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
    LANGUAGE = 'US'
    AND (
      kca_seq_date > (
        SELECT
          (
            executebegints - prune_days
          )
        FROM bec_etl_ctrl.batch_dw_info
        WHERE
          dw_table_name = 'dim_ra_payment_terms' AND batch_name = 'po'
      )
    )
);
/* Soft delete */
UPDATE gold_bec_dwh.DIM_RA_PAYMENT_TERMS SET is_deleted_flg = 'Y'
WHERE
  NOT COALESCE(TERM_ID, 0) IN (
    SELECT
      COALESCE(ods.TERM_ID, 0) AS TERM_ID
    FROM gold_bec_dwh.DIM_RA_PAYMENT_TERMS AS dw, (
      SELECT
        *
      FROM silver_bec_ods.RA_TERMS_TL
      WHERE
        LANGUAGE = 'US' AND is_deleted_flg <> 'Y'
    ) AS ods
    WHERE
      dw.dw_load_id = (
        SELECT
          system_id
        FROM bec_etl_ctrl.etlsourceappid
        WHERE
          source_system = 'EBS'
      ) || '-' || COALESCE(ods.TERM_ID, 0)
  );
UPDATE bec_etl_ctrl.batch_dw_info SET last_refresh_date = CURRENT_TIMESTAMP()
WHERE
  dw_table_name = 'dim_ra_payment_terms' AND batch_name = 'po';