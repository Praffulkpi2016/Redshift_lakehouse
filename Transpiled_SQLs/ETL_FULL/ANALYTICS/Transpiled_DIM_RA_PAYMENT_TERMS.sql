DROP table IF EXISTS gold_bec_dwh.DIM_RA_PAYMENT_TERMS;
CREATE TABLE gold_bec_dwh.DIM_RA_PAYMENT_TERMS AS
(
  SELECT
    term_id AS `TERM_ID`,
    NAME,
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
);
UPDATE bec_etl_ctrl.batch_dw_info SET load_type = 'I', last_refresh_date = CURRENT_TIMESTAMP()
WHERE
  dw_table_name = 'dim_ra_payment_terms' AND batch_name = 'po';