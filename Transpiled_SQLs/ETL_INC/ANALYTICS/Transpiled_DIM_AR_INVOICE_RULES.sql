/* Delete Records */
DELETE FROM gold_bec_dwh.DIM_AR_INVOICE_RULES
WHERE
  (
    COALESCE(RULE_ID, 0)
  ) IN (
    SELECT
      COALESCE(ods.RULE_ID, 0) AS RULE_ID
    FROM gold_bec_dwh.DIM_AR_INVOICE_RULES AS dw, silver_bec_ods.RA_RULES AS ods
    WHERE
      dw.dw_load_id = (
        SELECT
          system_id
        FROM bec_etl_ctrl.etlsourceappid
        WHERE
          source_system = 'EBS'
      ) || '-' || COALESCE(ods.RULE_ID, 0)
      AND (
        ods.kca_seq_date > (
          SELECT
            (
              executebegints - prune_days
            )
          FROM bec_etl_ctrl.batch_dw_info
          WHERE
            dw_table_name = 'dim_ar_invoice_rules' AND batch_name = 'ar'
        )
      )
  );
/* Insert records */
INSERT INTO gold_bec_dwh.DIM_AR_INVOICE_RULES (
  rule_id,
  invoicing_rule,
  status,
  last_update_date,
  is_deleted_flg,
  source_app_id,
  dw_load_id,
  dw_insert_date,
  dw_update_date
)
(
  SELECT
    rule_id AS RULE_ID,
    NAME AS INVOICING_RULE,
    status,
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
    ) || '-' || COALESCE(RULE_ID, 0) AS dw_load_id,
    CURRENT_TIMESTAMP() AS dw_insert_date,
    CURRENT_TIMESTAMP() AS dw_update_date
  FROM silver_bec_ods.ra_rules
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
          dw_table_name = 'dim_ar_invoice_rules' AND batch_name = 'ar'
      )
    )
);
/* Soft delete */
UPDATE gold_bec_dwh.DIM_AR_INVOICE_RULES SET is_deleted_flg = 'Y'
WHERE
  NOT COALESCE(RULE_ID, 0) IN (
    SELECT
      COALESCE(ods.RULE_ID, 0) AS RULE_ID
    FROM gold_bec_dwh.DIM_AR_INVOICE_RULES AS dw, silver_bec_ods.RA_RULES AS ods
    WHERE
      dw.dw_load_id = (
        SELECT
          system_id
        FROM bec_etl_ctrl.etlsourceappid
        WHERE
          source_system = 'EBS'
      ) || '-' || COALESCE(ods.RULE_ID, 0)
      AND ods.is_deleted_flg <> 'Y'
  );
UPDATE bec_etl_ctrl.batch_dw_info SET last_refresh_date = CURRENT_TIMESTAMP()
WHERE
  dw_table_name = 'dim_ar_invoice_rules' AND batch_name = 'ar';