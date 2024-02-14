/* Delete Records */
DELETE FROM gold_bec_dwh.DIM_AR_SALESREPS
WHERE
  (COALESCE(salesrep_id, 0), COALESCE(org_id, 0)) IN (
    SELECT
      COALESCE(ods.SALESREP_ID, 0),
      COALESCE(ods.org_id, 0)
    FROM gold_bec_dwh.DIM_AR_SALESREPS AS dw, silver_bec_ods.JTF_RS_SALESREPS AS ods
    WHERE
      dw.dw_load_id = (
        SELECT
          system_id
        FROM bec_etl_ctrl.etlsourceappid
        WHERE
          source_system = 'EBS'
      ) || '-' || COALESCE(ods.SALESREP_ID, 0) || '-' || COALESCE(ods.org_id, 0)
      AND (
        ods.kca_seq_date > (
          SELECT
            (
              executebegints - prune_days
            )
          FROM bec_etl_ctrl.batch_dw_info
          WHERE
            dw_table_name = 'dim_ar_salesreps' AND batch_name = 'ar'
        )
      )
  );
/* Insert records */
INSERT INTO gold_bec_dwh.DIM_AR_SALESREPS (
  salesrep_id,
  resource_id,
  sales_credit_type_id,
  `name`,
  status,
  person_id,
  salesrep_number,
  org_id,
  last_update_date,
  is_deleted_flg,
  source_app_id,
  dw_load_id,
  dw_insert_date,
  dw_update_date
)
(
  SELECT
    SALESREP_ID,
    RESOURCE_ID,
    SALES_CREDIT_TYPE_ID,
    NAME,
    STATUS,
    PERSON_ID,
    SALESREP_NUMBER,
    ORG_ID,
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
    ) || '-' || COALESCE(SALESREP_ID, 0) || '-' || COALESCE(ORG_ID, 0) AS dw_load_id,
    CURRENT_TIMESTAMP() AS dw_insert_date,
    CURRENT_TIMESTAMP() AS dw_update_date
  FROM silver_bec_ods.JTF_RS_SALESREPS
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
          dw_table_name = 'dim_ar_salesreps' AND batch_name = 'ar'
      )
    )
);
/* Soft delete */
UPDATE gold_bec_dwh.DIM_AR_SALESREPS SET is_deleted_flg = 'Y'
WHERE
  NOT (COALESCE(SALESREP_ID, 0), COALESCE(ORG_ID, 0)) IN (
    SELECT
      COALESCE(ods.SALESREP_ID, 0) AS SALESREP_ID,
      COALESCE(ods.ORG_ID, 0) AS ORG_ID
    FROM gold_bec_dwh.DIM_AR_SALESREPS AS dw, silver_bec_ods.JTF_RS_SALESREPS AS ods
    WHERE
      dw.dw_load_id = (
        SELECT
          system_id
        FROM bec_etl_ctrl.etlsourceappid
        WHERE
          source_system = 'EBS'
      ) || '-' || COALESCE(ods.SALESREP_ID, 0) || '-' || COALESCE(ods.ORG_ID, 0)
      AND ods.is_deleted_flg <> 'Y'
  );
UPDATE bec_etl_ctrl.batch_dw_info SET last_refresh_date = CURRENT_TIMESTAMP()
WHERE
  dw_table_name = 'dim_ar_salesreps' AND batch_name = 'ar';