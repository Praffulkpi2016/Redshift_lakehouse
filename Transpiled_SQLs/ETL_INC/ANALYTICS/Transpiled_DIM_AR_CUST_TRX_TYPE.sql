/* Delete Records */
DELETE FROM gold_bec_dwh.DIM_AR_CUST_TRX_TYPE
WHERE
  (COALESCE(CUST_TRX_TYPE_ID, 0), COALESCE(ORG_ID, 0)) IN (
    SELECT
      COALESCE(ods.CUST_TRX_TYPE_ID, 0),
      COALESCE(ods.ORG_ID, 0) AS ORG_ID
    FROM gold_bec_dwh.DIM_AR_CUST_TRX_TYPE AS dw, silver_bec_ods.RA_CUST_TRX_TYPES_ALL AS ods
    WHERE
      dw.dw_load_id = (
        SELECT
          system_id
        FROM bec_etl_ctrl.etlsourceappid
        WHERE
          source_system = 'EBS'
      ) || '-' || COALESCE(ods.CUST_TRX_TYPE_ID, 0) || '-' || COALESCE(ods.ORG_ID, 0)
      AND (
        ods.kca_seq_date > (
          SELECT
            (
              executebegints - prune_days
            )
          FROM bec_etl_ctrl.batch_dw_info
          WHERE
            dw_table_name = 'dim_ar_cust_trx_type' AND batch_name = 'ar'
        )
      )
  );
/* Insert records */
INSERT INTO gold_bec_dwh.DIM_AR_CUST_TRX_TYPE (
  cust_trx_type_id,
  last_update_date,
  status,
  cust_trx_type_name,
  cust_trx_type_description,
  cust_trx_type,
  set_of_books_id,
  end_date,
  start_date,
  org_id,
  legal_entity_id,
  is_deleted_flg,
  source_app_id,
  dw_load_id,
  dw_insert_date,
  dw_update_date
)
(
  SELECT
    CUST_TRX_TYPE_ID,
    LAST_UPDATE_DATE,
    STATUS,
    NAME AS CUST_TRX_TYPE_NAME,
    DESCRIPTION AS CUST_TRX_TYPE_DESCRIPTION,
    `TYPE` AS CUST_TRX_TYPE,
    SET_OF_BOOKS_ID,
    END_DATE,
    START_DATE,
    ORG_ID,
    LEGAL_ENTITY_ID,
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
    ) || '-' || COALESCE(CUST_TRX_TYPE_ID, 0) || '-' || COALESCE(ORG_ID, 0) AS dw_load_id,
    CURRENT_TIMESTAMP() AS dw_insert_date,
    CURRENT_TIMESTAMP() AS dw_update_date
  FROM silver_bec_ods.RA_CUST_TRX_TYPES_ALL
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
          dw_table_name = 'dim_ar_cust_trx_type' AND batch_name = 'ar'
      )
    )
);
/* Soft delete */
UPDATE gold_bec_dwh.DIM_AR_CUST_TRX_TYPE SET is_deleted_flg = 'Y'
WHERE
  NOT (COALESCE(CUST_TRX_TYPE_ID, 0), COALESCE(ORG_ID, 0)) IN (
    SELECT
      COALESCE(ods.CUST_TRX_TYPE_ID, 0),
      COALESCE(ods.ORG_ID, 0) AS ORG_ID
    FROM gold_bec_dwh.DIM_AR_CUST_TRX_TYPE AS dw, silver_bec_ods.RA_CUST_TRX_TYPES_ALL AS ods
    WHERE
      dw.dw_load_id = (
        SELECT
          system_id
        FROM bec_etl_ctrl.etlsourceappid
        WHERE
          source_system = 'EBS'
      ) || '-' || COALESCE(ods.CUST_TRX_TYPE_ID, 0) || '-' || COALESCE(ods.ORG_ID, 0)
      AND ods.is_deleted_flg <> 'Y'
  );
UPDATE bec_etl_ctrl.batch_dw_info SET last_refresh_date = CURRENT_TIMESTAMP()
WHERE
  dw_table_name = 'dim_ar_cust_trx_type' AND batch_name = 'ar';