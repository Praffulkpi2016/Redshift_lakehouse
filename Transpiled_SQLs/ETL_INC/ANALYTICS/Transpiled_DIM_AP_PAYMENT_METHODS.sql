/* Delete Records */
DELETE FROM gold_bec_dwh.DIM_AP_PAYMENT_METHODS
WHERE
  (
    COALESCE(PAYMENT_METHOD_CODE, 'NA')
  ) IN (
    SELECT
      COALESCE(ods.PAYMENT_METHOD_CODE, 'NA') AS PAYMENT_METHOD_CODE
    FROM gold_bec_dwh.dim_ap_payment_methods AS dw, (
      SELECT
        PAYMENT_METHOD_CODE
      FROM silver_bec_ods.iby_payment_methods_tl
      WHERE
        Language = 'US'
        AND kca_seq_date > (
          SELECT
            (
              executebegints - prune_days
            )
          FROM bec_etl_ctrl.batch_dw_info
          WHERE
            dw_table_name = 'dim_ap_payment_methods' AND batch_name = 'ap'
        )
    ) AS ods
    WHERE
      dw.dw_load_id = (
        SELECT
          system_id
        FROM bec_etl_ctrl.etlsourceappid
        WHERE
          source_system = 'EBS'
      ) || '-' || COALESCE(ods.PAYMENT_METHOD_CODE, 'NA')
  );
/* Insert records */
INSERT INTO gold_bec_dwh.dim_ap_payment_methods (
  PAYMENT_METHOD_CODE,
  PAYMENT_METHOD_NAME,
  DESCRIPTION,
  is_deleted_flg,
  SOURCE_APP_ID,
  DW_LOAD_ID,
  DW_INSERT_DATE,
  DW_UPDATE_DATE
)
(
  SELECT
    PAYMENT_METHOD_CODE,
    PAYMENT_METHOD_NAME,
    DESCRIPTION,
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
    ) || '-' || COALESCE(PAYMENT_METHOD_CODE, 'NA') AS dw_load_id,
    CURRENT_TIMESTAMP() AS dw_insert_date,
    CURRENT_TIMESTAMP() AS dw_update_date
  FROM silver_bec_ods.iby_payment_methods_tl
  WHERE
    language = 'US'
    AND (
      kca_seq_date > (
        SELECT
          (
            executebegints - prune_days
          )
        FROM bec_etl_ctrl.batch_dw_info
        WHERE
          dw_table_name = 'dim_ap_payment_methods' AND batch_name = 'ap'
      )
    )
);
/* Soft delete */
UPDATE gold_bec_dwh.dim_ap_payment_methods SET is_deleted_flg = 'Y'
WHERE
  NOT (
    COALESCE(PAYMENT_METHOD_CODE, 'NA')
  ) IN (
    SELECT
      COALESCE(ods.PAYMENT_METHOD_CODE, 'NA') AS PAYMENT_METHOD_CODE
    FROM gold_bec_dwh.dim_ap_payment_methods AS dw, (
      SELECT
        *
      FROM silver_bec_ods.iby_payment_methods_tl
      WHERE
        language = 'US'
    ) AS ods
    WHERE
      dw.dw_load_id = (
        SELECT
          system_id
        FROM bec_etl_ctrl.etlsourceappid
        WHERE
          source_system = 'EBS'
      ) || '-' || COALESCE(ods.PAYMENT_METHOD_CODE, 'NA')
      AND ods.is_deleted_flg <> 'Y'
  );
UPDATE bec_etl_ctrl.batch_dw_info SET last_refresh_date = CURRENT_TIMESTAMP()
WHERE
  dw_table_name = 'dim_ap_payment_methods' AND batch_name = 'ap';