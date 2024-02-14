/* Delete Records */
DELETE FROM gold_bec_dwh.DIM_SO_TRANSACTION_TYPES
WHERE
  (COALESCE(TRANSACTION_TYPE_ID, 0), COALESCE(language, 'NA')) IN (
    SELECT
      COALESCE(ods.TRANSACTION_TYPE_ID, 0) AS TRANSACTION_TYPE_ID,
      COALESCE(ods.language, 'NA') AS language
    FROM gold_bec_dwh.DIM_SO_TRANSACTION_TYPES AS dw, silver_bec_ods.OE_TRANSACTION_TYPES_TL AS ods
    WHERE
      dw.dw_load_id = (
        SELECT
          system_id
        FROM bec_etl_ctrl.etlsourceappid
        WHERE
          source_system = 'EBS'
      ) || '-' || COALESCE(ods.TRANSACTION_TYPE_ID, 0) || '-' || COALESCE(ods.language, 'NA')
      AND (
        ods.kca_seq_date > (
          SELECT
            (
              executebegints - prune_days
            )
          FROM bec_etl_ctrl.batch_dw_info
          WHERE
            dw_table_name = 'dim_so_transaction_types' AND batch_name = 'om'
        )
      )
  );
/* Insert records */
INSERT INTO gold_bec_dwh.DIM_SO_TRANSACTION_TYPES (
  TRANSACTION_TYPE_ID,
  language,
  SOURCE_LANG,
  NAME,
  DESCRIPTION,
  CREATION_DATE,
  CREATED_BY,
  LAST_UPDATE_DATE,
  LAST_UPDATED_BY,
  LAST_UPDATE_LOGIN,
  PROGRAM_APPLICATION_ID,
  PROGRAM_ID,
  REQUEST_ID,
  is_deleted_flg,
  source_app_id,
  dw_load_id,
  dw_insert_date,
  dw_update_date
)
(
  SELECT
    TRANSACTION_TYPE_ID,
    language,
    SOURCE_LANG,
    NAME,
    DESCRIPTION,
    CREATION_DATE,
    CREATED_BY,
    LAST_UPDATE_DATE,
    LAST_UPDATED_BY,
    LAST_UPDATE_LOGIN,
    PROGRAM_APPLICATION_ID,
    PROGRAM_ID,
    REQUEST_ID,
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
    ) || '-' || COALESCE(TRANSACTION_TYPE_ID, 0) || '-' || COALESCE(language, 'NA') AS dw_load_id,
    CURRENT_TIMESTAMP() AS dw_insert_date,
    CURRENT_TIMESTAMP() AS dw_update_date
  FROM silver_bec_ods.OE_TRANSACTION_TYPES_TL
  WHERE
    (
      kca_seq_date > (
        SELECT
          (
            executebegints - prune_days
          )
        FROM bec_etl_ctrl.batch_dw_info
        WHERE
          dw_table_name = 'dim_so_transaction_types' AND batch_name = 'om'
      )
    )
);
/* Soft delete */
UPDATE gold_bec_dwh.DIM_SO_TRANSACTION_TYPES SET is_deleted_flg = 'Y'
WHERE
  NOT (COALESCE(TRANSACTION_TYPE_ID, 0), COALESCE(language, 'NA')) IN (
    SELECT
      COALESCE(ods.TRANSACTION_TYPE_ID, 0) AS TRANSACTION_TYPE_ID,
      COALESCE(ods.language, 'NA') AS language
    FROM gold_bec_dwh.DIM_SO_TRANSACTION_TYPES AS dw, (
      SELECT
        *
      FROM silver_bec_ods.OE_TRANSACTION_TYPES_TL
      WHERE
        is_deleted_flg <> 'Y'
    ) AS ods
    WHERE
      dw.dw_load_id = (
        SELECT
          system_id
        FROM bec_etl_ctrl.etlsourceappid
        WHERE
          source_system = 'EBS'
      ) || '-' || COALESCE(ods.TRANSACTION_TYPE_ID, 0) || '-' || COALESCE(ods.language, 'NA')
  );
UPDATE bec_etl_ctrl.batch_dw_info SET load_type = 'I', last_refresh_date = CURRENT_TIMESTAMP()
WHERE
  dw_table_name = 'dim_so_transaction_types' AND batch_name = 'om';