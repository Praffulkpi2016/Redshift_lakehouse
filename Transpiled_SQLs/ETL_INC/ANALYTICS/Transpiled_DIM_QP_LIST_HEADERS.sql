/* Delete Records */
DELETE FROM gold_bec_dwh.DIM_QP_LIST_HEADERS
WHERE
  (COALESCE(LIST_HEADER_ID, 0), COALESCE(language, 'NA')) IN (
    SELECT
      COALESCE(ods.LIST_HEADER_ID, 0) AS LIST_HEADER_ID,
      COALESCE(ods.language, 'NA') AS language
    FROM gold_bec_dwh.DIM_QP_LIST_HEADERS AS dw, silver_bec_ods.QP_LIST_HEADERS_TL AS ods
    WHERE
      dw.dw_load_id = (
        SELECT
          system_id
        FROM bec_etl_ctrl.etlsourceappid
        WHERE
          source_system = 'EBS'
      ) || '-' || COALESCE(ods.LIST_HEADER_ID, 0) || '-' || COALESCE(ods.language, 'NA')
      AND (
        ods.kca_seq_date > (
          SELECT
            (
              executebegints - prune_days
            )
          FROM bec_etl_ctrl.batch_dw_info
          WHERE
            dw_table_name = 'dim_qp_list_headers' AND batch_name = 'om'
        )
      )
  );
/* Insert records */
INSERT INTO gold_bec_dwh.DIM_QP_LIST_HEADERS (
  LIST_HEADER_ID,
  CREATION_DATE,
  CREATED_BY,
  LAST_UPDATE_DATE,
  LAST_UPDATED_BY,
  LAST_UPDATE_LOGIN,
  language,
  SOURCE_LANG,
  NAME,
  DESCRIPTION,
  VERSION_NO,
  is_deleted_flg,
  source_app_id,
  dw_load_id,
  dw_insert_date,
  dw_update_date
)
(
  SELECT
    LIST_HEADER_ID,
    CREATION_DATE,
    CREATED_BY,
    LAST_UPDATE_DATE,
    LAST_UPDATED_BY,
    LAST_UPDATE_LOGIN,
    language,
    SOURCE_LANG,
    NAME,
    DESCRIPTION,
    VERSION_NO,
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
    ) || '-' || COALESCE(LIST_HEADER_ID, 0) || '-' || COALESCE(language, 'NA') AS dw_load_id,
    CURRENT_TIMESTAMP() AS dw_insert_date,
    CURRENT_TIMESTAMP() AS dw_update_date
  FROM silver_bec_ods.QP_LIST_HEADERS_TL
  WHERE
    (
      kca_seq_date > (
        SELECT
          (
            executebegints - prune_days
          )
        FROM bec_etl_ctrl.batch_dw_info
        WHERE
          dw_table_name = 'dim_qp_list_headers' AND batch_name = 'om'
      )
    )
);
/* Soft delete */
UPDATE gold_bec_dwh.DIM_QP_LIST_HEADERS SET is_deleted_flg = 'Y'
WHERE
  NOT (COALESCE(LIST_HEADER_ID, 0), COALESCE(language, 'NA')) IN (
    SELECT
      COALESCE(ods.LIST_HEADER_ID, 0) AS LIST_HEADER_ID,
      COALESCE(ods.language, 'NA') AS language
    FROM gold_bec_dwh.DIM_QP_LIST_HEADERS AS dw, (
      SELECT
        *
      FROM silver_bec_ods.QP_LIST_HEADERS_TL
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
      ) || '-' || COALESCE(ods.LIST_HEADER_ID, 0) || '-' || COALESCE(ods.language, 'NA')
  );
UPDATE bec_etl_ctrl.batch_dw_info SET load_type = 'I', last_refresh_date = CURRENT_TIMESTAMP()
WHERE
  dw_table_name = 'dim_qp_list_headers' AND batch_name = 'om';