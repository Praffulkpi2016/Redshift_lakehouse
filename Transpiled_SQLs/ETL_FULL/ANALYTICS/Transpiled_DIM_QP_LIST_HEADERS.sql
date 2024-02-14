DROP table IF EXISTS gold_bec_dwh.DIM_QP_LIST_HEADERS;
CREATE TABLE gold_bec_dwh.DIM_QP_LIST_HEADERS AS
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
);
UPDATE bec_etl_ctrl.batch_dw_info SET load_type = 'I', last_refresh_date = CURRENT_TIMESTAMP()
WHERE
  dw_table_name = 'dim_qp_list_headers' AND batch_name = 'om';