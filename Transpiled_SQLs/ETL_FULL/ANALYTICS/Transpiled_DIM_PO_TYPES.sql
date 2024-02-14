DROP table IF EXISTS gold_bec_dwh.DIM_PO_TYPES;
CREATE TABLE gold_bec_dwh.DIM_PO_TYPES AS
(
  SELECT
    LOOKUP_CODE AS PO_TYPE,
    LOOKUP_TYPE,
    DISPLAYED_FIELD AS PO_TYPE_DISPLAY,
    DESCRIPTION AS PO_TYPE_DESC,
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
    ) || '-' || COALESCE(LOOKUP_CODE, '0') || '-' || COALESCE(LOOKUP_TYPE, '0') AS dw_load_id,
    CURRENT_TIMESTAMP() AS dw_insert_date,
    CURRENT_TIMESTAMP() AS dw_update_date
  FROM silver_bec_ods.PO_LOOKUP_CODES
);
UPDATE bec_etl_ctrl.batch_dw_info SET load_type = 'I', last_refresh_date = CURRENT_TIMESTAMP()
WHERE
  dw_table_name = 'dim_po_types' AND batch_name = 'po';