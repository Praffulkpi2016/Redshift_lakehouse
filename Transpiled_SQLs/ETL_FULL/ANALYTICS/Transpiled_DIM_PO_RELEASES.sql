DROP table IF EXISTS gold_bec_dwh.DIM_PO_RELEASES;
CREATE TABLE gold_bec_dwh.DIM_PO_RELEASES AS
(
  SELECT
    PO_RELEASE_ID,
    PO_HEADER_ID,
    RELEASE_NUM,
    AGENT_ID,
    RELEASE_DATE,
    REVISION_NUM,
    REVISED_DATE,
    APPROVED_FLAG,
    APPROVED_DATE,
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
    ) || '-' || COALESCE(PO_RELEASE_ID, 0) AS dw_load_id,
    CURRENT_TIMESTAMP() AS dw_insert_date,
    CURRENT_TIMESTAMP() AS dw_update_date
  FROM silver_bec_ods.PO_RELEASES_ALL
);
UPDATE bec_etl_ctrl.batch_dw_info SET load_type = 'I', last_refresh_date = CURRENT_TIMESTAMP()
WHERE
  dw_table_name = 'dim_po_releases' AND batch_name = 'ap';