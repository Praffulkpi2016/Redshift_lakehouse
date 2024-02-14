DROP table IF EXISTS gold_bec_dwh.DIM_APPROVED_SUPPLIERS;
CREATE TABLE gold_bec_dwh.DIM_APPROVED_SUPPLIERS AS
(
  SELECT
    a.ASL_ID,
    a.using_organization_id,
    a.owning_organization_id,
    TRUNC(a.creation_date) AS creation_date,
    TRUNC(a.last_update_date) AS last_update_date,
    a.created_by,
    a.vendor_id,
    a.item_id,
    a.owning_organization_id || '-' || a.item_id AS item_category_set1,
    a.owning_organization_id || '-' || a.item_id AS item_category_set2,
    a.vendor_site_id,
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
    ) || '-' || COALESCE(ASL_ID, 0) AS dw_load_id,
    CURRENT_TIMESTAMP() AS dw_insert_date,
    CURRENT_TIMESTAMP() AS dw_update_date
  FROM BEC_ODS.po_approved_supplier_list AS a
);
UPDATE bec_etl_ctrl.batch_dw_info SET load_type = 'I', last_refresh_date = CURRENT_TIMESTAMP()
WHERE
  dw_table_name = 'dim_approved_suppliers' AND batch_name = 'po';