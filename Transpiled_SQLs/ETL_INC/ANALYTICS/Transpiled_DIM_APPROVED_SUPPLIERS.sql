/* Delete Records */
DELETE FROM gold_bec_dwh.DIM_APPROVED_SUPPLIERS
WHERE
  COALESCE(ASL_ID, 0) IN (
    SELECT
      COALESCE(ods.ASL_ID, 0)
    FROM gold_bec_dwh.DIM_APPROVED_SUPPLIERS AS dw, silver_bec_ods.po_approved_supplier_list AS ods
    WHERE
      dw.dw_load_id = (
        SELECT
          system_id
        FROM bec_etl_ctrl.etlsourceappid
        WHERE
          source_system = 'EBS'
      ) || '-' || COALESCE(ods.ASL_ID, 0)
      AND (
        ods.kca_seq_date > (
          SELECT
            (
              executebegints - prune_days
            )
          FROM bec_etl_ctrl.batch_dw_info
          WHERE
            dw_table_name = 'dim_approved_suppliers' AND batch_name = 'po'
        )
      )
  );
/* Insert records */
INSERT INTO gold_bec_dwh.DIM_APPROVED_SUPPLIERS (
  ASL_ID,
  using_organization_id,
  owning_organization_id,
  creation_date,
  last_update_date,
  created_by,
  vendor_id,
  item_id,
  item_category_set1,
  item_category_set2,
  vendor_site_id,
  is_deleted_flg,
  source_app_id,
  dw_load_id,
  dw_insert_date,
  dw_update_date
)
(
  SELECT
    ASL_ID,
    using_organization_id,
    owning_organization_id,
    TRUNC(creation_date) AS creation_date,
    TRUNC(last_update_date) AS last_update_date,
    created_by,
    vendor_id,
    item_id,
    owning_organization_id || '-' || item_id AS item_category_set1,
    owning_organization_id || '-' || item_id AS item_category_set2,
    vendor_site_id,
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
  FROM silver_bec_ods.po_approved_supplier_list
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
          dw_table_name = 'dim_approved_suppliers' AND batch_name = 'po'
      )
    )
);
/* Soft delete */
UPDATE gold_bec_dwh.DIM_APPROVED_SUPPLIERS SET is_deleted_flg = 'Y'
WHERE
  NOT COALESCE(ASL_ID, 0) IN (
    SELECT
      COALESCE(ods.ASL_ID, 0)
    FROM gold_bec_dwh.DIM_APPROVED_SUPPLIERS AS dw, silver_bec_ods.po_approved_supplier_list AS ods
    WHERE
      dw.dw_load_id = (
        SELECT
          system_id
        FROM bec_etl_ctrl.etlsourceappid
        WHERE
          source_system = 'EBS'
      ) || '-' || COALESCE(ods.ASL_ID, 0)
      AND ods.is_deleted_flg <> 'Y'
  );
UPDATE bec_etl_ctrl.batch_dw_info SET last_refresh_date = CURRENT_TIMESTAMP()
WHERE
  dw_table_name = 'dim_approved_suppliers' AND batch_name = 'po';