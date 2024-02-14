DROP table IF EXISTS gold_bec_dwh.FACT_PO_CVMI_CONSUMPTION;
CREATE TABLE gold_bec_dwh.fact_po_cvmi_consumption AS
(
  WITH CTE AS (
    SELECT
      ch.material,
      ch.organization_id,
      ch.inventory_item_id,
      mmt.transaction_id
    FROM (
      SELECT
        *
      FROM silver_bec_ods.CST_COST_HISTORY_V
      WHERE
        is_deleted_flg <> 'Y'
    ) AS ch, (
      SELECT
        *
      FROM silver_bec_ods.mtl_material_transactions
      WHERE
        is_deleted_flg <> 'Y'
    ) AS mmt
    /* silver_bec_ods.MTL_CONSUMPTION_TRANSACTIONS mct1 */
    WHERE
      1 = 1
      AND ch.organization_id = mmt.organization_id
      AND ch.inventory_item_id = mmt.inventory_item_id
      AND ch.cost_update_id = mmt.cost_update_id
  ) /* and mmt.transaction_id = mct1.transaction_id) */
  SELECT
    mct.TRANSACTION_ID,
    poh.PO_HEADER_ID,
    prl.PO_RELEASE_ID,
    mct.CREATION_DATE AS consumption_date,
    poh.SEGMENT1 AS po_number,
    prl.RELEASE_NUM,
    mct.NET_QTY AS consumption_quantity,
    mct.blanket_price AS unit_price,
    (
      mct.NET_QTY * mct.blanket_price
    ) AS consumption_value,
    mct.consumption_release_id,
    mct.INVENTORY_ITEM_ID,
    (
      SELECT
        system_id
      FROM bec_etl_ctrl.etlsourceappid
      WHERE
        source_system = 'EBS'
    ) || '-' || mct.INVENTORY_ITEM_ID AS INVENTORY_ITEM_ID_KEY,
    mct.ORGANIZATION_ID,
    poh.VENDOR_ID,
    poh.VENDOR_SITE_ID,
    mct.CONSUMPTION_PROCESSED_FLAG,
    cte.material AS material_cost,
    (
      mct.NET_QTY * cte.material
    ) AS extended_cost,
    (
      mct.NET_QTY * mct.blanket_price - mct.NET_QTY * (
        cte.material
      )
    ) AS consigned_ppv,
    'N' AS IS_DELETED_FLG,
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
    ) || '-' || COALESCE(mct.TRANSACTION_ID, 0) || '-' || COALESCE(poh.PO_HEADER_ID, 0) || '-' || COALESCE(prl.PO_RELEASE_ID, 0) AS dw_load_id,
    CURRENT_TIMESTAMP() AS dw_insert_date,
    CURRENT_TIMESTAMP() AS dw_update_date
  FROM (
    SELECT
      *
    FROM silver_bec_ods.MTL_CONSUMPTION_TRANSACTIONS
    WHERE
      is_deleted_flg <> 'Y'
  ) AS mct, (
    SELECT
      *
    FROM silver_bec_ods.po_headers_all
    WHERE
      is_deleted_flg <> 'Y'
  ) AS poh, (
    SELECT
      *
    FROM silver_bec_ods.po_releases_all
    WHERE
      is_deleted_flg <> 'Y'
  ) AS prl, CTE
  WHERE
    1 = 1
    AND mct.consumption_RELEASE_ID = prl.PO_RELEASE_ID()
    AND prl.PO_HEADER_ID = poh.PO_HEADER_ID
    AND mct.organization_id = CTE.ORGANIZATION_ID()
    AND mct.inventory_item_id = CTE.INVENTORY_ITEM_ID()
    AND mct.transaction_id = CTE.TRANSACTION_ID()
);
UPDATE bec_etl_ctrl.batch_dw_info SET load_type = 'I', last_refresh_date = CURRENT_TIMESTAMP()
WHERE
  dw_table_name = 'fact_po_cvmi_consumption' AND batch_name = 'po';