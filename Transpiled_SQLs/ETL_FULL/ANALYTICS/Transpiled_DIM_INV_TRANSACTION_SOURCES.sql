DROP table IF EXISTS gold_bec_dwh.DIM_INV_TRANSACTION_SOURCES;
CREATE TABLE gold_bec_dwh.DIM_INV_TRANSACTION_SOURCES AS
(
  /* main query starts here */
  WITH ct1 AS (
    SELECT
      po_header_id,
      segment1
    FROM silver_bec_ods.po_headers_all
  ), ct2 AS (
    SELECT
      wip_entity_id,
      wip_entity_name
    FROM silver_bec_ods.wip_entities
  ), ct3 AS (
    SELECT
      oola.line_id,
      CAST(ooha.order_number AS STRING) AS order_number
    FROM silver_bec_ods.oe_order_headers_all AS ooha
    INNER JOIN silver_bec_ods.oe_order_lines_all AS oola
      ON oola.header_id = ooha.header_id
  ), ct4 AS (
    SELECT
      header_id,
      request_number
    FROM silver_bec_ods.mtl_txn_request_headers
  ), ct5 AS (
    SELECT
      disposition_id,
      segment1
    FROM silver_bec_ods.mtl_generic_dispositions
  ), ct6 AS (
    SELECT
      requisition_header_id,
      segment1
    FROM silver_bec_ods.po_requisition_headers_all
  ), ct7 AS (
    SELECT
      organization_id,
      cycle_count_header_id,
      cycle_count_header_name
    FROM silver_bec_ods.mtl_cycle_count_headers
  ), ct8 AS (
    SELECT
      organization_id,
      physical_inventory_id,
      physical_inventory_name
    FROM silver_bec_ods.mtl_physical_inventories
  ), ct9 AS (
    SELECT
      cost_update_id,
      description
    FROM silver_bec_ods.cst_cost_updates
  )
  SELECT
    mmt.transaction_id,
    mtt.transaction_type_name,
    mmt.transaction_type_id,
    CASE
      WHEN SUBSTRING(mtt.transaction_type_name, 1, 2) = 'PO'
      THEN ct1.segment1
      WHEN SUBSTRING(mtt.transaction_type_name, 1, 2) = 'WI'
      THEN ct2.wip_entity_name
      WHEN SUBSTRING(mtt.transaction_type_name, 1, 2) = 'Sa'
      THEN ct3.order_number
      ELSE CASE
        WHEN mmt.transaction_source_type_id = 4
        THEN ct4.request_number
        WHEN mmt.transaction_source_type_id = 6
        THEN ct5.segment1
        WHEN mmt.transaction_source_type_id = 7
        THEN ct6.segment1
        WHEN mmt.transaction_source_type_id = 9
        THEN ct7.cycle_count_header_name
        WHEN mmt.transaction_source_type_id = 10
        THEN ct8.physical_inventory_name
        WHEN mmt.transaction_source_type_id = 11
        THEN ct9.description
        ELSE NULL
      END
    END AS transaction_source,
    mmt.last_update_date,
    'N' AS is_deleted_flg, /* audit columns */
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
    ) || '-' || COALESCE(mmt.transaction_id, 0) AS dw_load_id,
    CURRENT_TIMESTAMP() AS dw_insert_date,
    CURRENT_TIMESTAMP() AS dw_update_date
  FROM silver_bec_ods.mtl_material_transactions AS mmt
  LEFT OUTER JOIN silver_bec_ods.mtl_transaction_types AS mtt
    ON mmt.transaction_type_id = mtt.transaction_type_id
  LEFT OUTER JOIN ct1
    ON mmt.transaction_source_id = ct1.po_header_id
  LEFT OUTER JOIN ct2
    ON mmt.transaction_source_id = ct2.wip_entity_id
  LEFT OUTER JOIN ct3
    ON mmt.trx_source_line_id = ct3.line_id
  LEFT OUTER JOIN ct4
    ON mmt.transaction_source_id = ct4.header_id
  LEFT OUTER JOIN ct5
    ON mmt.transaction_source_id = ct5.disposition_id
  LEFT OUTER JOIN ct6
    ON mmt.transaction_source_id = ct6.requisition_header_id
  LEFT OUTER JOIN ct7
    ON mmt.transaction_source_id = ct7.cycle_count_header_id
    AND mmt.organization_id = ct7.organization_id
  LEFT OUTER JOIN ct8
    ON mmt.transaction_source_id = ct8.physical_inventory_id
    AND mmt.organization_id = ct8.organization_id
  LEFT OUTER JOIN ct9
    ON mmt.transaction_source_id = ct9.cost_update_id
);
UPDATE bec_etl_ctrl.batch_dw_info SET load_type = 'I', last_refresh_date = CURRENT_TIMESTAMP()
WHERE
  dw_table_name = 'dim_inv_transaction_sources' AND batch_name = 'inv';