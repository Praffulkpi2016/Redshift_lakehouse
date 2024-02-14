TRUNCATE table gold_bec_dwh_rpt.FACT_MAT_DIST_SO_SLNO_RT_TMP;
/* Insert records into temp table */
INSERT INTO gold_bec_dwh_rpt.FACT_MAT_DIST_SO_SLNO_RT_TMP
(
  SELECT DISTINCT
    transaction_id,
    inventory_item_id,
    organization_id
  FROM gold_bec_dwh.fact_cst_inv_distribution_stg2 AS mta
  WHERE
    1 = 1
    AND COALESCE(mta.dw_update_date, '2022-01-01 12:00:00.000') > (
      SELECT
        (
          executebegints - prune_days
        )
      FROM bec_etl_ctrl.batch_dw_info
      WHERE
        dw_table_name = 'fact_mat_dist_so_slno_rt' AND batch_name = 'inv'
    )
);
/* delete records from fact table */
DELETE FROM gold_bec_dwh_rpt.FACT_MAT_DIST_SO_SLNO_RT
WHERE
  EXISTS(
    SELECT
      1
    FROM gold_bec_dwh_rpt.FACT_MAT_DIST_SO_SLNO_RT_TMP AS tmp
    WHERE
      tmp.transaction_id = FACT_MAT_DIST_SO_SLNO_RT.transaction_id
      AND tmp.inventory_item_id = FACT_MAT_DIST_SO_SLNO_RT.inventory_item_id
      AND tmp.organization_id = FACT_MAT_DIST_SO_SLNO_RT.organization_id
  );
WITH CTE_INSTALL AS (
  SELECT
    install.party_name,
    install.address1,
    install.address2,
    install.address5,
    install.party_site_id,
    install.site_use_code
  FROM gold_bec_dwh.dim_customer_details AS install
  WHERE
    SITE_USE_CODE = 'SHIP_TO'
  GROUP BY
    install.party_name,
    install.address1,
    install.address2,
    install.address5,
    install.party_site_id,
    install.site_use_code
)
INSERT INTO gold_bec_dwh_rpt.FACT_MAT_DIST_SO_SLNO_RT
(
  SELECT
    mtrx.inventory_item_id,
    mtrx.transaction_source_type_id,
    mtrx.transaction_id,
    gcc.concatenated_segments AS code_combination,
    gcc.segment2 AS dept,
    gcc.segment3 AS account_number,
    gcc.segment1 AS company_code,
    gcc.segment4 AS intercompany,
    gcc.segment6 AS future2,
    mtrx.transaction_date,
    mtrx.creation_date,
    mtrx.last_update_date,
    msi.item_name AS part_number,
    msi.item_description,
    mtrx.subinventory_code,
    mtrx.primary_quantity AS primary_quantity,
    mtrx.serial_number,
    msi.primary_uom_code AS primary_uom,
    cce.cost_element,
    mtrx.transaction_source_id,
    mtrx.trx_source_line_id,
    mtrx.created_by,
    CASE
      WHEN CAST(mtrx.transaction_source_type_id AS STRING) = '1'
      THEN poh.segment1
      WHEN CAST(mtrx.transaction_source_type_id AS STRING) = '2'
      THEN CAST(oeh.order_number AS STRING)
      WHEN CAST(mtrx.transaction_source_type_id AS STRING) = '8'
      THEN CAST(oeh.order_number AS STRING)
      WHEN CAST(mtrx.transaction_source_type_id AS STRING) = '12'
      THEN CAST(oeh.order_number AS STRING)
      WHEN CAST(mtrx.transaction_source_type_id AS STRING) = '3'
      THEN gcc.concatenated_segments
      WHEN CAST(mtrx.transaction_source_type_id AS STRING) = '4'
      THEN CAST(mtrh.request_number AS STRING)
      WHEN CAST(mtrx.transaction_source_type_id AS STRING) = '5'
      THEN wtv.wip_entity_name
      WHEN CAST(mtrx.transaction_source_type_id AS STRING) = '6'
      THEN mgd.segment1
      WHEN CAST(mtrx.transaction_source_type_id AS STRING) = '7'
      THEN prh.segment1
      WHEN CAST(mtrx.transaction_source_type_id AS STRING) = '9'
      THEN mcch.cycle_count_header_name
      WHEN CAST(mtrx.transaction_source_type_id AS STRING) = '10'
      THEN mpi.physical_inventory_name
      WHEN CAST(mtrx.transaction_source_type_id AS STRING) = '11'
      THEN ccu.description
      ELSE CAST(mtrx.transaction_source_id AS STRING)
    END AS transaction_source_name,
    CASE
      WHEN mtrx.transaction_type_id = 24
      THEN mtrx.transaction_cost
      ELSE mtrx.unit_cost
    END AS unit_cost,
    mtrx.transaction_cost,
    mtrx.rcv_transaction_id,
    mtrx.source_line_id,
    mtrx.organization_id,
    mtrx.transaction_organization_id,
    mtrx.transaction_type_id,
    mtrx.accounting_line_type,
    dlu.meaning AS accounting_line_type_name,
    mtrx.transaction_reference,
    oeh.order_number AS sales_order_number,
    oeh.line_number AS sales_order_line_number,
    bill_to.party_name AS bill_customer,
    bill_to.address1 AS bill_address1,
    bill_to.address2 AS bill_address2,
    bill_to.address5 AS bill_address5,
    oeh.ship_to_customer_name AS ship_customer,
    oeh.ship_to_addr_line1 AS ship_address1,
    oeh.ship_to_addr_line2 AS ship_address2,
    oeh.ship_to_addr_line5 AS ship_address5,
    oeh.order_type_id,
    gcc.segment5 AS budget_id,
    mic.item_category_segment1 AS category_seg1,
    mic.item_category_segment2 AS category_seg2,
    mic1.item_category_segment1 AS new_category_seg1,
    mic1.item_category_segment2 AS new_category_seg2,
    dbh.incident_number AS service_request,
    dbh.task_name,
    dbh.task_number,
    dbh.debrief_number,
    install.party_name AS install_customer,
    install.address1 AS install_address1,
    install.address2 AS install_address2,
    install.address5 AS install_address5,
    mtrx.REFERENCE_ACCOUNT,
    mtrx.COST_ELEMENT_ID,
    msi.program_name,
    oeh.sales_order_line_type,
    mtrx.dw_load_id AS dw_load_id_mtrx,
    (
      SELECT
        system_id
      FROM bec_etl_ctrl.etlsourceappid
      WHERE
        source_system = 'EBS'
    ) || '-' || COALESCE(mtrx.dw_load_id, 'NA') || '-' || COALESCE(mtrx.serial_number, 'NA') AS dw_load_id
  FROM gold_bec_dwh.fact_cst_inv_distribution_stg2 AS mtrx, gold_bec_dwh_rpt.FACT_MAT_DIST_SO_SLNO_RT_TMP AS tmp, gold_bec_dwh.dim_gl_accounts AS gcc, gold_bec_dwh.dim_inv_master_items AS msi, (
    SELECT
      *
    FROM gold_bec_dwh.dim_inv_item_category_set
    WHERE
      category_set_id = 1
  ) AS mic, (
    SELECT
      *
    FROM gold_bec_dwh.dim_inv_item_category_set
    WHERE
      category_set_id = 1100000081
  ) AS mic1, gold_bec_dwh.dim_wip_jobs AS wtv, gold_bec_dwh.fact_om_order_details AS oeh, (
    SELECT
      *
    FROM silver_bec_ods.cst_cost_elements
    WHERE
      is_deleted_flg <> 'Y'
  ) AS cce, gold_bec_dwh.dim_customer_details AS bill_to, gold_bec_dwh.FACT_CSF_DEBRIEF_HEADERS AS dbh, CTE_INSTALL AS install, (
    SELECT
      dl.lookup_code,
      dl.meaning
    FROM gold_bec_dwh.dim_lookups AS dl
    WHERE
      dl.lookup_type = 'CST_ACCOUNTING_LINE_TYPE'
  ) AS dlu, (
    SELECT
      *
    FROM silver_bec_ods.cst_cost_updates
    WHERE
      is_deleted_flg <> 'Y'
  ) AS ccu, (
    SELECT
      *
    FROM silver_bec_ods.po_headers_all
    WHERE
      is_deleted_flg <> 'Y'
  ) AS poh, (
    SELECT
      *
    FROM silver_bec_ods.mtl_txn_request_headers
    WHERE
      is_deleted_flg <> 'Y'
  ) AS mtrh, (
    SELECT
      *
    FROM silver_bec_ods.mtl_generic_dispositions
    WHERE
      is_deleted_flg <> 'Y'
  ) AS mgd, (
    SELECT
      *
    FROM silver_bec_ods.po_requisition_headers_all
    WHERE
      is_deleted_flg <> 'Y'
  ) AS prh, (
    SELECT
      *
    FROM silver_bec_ods.mtl_cycle_count_headers
    WHERE
      is_deleted_flg <> 'Y'
  ) AS mcch, (
    SELECT
      *
    FROM silver_bec_ods.mtl_physical_inventories
    WHERE
      is_deleted_flg <> 'Y'
  ) AS mpi
  WHERE
    1 = 1
    AND mtrx.last_update_date >= '2022-10-01 12:00:00.000'
    AND mtrx.reference_account = gcc.code_combination_id
    AND mtrx.inventory_item_id = tmp.inventory_item_id
    AND mtrx.organization_id = tmp.organization_id
    AND mtrx.transaction_id = tmp.transaction_id
    AND mtrx.inventory_item_id = msi.inventory_item_id
    AND mtrx.organization_id = msi.organization_id
    AND mtrx.inventory_item_id = mic.INVENTORY_ITEM_ID()
    AND mtrx.organization_id = mic.ORGANIZATION_ID()
    AND mtrx.inventory_item_id = mic1.INVENTORY_ITEM_ID()
    AND mtrx.organization_id = mic1.ORGANIZATION_ID()
    AND mtrx.transaction_source_id = wtv.WIP_ENTITY_ID()
    AND mtrx.organization_id = wtv.ORGANIZATION_ID()
    AND mtrx.trx_source_line_id = oeh.LINE_ID()
    AND oeh.invoice_to_org_id = bill_to.SITE_USE_ID()
    AND mtrx.cost_element_id = cce.COST_ELEMENT_ID()
    AND mtrx.transaction_source_id = dbh.DEBRIEF_HEADER_ID()
    AND dbh.install_location_id = install.PARTY_SITE_ID()
    AND mtrx.accounting_line_type = dlu.LOOKUP_CODE()
    AND mtrx.transaction_source_id = ccu.COST_UPDATE_ID()
    AND mtrx.transaction_source_id = poh.PO_HEADER_ID()
    AND mtrx.transaction_source_id = mtrh.HEADER_ID()
    AND mtrx.transaction_source_id = mgd.DISPOSITION_ID()
    AND mtrx.transaction_source_id = prh.REQUISITION_HEADER_ID()
    AND mtrx.transaction_source_id = mcch.CYCLE_COUNT_HEADER_ID()
    AND mtrx.organization_id = mcch.ORGANIZATION_ID()
    AND mtrx.transaction_source_id = mpi.PHYSICAL_INVENTORY_ID()
    AND mtrx.organization_id = mpi.ORGANIZATION_ID()
);
UPDATE bec_etl_ctrl.batch_dw_info SET load_type = 'I', last_refresh_date = CURRENT_TIMESTAMP()
WHERE
  dw_table_name = 'fact_mat_dist_so_slno_rt' AND batch_name = 'inv';